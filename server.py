import argparse
import asyncio
import base64
import logging
import time
from asyncio import PriorityQueue, Queue, StreamReader, StreamWriter
from typing import Any, Callable, Dict, Tuple

import aiohttp

from chat_rotator import ChatRotator
from config import VK_CHAT_PEER_IDS, VK_SERVER_GROUP_ID, VK_SERVER_TOKEN
from constants import (
    CONNECTION_POOL_LIMIT,
    CONNECTION_POOL_LIMIT_PER_HOST,
    MAX_PACKET_BUFFER_SIZE,
    NUM_SENDER_WORKERS,
    PACKET_BUFFER_TIMEOUT,
    SENDER_QUEUE_MAX_SIZE,
    MessageType,
    Target,
)
from protocol import (
    cleanup_session_metrics,
    data_sender_handler,
    get_session_metrics,
    report_global_statistics,
    send_vk_message,
    upload_and_send_chunk,
)
from vk_api import VkontakteApiError, start_long_poll_listener

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
)
# ---

VERBOSE = False


# Define a structured type for a session
class Session:
    def __init__(
        self,
        destination: Tuple[str, int],
        http_session: aiohttp.ClientSession,
        sender_queue: Queue,
    ):
        self.destination = destination
        self.http_session = http_session
        self.sender_queue = sender_queue
        self.incoming_queue: PriorityQueue[Tuple[int, bytes | None]] = PriorityQueue()
        self.writer_task_started = False


SESSIONS: Dict[str, Session] = {}

# Буфер для сборки multi-part сообщений: {session_id: {seq: {part_idx: data}}}
MULTIPART_BUFFERS: Dict[str, Dict[int, Dict[int, bytes]]] = {}

# Глобальный ротатор чатов для сервера
chat_rotator: ChatRotator = None

# --- Core Logic: Data Handlers ---


async def close_downlink_session(session_id: str, sequence_number: int) -> None:
    """Callback to send a CLOSE message for a downlink session."""
    if session_id in SESSIONS:
        session = SESSIONS[session_id]
        try:
            peer_id = await chat_rotator.get_next_peer_id()
            await send_vk_message(
                session.http_session,
                VK_SERVER_TOKEN,
                peer_id,
                Target.CLIENT,
                MessageType.CLOSE,
                session_id,
                sequence_number,
            )
        except (RuntimeError, aiohttp.ClientError):
            # Session is closed during shutdown, ignore
            logging.debug(
                f"[{session_id}] Cannot send CLOSE message, session is closed"
            )


async def downlink_handler(
    session_id: str,
    reader: StreamReader,
    http_session: aiohttp.ClientSession,
    sender_queue: Queue,
) -> None:
    """Wrapper for the generic data sender to handle the server's downlink."""
    sequence_number = 0
    try:
        sequence_number = await data_sender_handler(
            "Downlink",
            session_id,
            reader,
            Target.CLIENT,
            sender_queue,
        )
    except asyncio.CancelledError:
        logging.debug(f"[{session_id}] Downlink handler cancelled")
        raise
    finally:
        await close_downlink_session(session_id, sequence_number + 1)


async def server_writer_task(session_id: str, session: Session) -> None:
    """Connects to the destination and writes data from the client, handling out-of-order packets."""
    writer: StreamWriter | None = None
    try:
        host, port = session.destination
        logging.info(f"[{session_id}] Writer task started, connecting to {host}:{port}")
        reader, writer = await asyncio.open_connection(host, port)

        asyncio.create_task(
            downlink_handler(
                session_id, reader, session.http_session, session.sender_queue
            )
        )

        next_expected_seq = 0
        # Buffer for out-of-order packets: {seq: payload}
        packet_buffer: Dict[int, bytes] = {}
        buffer_timestamps: Dict[int, float] = {}

        while True:
            # First, process any buffered packets that are now in sequence
            while next_expected_seq in packet_buffer:
                payload_chunk = packet_buffer.pop(next_expected_seq)
                buffer_timestamps.pop(next_expected_seq, None)
                writer.write(payload_chunk)
                await writer.drain()
                if VERBOSE:
                    logging.info(
                        f"[{session_id}] Wrote buffered packet {next_expected_seq} to destination."
                    )
                next_expected_seq += 1

            # Clean up old packets from buffer (prevent memory leak)
            current_time = time.time()
            for seq in list(buffer_timestamps.keys()):
                if current_time - buffer_timestamps[seq] > PACKET_BUFFER_TIMEOUT:
                    packet_buffer.pop(seq, None)
                    buffer_timestamps.pop(seq)
                    logging.warning(
                        f"[{session_id}] Packet {seq} timeout, dropped from buffer"
                    )

            # Then, wait for the next packet from the queue
            seq, payload_chunk = await session.incoming_queue.get()

            if payload_chunk is None:  # Sentinel for closing
                session.incoming_queue.task_done()
                break

            # If the packet is the one we expect, process it directly
            if seq == next_expected_seq:
                writer.write(payload_chunk)
                await writer.drain()
                if VERBOSE:
                    logging.info(f"[{session_id}] Wrote packet {seq} to destination.")
                next_expected_seq += 1
            # If the packet is from the future, buffer it
            elif seq > next_expected_seq:
                # Buffer overflow protection
                if len(packet_buffer) >= MAX_PACKET_BUFFER_SIZE:
                    # Drop oldest packet
                    oldest_seq = min(
                        buffer_timestamps.keys(), key=lambda k: buffer_timestamps[k]
                    )
                    packet_buffer.pop(oldest_seq, None)
                    buffer_timestamps.pop(oldest_seq)
                    logging.warning(
                        f"[{session_id}] Buffer overflow, dropped packet {oldest_seq}"
                    )

                if VERBOSE:
                    logging.info(
                        f"[{session_id}] Buffered out-of-order packet {seq} (expected {next_expected_seq})."
                    )
                packet_buffer[seq] = payload_chunk
                buffer_timestamps[seq] = time.time()
            # If the packet is from the past (already processed), ignore it
            else:
                if VERBOSE:
                    logging.warning(
                        f"[{session_id}] Discarding old packet {seq} (expected {next_expected_seq})."
                    )

            session.incoming_queue.task_done()

    except (ConnectionRefusedError, OSError) as e:
        logging.error(f"[{session_id}] Connection failed in server_writer_task: {e}")
    except Exception as e:
        logging.error(f"[{session_id}] Error in server_writer_task: {e}")
    finally:
        logging.info(f"[{session_id}] Writer task stopped.")
        if writer and not writer.is_closing():
            writer.close()
            await writer.wait_closed()
        SESSIONS.pop(session_id, None)


async def handle_client_message(
    headers: Dict[str, str],
    payload_str: str,
    attachment: Dict[str, Any] | None,
    http_session: aiohttp.ClientSession,
    sender_queue: Queue,
) -> None:
    """Routes messages from the client to the correct session handler."""
    session_id = headers.get("SessionID")
    if not session_id:
        return

    try:
        if headers.get("To") != Target.SERVER.value:
            return

        msg_type = headers.get("Type")
        if not msg_type:
            return

        if VERBOSE:
            logging.info(
                f"[VERBOSE] Received: Type={msg_type}, SessionID={session_id}, HasAttachment={attachment is not None}"
            )

        if msg_type == MessageType.CONNECT.value:
            if session_id in SESSIONS:
                return
            try:
                host, port_str = payload_str.split(":", 1)
                port = int(port_str)
                session = Session(
                    destination=(host, port),
                    http_session=http_session,
                    sender_queue=sender_queue,
                )
                SESSIONS[session_id] = session
                peer_id = await chat_rotator.get_next_peer_id()
                await send_vk_message(
                    http_session,
                    VK_SERVER_TOKEN,
                    peer_id,
                    Target.CLIENT,
                    MessageType.ACK,
                    session_id,
                    0,
                )
                logging.info(
                    f"[{session_id}] Acknowledged connect request for {host}:{port}"
                )
            except ValueError:
                logging.error(
                    f"[{session_id}] Invalid connect payload received: {payload_str}"
                )
                return

        elif msg_type == MessageType.DATA.value:
            if session_id in SESSIONS:
                session = SESSIONS[session_id]
                data_chunk = None

                # Check if data is sent as document (attachment) or as text (Base64)
                if attachment:
                    # Document method: download the file
                    doc_url = attachment.get("url")
                    if not doc_url:
                        logging.warning(
                            f"[{session_id}] DATA message with attachment but no URL."
                        )
                        return

                    try:
                        async with http_session.get(doc_url) as response:
                            if response.status == 200:
                                data_chunk = await response.read()
                            else:
                                logging.error(
                                    f"[{session_id}] Failed to download chunk, status: {response.status}"
                                )
                                return
                    except aiohttp.ClientError as e:
                        logging.error(
                            f"[{session_id}] Network error downloading chunk: {e}"
                        )
                        return

                elif payload_str:
                    # Text message method: decode Base64
                    try:
                        data_chunk = base64.b64decode(payload_str)
                        if VERBOSE:
                            logging.info(
                                f"[{session_id}] Received TEXT message with {len(data_chunk)} bytes"
                            )
                    except Exception as e:
                        logging.error(
                            f"[{session_id}] Failed to decode Base64 text message: {e}"
                        )
                        return
                else:
                    logging.warning(
                        f"[{session_id}] DATA message without attachment or payload."
                    )
                    return

                # Get metrics for recording
                metrics = get_session_metrics(session_id)
                metrics.record_receive(len(data_chunk), success=True)

                # Process the data chunk (might be multi-part)
                try:
                    seq = int(headers.get("MessageID", "-1"))

                    # Check if this is a multi-part message
                    part_header = headers.get("Part")
                    if part_header:
                        # Multi-part message: buffer parts and reassemble
                        try:
                            part_idx, total_parts = map(int, part_header.split("/"))

                            # Initialize buffers if needed
                            if session_id not in MULTIPART_BUFFERS:
                                MULTIPART_BUFFERS[session_id] = {}
                            if seq not in MULTIPART_BUFFERS[session_id]:
                                MULTIPART_BUFFERS[session_id][seq] = {}

                            # Store this part
                            MULTIPART_BUFFERS[session_id][seq][part_idx] = data_chunk

                            # Check if we have all parts
                            if len(MULTIPART_BUFFERS[session_id][seq]) == total_parts:
                                # Reassemble all parts in order
                                complete_data = b"".join(
                                    MULTIPART_BUFFERS[session_id][seq][i]
                                    for i in range(total_parts)
                                )

                                # Clean up buffer
                                del MULTIPART_BUFFERS[session_id][seq]

                                # Send to queue
                                await session.incoming_queue.put((seq, complete_data))

                                if VERBOSE:
                                    logging.info(
                                        f"[{session_id}] Reassembled {total_parts} parts for seq {seq}, total {len(complete_data)} bytes"
                                    )
                            else:
                                if VERBOSE:
                                    logging.info(
                                        f"[{session_id}] Buffered part {part_idx}/{total_parts} for seq {seq}"
                                    )
                                return  # Wait for more parts

                        except (ValueError, KeyError) as e:
                            logging.error(
                                f"[{session_id}] Invalid Part header: {part_header}, error: {e}"
                            )
                            return
                    else:
                        # Single-part message: send directly to queue
                        await session.incoming_queue.put((seq, data_chunk))

                except (ValueError, TypeError):
                    logging.error(
                        f"[{session_id}] Invalid MessageID received: {headers.get('MessageID')}"
                    )
                    return

                if not session.writer_task_started:
                    session.writer_task_started = True
                    asyncio.create_task(server_writer_task(session_id, session))

        elif msg_type == MessageType.CLOSE.value:
            session = SESSIONS.pop(session_id, None)
            if session and session.writer_task_started:
                await session.incoming_queue.put((-1, None))  # Sentinel to stop writer
            logging.info(f"[{session_id}] Session terminated by client.")

            # Cleanup metrics on session close
            cleanup_session_metrics(session_id)

    except Exception as e:
        logging.error(
            f"Could not process incoming message for session {session_id}: {e}"
        )


# --- Main Loop ---


async def main() -> None:
    global chat_rotator

    logging.info("Starting VK-based Remote Server...")
    try:
        # This check is now implicitly handled by config.py, but we can keep it as a safeguard.
        if not all([VK_SERVER_TOKEN, VK_SERVER_GROUP_ID, VK_CHAT_PEER_IDS]):
            logging.critical("One or more required environment variables are not set.")
            return
    except NameError:
        # This will catch if config.py failed to define the variables
        logging.critical(
            "Required configuration variables are not defined. Check config.py and .env file."
        )
        return

    # Инициализируем ротатор чатов
    chat_rotator = ChatRotator(VK_CHAT_PEER_IDS)
    logging.info(
        f"Initialized chat rotator with {len(VK_CHAT_PEER_IDS)} peer IDs: {VK_CHAT_PEER_IDS}"
    )

    sender_queue = Queue(maxsize=SENDER_QUEUE_MAX_SIZE)

    # Configure connection pooling for better performance
    connector = aiohttp.TCPConnector(
        limit=CONNECTION_POOL_LIMIT,
        limit_per_host=CONNECTION_POOL_LIMIT_PER_HOST,
        ttl_dns_cache=300,
    )
    logging.info(
        f"Connection pool configured: limit={CONNECTION_POOL_LIMIT}, per_host={CONNECTION_POOL_LIMIT_PER_HOST}"
    )

    async with aiohttp.ClientSession(connector=connector) as http_session:

        async def sender_worker(worker_id: int, queue: Queue, token: str):
            """Воркер для параллельной отправки сообщений."""
            logging.info(f"[Worker-{worker_id}] Sender worker started.")
            try:
                while True:
                    session_id, sequence_number, data_chunk, target = await queue.get()
                    try:
                        # Получаем следующий peer_id циклически
                        peer_id = await chat_rotator.get_next_peer_id()
                        logging.info(
                            f"[Worker-{worker_id}][{session_id}] Sending {len(data_chunk)} bytes for seq {sequence_number} to peer {peer_id}."
                        )
                        await upload_and_send_chunk(
                            http_session,
                            token,
                            peer_id,
                            target,
                            session_id,
                            sequence_number,
                            data_chunk,
                        )
                    except VkontakteApiError:
                        logging.error(
                            f"[Worker-{worker_id}][{session_id}] Sender failed for seq {sequence_number}."
                        )
                    except (RuntimeError, aiohttp.ClientError) as e:
                        logging.debug(
                            f"[Worker-{worker_id}][{session_id}] Session closed during send: {e}"
                        )
                    except Exception as e:
                        logging.error(
                            f"[Worker-{worker_id}][{session_id}] Unexpected error: {e}"
                        )
                    finally:
                        queue.task_done()
            except asyncio.CancelledError:
                logging.info(f"[Worker-{worker_id}] Sender worker cancelled.")
                raise

        # Создаем пул воркеров для параллельной отправки
        logging.info(f"Creating {NUM_SENDER_WORKERS} sender workers...")
        worker_tasks = []
        for worker_id in range(NUM_SENDER_WORKERS):
            task = asyncio.create_task(
                sender_worker(worker_id, sender_queue, VK_SERVER_TOKEN)
            )
            worker_tasks.append(task)

        async def handler_with_queue(
            headers, payload, attachment, http_session_from_poll
        ):
            await handle_client_message(
                headers, payload, attachment, http_session, sender_queue
            )

        long_poll_task = asyncio.create_task(
            start_long_poll_listener(
                http_session,
                VK_SERVER_TOKEN,
                VK_SERVER_GROUP_ID,
                handler_with_queue,
                peer_ids=VK_CHAT_PEER_IDS,
            )
        )

        try:
            await long_poll_task
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            # Graceful shutdown
            logging.info("Shutting down: stopping long poll listener...")
            if long_poll_task and not long_poll_task.done():
                long_poll_task.cancel()
                await asyncio.gather(long_poll_task, return_exceptions=True)

            logging.info(
                f"Waiting for {sender_queue.qsize()} items in sender queue to be processed..."
            )
            await sender_queue.join()  # Wait for the queue to be empty

            logging.info("Shutting down worker tasks...")
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            logging.info("All worker tasks stopped.")

            # Report global statistics before shutdown
            report_global_statistics()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    args = parser.parse_args()
    if args.verbose:
        VERBOSE = True
        logging.getLogger().setLevel(logging.DEBUG)  # Or another level

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nShutting down.")
