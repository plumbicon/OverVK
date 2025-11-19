import asyncio
import base64
import logging
import struct
import time
import uuid
from asyncio import Event, PriorityQueue, Queue, StreamReader, StreamWriter, Task
from typing import Any, Callable, Dict, Optional, Tuple

import aiohttp

from chat_rotator import ChatRotator
from config import (
    VK_CHAT_PEER_IDS,
    VK_CLIENT_GROUP_ID,
    VK_CLIENT_TOKEN,
    VK_SERVER_GROUP_ID,
)
from constants import (
    CONNECTION_POOL_LIMIT,
    CONNECTION_POOL_LIMIT_PER_HOST,
    MAX_PACKET_BUFFER_SIZE,
    NUM_SENDER_WORKERS,
    PACKET_BUFFER_TIMEOUT,
    SENDER_QUEUE_MAX_SIZE,
    SOCKS_SERVER_HOST,
    SOCKS_SERVER_PORT,
    SOCKS_VERSION,
    MessageType,
    SocksAtyp,
    SocksCmd,
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


# Define a structured type for a client-side session
class ClientSession:
    def __init__(self, writer: StreamWriter, http_session: aiohttp.ClientSession):
        self.writer = writer
        self.http_session = http_session
        self.ack_event = Event()
        self.incoming_queue: PriorityQueue[Tuple[int, bytes | None]] = PriorityQueue()
        self.writer_task: Optional[Task] = None


SESSIONS: Dict[str, ClientSession] = {}

# Буфер для сборки multi-part сообщений: {session_id: {seq: {part_idx: data}}}
MULTIPART_BUFFERS: Dict[str, Dict[int, Dict[int, bytes]]] = {}

# Глобальный ротатор чатов для клиента (инициализируется в main)
chat_rotator: ChatRotator

# --- Core Logic: Data Handlers ---


async def close_uplink_session(session_id: str, sequence_number: int) -> None:
    """Callback to send a CLOSE message for an uplink session."""
    if session_id in SESSIONS:
        session = SESSIONS[session_id]
        try:
            peer_id = await chat_rotator.get_next_peer_id()
            await send_vk_message(
                session.http_session,
                VK_CLIENT_TOKEN,
                peer_id,
                Target.SERVER,
                MessageType.CLOSE,
                session_id,
                sequence_number,
            )
        except (RuntimeError, aiohttp.ClientError):
            # Session is closed during shutdown, ignore
            logging.debug(
                f"[{session_id}] Cannot send CLOSE message, session is closed"
            )


async def uplink_handler(
    session_id: str,
    reader: StreamReader,
    http_session: aiohttp.ClientSession,
    sender_queue: Queue,
) -> None:
    """Wrapper for the generic data sender to handle the client's uplink."""
    sequence_number = 0
    try:
        sequence_number = await data_sender_handler(
            "Uplink",
            session_id,
            reader,
            Target.SERVER,
            sender_queue,
        )
    except asyncio.CancelledError:
        logging.debug(f"[{session_id}] Uplink handler cancelled")
        raise
    finally:
        # The sequence number for the CLOSE message should be the next one
        await close_uplink_session(session_id, sequence_number)


async def client_writer_task(session_id: str, session: ClientSession) -> None:
    """Writes data from the server to the browser socket, handling out-of-order packets."""
    next_expected_seq = 0
    packet_buffer: Dict[int, bytes] = {}
    buffer_timestamps: Dict[int, float] = {}
    writer = session.writer

    try:
        while not writer.is_closing():
            # Process buffered packets that are now in sequence
            while next_expected_seq in packet_buffer:
                payload_chunk = packet_buffer.pop(next_expected_seq)
                buffer_timestamps.pop(next_expected_seq, None)
                writer.write(payload_chunk)
                await writer.drain()
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

            seq, payload_chunk = await session.incoming_queue.get()

            if payload_chunk is None:  # Sentinel for closing
                session.incoming_queue.task_done()
                break

            # If this is the expected packet, write it immediately
            if seq == next_expected_seq:
                writer.write(payload_chunk)
                await writer.drain()
                next_expected_seq += 1
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

                packet_buffer[seq] = payload_chunk
                buffer_timestamps[seq] = time.time()

            session.incoming_queue.task_done()

    except (ConnectionResetError, asyncio.CancelledError):
        logging.info(f"[{session_id}] Client writer connection closed.")
    except Exception as e:
        logging.error(f"[{session_id}] Error in client_writer_task: {e}")
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()


async def handle_server_message(
    headers: Dict[str, str],
    payload_str: str,
    attachment: Dict[str, Any] | None,
    http_session: aiohttp.ClientSession,
) -> None:
    """Routes messages from the server to the correct session's queue."""
    session_id = headers.get("SessionID")
    if not session_id:
        return

    try:
        if headers.get("To") != Target.CLIENT.value:
            return

        msg_type = headers.get("Type")
        if not msg_type:
            return

        if msg_type == MessageType.ACK.value:
            if session_id in SESSIONS:
                session = SESSIONS[session_id]
                session.ack_event.set()
            return

        if session_id not in SESSIONS:
            return

        session = SESSIONS[session_id]

        if msg_type == MessageType.DATA.value:
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

                            logging.debug(
                                f"[{session_id}] Reassembled {total_parts} parts for seq {seq}, total {len(complete_data)} bytes"
                            )
                        else:
                            logging.debug(
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

            if session.writer_task is None:
                session.writer_task = asyncio.create_task(
                    client_writer_task(session_id, session)
                )

        elif msg_type == MessageType.CLOSE.value:
            if session.writer_task is not None:
                await session.incoming_queue.put((-1, None))  # Sentinel
            elif not session.writer.is_closing():
                session.writer.close()
                await session.writer.wait_closed()

            # Cleanup metrics on session close
            cleanup_session_metrics(session_id)

    except Exception as e:
        logging.error(f"[{session_id}] Error handling server message: {e}")


# --- SOCKS5 Proxy Logic ---


async def handle_browser_connection(
    browser_reader: StreamReader,
    browser_writer: StreamWriter,
    http_session: aiohttp.ClientSession,
    sender_queue: Queue,
) -> None:
    session_id = str(uuid.uuid4())
    session = ClientSession(writer=browser_writer, http_session=http_session)
    SESSIONS[session_id] = session
    logging.info(f"[{session_id}] New browser connection.")

    try:
        # SOCKS5 Greeting
        greeting = await browser_reader.read(2)
        ver, nmethods = struct.unpack("!BB", greeting)
        if ver != SOCKS_VERSION:
            return
        await browser_reader.read(nmethods)  # Read methods list, no auth supported
        browser_writer.write(struct.pack("!BB", SOCKS_VERSION, 0))  # No auth
        await browser_writer.drain()

        # SOCKS5 Request
        request_header = await browser_reader.read(4)
        ver, cmd, rsv, atyp = struct.unpack("!BBBB", request_header)
        if cmd != SocksCmd.CONNECT:
            return

        if atyp == SocksAtyp.DOMAINNAME:
            domain_len = (await browser_reader.read(1))[0]
            host = (await browser_reader.read(domain_len)).decode("utf-8")
        elif atyp == SocksAtyp.IPV4:
            host = ".".join(str(b) for b in await browser_reader.read(4))
        else:
            logging.warning(f"[{session_id}] Unsupported address type: {atyp}")
            return

        port = struct.unpack("!H", await browser_reader.read(2))[0]
        logging.info(f"[{session_id}] SOCKS5 CONNECT for {host}:{port}")

        # Initiate tunnel
        connect_payload = f"{host}:{port}"
        peer_id = await chat_rotator.get_next_peer_id()
        await send_vk_message(
            http_session,
            VK_CLIENT_TOKEN,
            peer_id,
            Target.SERVER,
            MessageType.CONNECT,
            session_id,
            0,
            payload=connect_payload,
        )

        logging.info(f"[{session_id}] Waiting for session acknowledgement...")
        await asyncio.wait_for(session.ack_event.wait(), timeout=60.0)

        # Send success response to browser
        # BND.ADDR and BND.PORT are left as zeros, as they are not used by most clients
        browser_writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        await browser_writer.drain()
        logging.info(f"[{session_id}] Tunnel established.")

        await uplink_handler(session_id, browser_reader, http_session, sender_queue)

    except asyncio.TimeoutError:
        logging.error(f"[{session_id}] Session acknowledgement timeout.")
    except (ConnectionResetError, asyncio.CancelledError) as e:
        logging.info(f"[{session_id}] Browser connection closed: {type(e).__name__}")
    except Exception as e:
        logging.error(f"[{session_id}] Error in connection handler: {e}")
    finally:
        session = SESSIONS.pop(session_id, None)
        if session and session.writer_task and not session.writer_task.done():
            session.writer_task.cancel()
            try:
                await session.writer_task
            except asyncio.CancelledError:
                pass

        if not browser_writer.is_closing():
            browser_writer.close()
            await browser_writer.wait_closed()

        logging.info(f"[{session_id}] Browser connection closed.")


async def main() -> None:
    global chat_rotator

    logging.info(
        f"Starting SOCKS5 Local Proxy (Client Mode) on {SOCKS_SERVER_HOST}:{SOCKS_SERVER_PORT}..."
    )
    try:
        if not all(
            [VK_CLIENT_TOKEN, VK_CLIENT_GROUP_ID, VK_SERVER_GROUP_ID, VK_CHAT_PEER_IDS]
        ):
            logging.critical("Required config values are not set in config.py.")
            return
    except NameError:
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
                sender_worker(worker_id, sender_queue, VK_CLIENT_TOKEN)
            )
            worker_tasks.append(task)

        handler: Callable = handle_server_message
        asyncio.create_task(
            start_long_poll_listener(
                http_session,
                VK_CLIENT_TOKEN,
                VK_CLIENT_GROUP_ID,
                handler,
                peer_ids=VK_CHAT_PEER_IDS,
            )
        )

        def conn_handler(reader: StreamReader, writer: StreamWriter) -> None:
            asyncio.create_task(
                handle_browser_connection(reader, writer, http_session, sender_queue)
            )

        server = await asyncio.start_server(
            conn_handler, SOCKS_SERVER_HOST, SOCKS_SERVER_PORT
        )

        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        logging.info(f"SOCKS5 Proxy started on {addrs}")

        try:
            await server.serve_forever()
        except (asyncio.CancelledError, KeyboardInterrupt):
            # This part is reached on Ctrl+C
            pass
        finally:
            # Graceful shutdown
            logging.info("Shutting down: closing server...")
            server.close()
            await server.wait_closed()

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nShutting down.")
