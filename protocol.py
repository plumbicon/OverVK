import asyncio
import base64
import logging
import random
import time
from asyncio import StreamReader
from collections import deque
from typing import Any, Dict, Tuple

import aiohttp

from constants import (
    CHUNK_TIMEOUT,
    ENABLE_SESSION_STATISTICS,
    MAX_CHUNK_SIZE,
    MAX_TEXT_MESSAGE_PAYLOAD,
    TEXT_MESSAGE_THRESHOLD,
    UPLOAD_URL_CACHE_TTL,
    MessageType,
    Target,
)
from vk_api import VkontakteApiError, api_call

# --- Session Statistics ---


class SessionMetrics:
    """Collects and reports statistics for a single session."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.bytes_sent = 0
        self.bytes_received = 0
        self.packets_sent = 0
        self.packets_received = 0
        self.text_messages_sent = 0
        self.documents_sent = 0
        self.multipart_messages_sent = 0
        self.send_errors = 0
        self.receive_errors = 0

        # Latency tracking (last 10 measurements)
        self.latencies = deque(maxlen=10)

        # Packet size distribution
        self.small_packets = 0  # < 1 KB
        self.medium_packets = 0  # 1-10 KB
        self.large_packets = 0  # 10-100 KB
        self.huge_packets = 0  # > 100 KB

    def record_send(
        self, data_size: int, is_text: bool, num_parts: int = 1, success: bool = True
    ):
        """Record sent data statistics."""
        self.bytes_sent += data_size
        self.packets_sent += 1

        if not success:
            self.send_errors += 1
            return

        if is_text:
            self.text_messages_sent += num_parts
            if num_parts > 1:
                self.multipart_messages_sent += 1
        else:
            self.documents_sent += 1

        # Classify packet size
        if data_size < 1024:
            self.small_packets += 1
        elif data_size < 10240:
            self.medium_packets += 1
        elif data_size < 102400:
            self.large_packets += 1
        else:
            self.huge_packets += 1

    def record_receive(self, data_size: int, success: bool = True):
        """Record received data statistics."""
        self.bytes_received += data_size
        self.packets_received += 1

        if not success:
            self.receive_errors += 1

    def record_latency(self, latency_seconds: float):
        """Record round-trip latency."""
        self.latencies.append(latency_seconds)

    def get_duration(self) -> float:
        """Get session duration in seconds."""
        return time.time() - self.start_time

    def get_throughput_sent(self) -> float:
        """Get send throughput in KB/s."""
        duration = self.get_duration()
        if duration > 0:
            return self.bytes_sent / duration / 1024
        return 0.0

    def get_throughput_received(self) -> float:
        """Get receive throughput in KB/s."""
        duration = self.get_duration()
        if duration > 0:
            return self.bytes_received / duration / 1024
        return 0.0

    def get_avg_latency(self) -> float:
        """Get average latency in milliseconds."""
        if self.latencies:
            return sum(self.latencies) / len(self.latencies) * 1000
        return 0.0

    def get_packet_loss(self) -> float:
        """Get packet loss rate as percentage."""
        total = self.packets_sent + self.send_errors
        if total > 0:
            return (self.send_errors / total) * 100
        return 0.0

    def should_report_periodic(self, interval_seconds: float = 30.0) -> bool:
        """Check if it's time for periodic reporting."""
        current_time = time.time()
        if current_time - self.last_report_time >= interval_seconds:
            self.last_report_time = current_time
            return True
        return False

    def report_periodic(self):
        """Log brief periodic statistics (for long-running sessions)."""
        if not ENABLE_SESSION_STATISTICS:
            return

        duration = self.get_duration()
        logging.info(
            f"[{self.session_id}] Stats: "
            f"Duration={duration:.1f}s, "
            f"Sent={self.bytes_sent / 1024:.1f}KB({self.get_throughput_sent():.1f}KB/s), "
            f"Recv={self.bytes_received / 1024:.1f}KB({self.get_throughput_received():.1f}KB/s), "
            f"Pkts={self.packets_sent}/{self.packets_received}, "
            f"TxtMsg={self.text_messages_sent}, Docs={self.documents_sent}"
        )

    def report(self):
        """Log detailed statistics report."""
        if not ENABLE_SESSION_STATISTICS:
            return

        duration = self.get_duration()

        logging.info(f"[{self.session_id}] === Session Statistics ===")
        logging.info(f"[{self.session_id}] Duration: {duration:.2f}s")
        logging.info(
            f"[{self.session_id}] Sent: {self.bytes_sent / 1024:.2f} KB "
            f"({self.get_throughput_sent():.2f} KB/s)"
        )
        logging.info(
            f"[{self.session_id}] Received: {self.bytes_received / 1024:.2f} KB "
            f"({self.get_throughput_received():.2f} KB/s)"
        )
        logging.info(
            f"[{self.session_id}] Packets sent: {self.packets_sent}, "
            f"received: {self.packets_received}"
        )
        logging.info(
            f"[{self.session_id}] Text messages: {self.text_messages_sent}, "
            f"Documents: {self.documents_sent}, "
            f"Multi-part: {self.multipart_messages_sent}"
        )

        if self.latencies:
            logging.info(
                f"[{self.session_id}] Avg latency: {self.get_avg_latency():.2f}ms"
            )

        loss = self.get_packet_loss()
        if loss > 0:
            logging.info(
                f"[{self.session_id}] Packet loss: {loss:.2f}% "
                f"({self.send_errors}/{self.packets_sent + self.send_errors})"
            )

        logging.info(
            f"[{self.session_id}] Packet size distribution: "
            f"small(<1KB)={self.small_packets}, "
            f"medium(1-10KB)={self.medium_packets}, "
            f"large(10-100KB)={self.large_packets}, "
            f"huge(>100KB)={self.huge_packets}"
        )

        if self.send_errors > 0 or self.receive_errors > 0:
            logging.warning(
                f"[{self.session_id}] Errors: send={self.send_errors}, "
                f"receive={self.receive_errors}"
            )


# Global metrics registry: {session_id: SessionMetrics}
_session_metrics: Dict[str, SessionMetrics] = {}

# Global cache for upload URLs: {(token, peer_id): (url, timestamp)}
_upload_url_cache: Dict[Tuple[str, int], Tuple[str, float]] = {}
_cache_lock = asyncio.Lock()


async def send_as_text_message(
    http_session: aiohttp.ClientSession,
    access_token: str,
    chat_peer_id: int,
    to_id: Target,
    session_id: str,
    sequence_number: int,
    data: bytes,
    part_index: int = 0,
    total_parts: int = 1,
) -> None:
    """Sends data as a Base64-encoded text message (single part of potentially multi-part message)."""
    try:
        # Encode data as Base64
        encoded_data = base64.b64encode(data).decode("ascii")

        # Build headers
        headers = {
            "To": to_id.value,
            "Type": MessageType.DATA.value,
            "SessionID": session_id,
            "MessageID": str(sequence_number),
        }

        # Add multi-part headers if needed
        if total_parts > 1:
            headers["Part"] = f"{part_index}/{total_parts}"

        header_block = "\n".join([f"{k}: {v}" for k, v in headers.items()])

        # Message format: headers + empty line + Base64 data
        message_text = f"{header_block}\n\n{encoded_data}"

        send_params = {
            "peer_id": chat_peer_id,
            "message": message_text,
            "random_id": random.randint(0, 2**31),
        }

        await api_call(http_session, "messages.send", send_params, access_token)

    except VkontakteApiError as e:
        logging.error(
            f"[{session_id}] Failed to send text message for seq {sequence_number} part {part_index}/{total_parts}: {e}"
        )
        raise
    except Exception as e:
        logging.error(
            f"[{session_id}] Unexpected error in send_as_text_message for seq {sequence_number} part {part_index}/{total_parts}: {e}"
        )
        raise VkontakteApiError("Unexpected error during text message send.") from e


async def send_as_document(
    http_session: aiohttp.ClientSession,
    access_token: str,
    chat_peer_id: int,
    to_id: Target,
    session_id: str,
    sequence_number: int,
    data: bytes,
) -> None:
    """Uploads data as a document and sends it (for large chunks over 30 KB)."""
    try:
        # 1. Get upload server URL (with caching)
        cache_key = (access_token, chat_peer_id)
        current_time = time.time()
        upload_url = None

        # Check cache with lock
        async with _cache_lock:
            if cache_key in _upload_url_cache:
                cached_url, cached_time = _upload_url_cache[cache_key]
                if current_time - cached_time < UPLOAD_URL_CACHE_TTL:
                    upload_url = cached_url
                    logging.debug(f"[{session_id}] Using cached upload URL")

        # If not in cache or expired, fetch new URL
        if upload_url is None:
            upload_server_params = {"peer_id": chat_peer_id}
            upload_server_resp = await api_call(
                http_session,
                "docs.getMessagesUploadServer",
                upload_server_params,
                access_token,
            )
            upload_url = upload_server_resp["response"]["upload_url"]
            # Cache the URL with lock
            async with _cache_lock:
                _upload_url_cache[cache_key] = (upload_url, current_time)
            logging.debug(f"[{session_id}] Cached new upload URL")

        # 2. Upload the file
        form_data = aiohttp.FormData()
        form_data.add_field(
            "file",
            data,
            filename="chunk.dat",
            content_type="application/octet-stream",
        )

        try:
            async with http_session.post(upload_url, data=form_data) as upload_resp:
                if upload_resp.status != 200:
                    # Invalidate cache on error
                    async with _cache_lock:
                        _upload_url_cache.pop(cache_key, None)
                    raise VkontakteApiError(
                        f"File upload failed with status {upload_resp.status}"
                    )
                upload_result = await upload_resp.json()
        except Exception:
            # Invalidate cache on any error
            async with _cache_lock:
                _upload_url_cache.pop(cache_key, None)
            raise

        # 3. Save the document
        save_doc_params = {"file": upload_result["file"]}
        saved_doc_resp = await api_call(
            http_session, "docs.save", save_doc_params, access_token
        )
        doc_info = saved_doc_resp["response"]["doc"]
        attachment_str = f"doc{doc_info['owner_id']}_{doc_info['id']}"

        # 4. Send the message with the attachment
        headers = {
            "To": to_id.value,
            "Type": MessageType.DATA.value,
            "SessionID": session_id,
            "MessageID": str(sequence_number),
        }
        header_block = "\n".join([f"{k}: {v}" for k, v in headers.items()])

        send_params = {
            "peer_id": chat_peer_id,
            "message": header_block,
            "attachment": attachment_str,
            "random_id": random.randint(0, 2**31),
        }
        await api_call(http_session, "messages.send", send_params, access_token)

    except VkontakteApiError as e:
        logging.error(
            f"[{session_id}] Failed to upload and send chunk {sequence_number}: {e}"
        )
        raise  # Re-raise to be handled by the caller
    except Exception as e:
        logging.error(
            f"[{session_id}] Unexpected error in upload_and_send_chunk for seq {sequence_number}: {e}"
        )
        raise VkontakteApiError("Unexpected error during document upload.") from e


def get_session_metrics(session_id: str) -> SessionMetrics:
    """Get or create metrics for a session."""
    if not ENABLE_SESSION_STATISTICS:
        # Return a dummy metrics object if statistics are disabled
        if session_id not in _session_metrics:
            _session_metrics[session_id] = SessionMetrics(session_id)
        return _session_metrics[session_id]

    if session_id not in _session_metrics:
        _session_metrics[session_id] = SessionMetrics(session_id)
    return _session_metrics[session_id]


def cleanup_session_metrics(session_id: str):
    """Report and cleanup metrics for a session."""
    if session_id in _session_metrics:
        metrics = _session_metrics[session_id]
        metrics.report()
        del _session_metrics[session_id]


def report_global_statistics():
    """Report aggregated statistics for all active sessions."""
    if not ENABLE_SESSION_STATISTICS or not _session_metrics:
        return

    total_sessions = len(_session_metrics)
    total_bytes_sent = sum(m.bytes_sent for m in _session_metrics.values())
    total_bytes_received = sum(m.bytes_received for m in _session_metrics.values())
    total_packets_sent = sum(m.packets_sent for m in _session_metrics.values())
    total_packets_received = sum(m.packets_received for m in _session_metrics.values())
    total_text_messages = sum(m.text_messages_sent for m in _session_metrics.values())
    total_documents = sum(m.documents_sent for m in _session_metrics.values())
    total_errors = sum(
        m.send_errors + m.receive_errors for m in _session_metrics.values()
    )

    # Calculate average throughput
    total_duration = sum(m.get_duration() for m in _session_metrics.values())
    avg_throughput_sent = (
        total_bytes_sent / total_duration / 1024 if total_duration > 0 else 0
    )
    avg_throughput_received = (
        total_bytes_received / total_duration / 1024 if total_duration > 0 else 0
    )

    logging.info("=" * 60)
    logging.info("=== GLOBAL SESSION STATISTICS ===")
    logging.info(f"Active sessions: {total_sessions}")
    logging.info(
        f"Total sent: {total_bytes_sent / 1024:.2f} KB ({avg_throughput_sent:.2f} KB/s avg)"
    )
    logging.info(
        f"Total received: {total_bytes_received / 1024:.2f} KB ({avg_throughput_received:.2f} KB/s avg)"
    )
    logging.info(
        f"Total packets: sent={total_packets_sent}, received={total_packets_received}"
    )
    logging.info(f"Messages: text={total_text_messages}, documents={total_documents}")

    if total_errors > 0:
        logging.warning(f"Total errors: {total_errors}")

    # Show top 5 sessions by data sent
    top_sessions = sorted(
        _session_metrics.values(), key=lambda m: m.bytes_sent, reverse=True
    )[:5]
    if top_sessions:
        logging.info("Top sessions by data sent:")
        for i, m in enumerate(top_sessions, 1):
            logging.info(
                f"  {i}. [{m.session_id}] {m.bytes_sent / 1024:.2f} KB "
                f"({m.get_throughput_sent():.2f} KB/s)"
            )

    logging.info("=" * 60)


async def upload_and_send_chunk(
    http_session: aiohttp.ClientSession,
    access_token: str,
    chat_peer_id: int,
    to_id: Target,
    session_id: str,
    sequence_number: int,
    data_chunk: bytes,
) -> None:
    """
    Smart chunk sender: chooses optimal method based on data size.
    - Small chunks (≤ 30 KB): split into multiple text messages (~10 messages, faster than 1 document)
    - Large chunks (> 30 KB): sent as documents (more efficient for bulk data)
    """
    metrics = get_session_metrics(session_id)
    data_size = len(data_chunk)
    send_start = time.time()

    try:
        # Choose method based on data size
        if data_size <= TEXT_MESSAGE_THRESHOLD:
            # Small chunk: split into multiple text messages
            # Each message can carry ~3KB of payload (MAX_TEXT_MESSAGE_PAYLOAD)
            num_parts = (
                data_size + MAX_TEXT_MESSAGE_PAYLOAD - 1
            ) // MAX_TEXT_MESSAGE_PAYLOAD

            logging.info(
                f"[{session_id}] Sending {data_size} bytes as {num_parts} TEXT message(s) (seq {sequence_number})"
            )

            # Send each part
            for part_idx in range(num_parts):
                start = part_idx * MAX_TEXT_MESSAGE_PAYLOAD
                end = min(start + MAX_TEXT_MESSAGE_PAYLOAD, data_size)
                part_data = data_chunk[start:end]

                await send_as_text_message(
                    http_session,
                    access_token,
                    chat_peer_id,
                    to_id,
                    session_id,
                    sequence_number,
                    part_data,
                    part_index=part_idx,
                    total_parts=num_parts,
                )

            # Record success
            send_time = time.time() - send_start
            metrics.record_send(
                data_size, is_text=True, num_parts=num_parts, success=True
            )
            metrics.record_latency(send_time)

        else:
            # Large chunk: send as document (3 API calls, ~700ms, but more efficient for large data)
            logging.info(
                f"[{session_id}] Sending {data_size} bytes as DOCUMENT (seq {sequence_number})"
            )
            await send_as_document(
                http_session,
                access_token,
                chat_peer_id,
                to_id,
                session_id,
                sequence_number,
                data_chunk,
            )

            # Record success
            send_time = time.time() - send_start
            metrics.record_send(data_size, is_text=False, num_parts=1, success=True)
            metrics.record_latency(send_time)

    except VkontakteApiError:
        # Record failure
        metrics.record_send(
            data_size, is_text=data_size <= TEXT_MESSAGE_THRESHOLD, success=False
        )
        raise  # Already logged
    except Exception as e:
        # Record failure
        metrics.record_send(
            data_size, is_text=data_size <= TEXT_MESSAGE_THRESHOLD, success=False
        )
        logging.error(
            f"[{session_id}] Unexpected error in upload_and_send_chunk for seq {sequence_number}: {e}"
        )
        raise VkontakteApiError("Unexpected error during chunk send.") from e


async def send_vk_message(
    http_session: aiohttp.ClientSession,
    access_token: str,
    chat_peer_id: int,
    to_id: Target,
    msg_type: MessageType,
    session_id: str,
    sequence_number: int,
    payload: str = "",
) -> None:
    """Constructs and sends a CONTROL message (CONNECT, ACK, CLOSE)."""
    headers = {
        "To": to_id.value,
        "Type": msg_type.value,
        "SessionID": session_id,
        "MessageID": str(sequence_number),
    }
    header_block = "\n".join([f"{k}: {v}" for k, v in headers.items()])
    message_text = f"{header_block}\n\n{payload}"

    send_params = {
        "peer_id": chat_peer_id,
        "message": message_text,
        "random_id": random.randint(0, 2**31),
    }

    try:
        await api_call(http_session, "messages.send", send_params, access_token)
    except (RuntimeError, aiohttp.ClientError) as e:
        # Session is closed during shutdown, ignore
        logging.debug(
            f"Cannot send control message {msg_type.value} for session {session_id}: {e}"
        )
    except VkontakteApiError as e:
        logging.error(
            f"Failed to send control message {msg_type.value} for session {session_id}: {e}"
        )
        # Depending on desired behavior, you might want to raise this
        raise


def parse_message(
    message_obj: Dict[str, Any],
) -> Tuple[Dict[str, str], str, Dict[str, Any] | None]:
    """Parses a message object into headers, payload, and attachment."""
    message_text = message_obj.get("text", "")
    parts = message_text.split("\n\n", 1)
    header_block = parts[0]
    payload_str = parts[1].strip() if len(parts) > 1 else ""
    headers = {
        key.strip(): value.strip()
        for key, value in (
            line.split(":", 1) for line in header_block.split("\n") if ":" in line
        )
    }

    # Find the first document attachment
    attachment = None
    for att in message_obj.get("attachments", []):
        if att.get("type") == "doc":
            attachment = att["doc"]
            break

    return headers, payload_str, attachment


async def data_sender_handler(
    handler_name: str,
    session_id: str,
    reader: StreamReader,
    target: Target,
    sender_queue: asyncio.Queue,
) -> int:
    """
    Generic handler to read from a stream, buffer data, and queue it for sending.
    Returns the last sequence number used.
    """
    logging.info(
        f"[{session_id}] Starting {handler_name} handler (Centralized Queue Mode)"
    )

    # Initialize metrics for this session
    metrics = get_session_metrics(session_id)

    buffer = bytearray()
    sequence_number = 0

    try:
        while True:
            try:
                # Periodic statistics reporting for long-running sessions
                if metrics.should_report_periodic():
                    metrics.report_periodic()

                # Wait for data with a timeout.
                data = await asyncio.wait_for(
                    reader.read(MAX_CHUNK_SIZE), timeout=CHUNK_TIMEOUT
                )
                if not data:  # End of stream
                    if buffer:
                        logging.info(
                            f"[{session_id}] Queuing remaining {len(buffer)} bytes at EOF."
                        )
                        await sender_queue.put(
                            (session_id, sequence_number, bytes(buffer), target)
                        )
                        sequence_number += 1
                    break

                buffer.extend(data)

                # Отправляем буфер сразу, если он превысил максимальный размер
                # Проверяем ПОСЛЕ добавления, чтобы не терять данные
                while len(buffer) >= MAX_CHUNK_SIZE:
                    # Отправляем ровно MAX_CHUNK_SIZE байт
                    chunk_to_send = bytes(buffer[:MAX_CHUNK_SIZE])
                    logging.info(
                        f"[{session_id}] Queuing chunk of {len(chunk_to_send)} bytes (buffer full)."
                    )
                    await sender_queue.put(
                        (session_id, sequence_number, chunk_to_send, target)
                    )
                    # Удаляем отправленные данные из буфера (эффективно, на месте)
                    del buffer[:MAX_CHUNK_SIZE]
                    sequence_number += 1

            except asyncio.TimeoutError:
                # Timeout occurred. This means the stream is idle. Send what we have.
                if buffer:
                    logging.info(
                        f"[{session_id}] Queuing chunk of {len(buffer)} bytes due to timeout."
                    )
                    await sender_queue.put(
                        (session_id, sequence_number, bytes(buffer), target)
                    )
                    buffer.clear()
                    sequence_number += 1

    except (asyncio.CancelledError, ConnectionResetError):
        logging.info(f"[{session_id}] {handler_name} connection closed.")
    except Exception as e:
        logging.error(f"[{session_id}] Unexpected error in {handler_name}: {e}")
    finally:
        logging.info(f"[{session_id}] {handler_name} handler stopped.")

        # Report metrics when handler stops
        cleanup_session_metrics(session_id)

    return sequence_number
