import asyncio
import base64
import logging
from asyncio import Queue
from typing import Any, Callable, Coroutine, Dict, List, Tuple

import aiohttp

from chat_rotator import ChatRotator
from constants import Target
from protocol import (
    VkontakteApiError,
    get_session_metrics,
    upload_and_send_chunk,
)


async def sender_worker(
    worker_id: int,
    queue: Queue,
    http_session: aiohttp.ClientSession,
    token: str,
    chat_rotator: ChatRotator,
):
    """Воркер для параллельной отправки сообщений."""
    logging.info(f"[Worker-{worker_id}] Sender worker started.")
    try:
        while True:
            session_id, sequence_number, data_chunk, target = await queue.get()
            try:
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


def create_sender_workers(
    num_workers: int,
    queue: Queue,
    http_session: aiohttp.ClientSession,
    token: str,
    chat_rotator: ChatRotator,
) -> List[asyncio.Task]:
    """Создает и запускает пул воркеров-отправителей."""
    tasks = []
    for i in range(num_workers):
        task = asyncio.create_task(
            sender_worker(i, queue, http_session, token, chat_rotator)
        )
        tasks.append(task)
    return tasks


async def process_data_message(
    headers: Dict[str, str],
    payload_str: str,
    attachment: Dict[str, Any] | None,
    http_session: aiohttp.ClientSession,
    sessions: Dict[str, Any],
    multipart_buffers: Dict[str, Dict[int, Dict[int, bytes]]],
    verbose: bool = False,
):
    """
    Общая логика обработки DATA-сообщений для клиента и сервера.
    Скачивает данные (из аттача или payload), собирает multi-part сообщения.
    """
    session_id = headers.get("SessionID")
    if not session_id or session_id not in sessions:
        return

    session = sessions[session_id]
    data_chunk = None

    # 1. Извлечение данных
    if attachment:
        doc_url = attachment.get("url")
        if not doc_url:
            logging.warning(f"[{session_id}] DATA message with attachment but no URL.")
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
            logging.error(f"[{session_id}] Network error downloading chunk: {e}")
            return
    elif payload_str:
        try:
            data_chunk = base64.b64decode(payload_str)
            if verbose:
                logging.info(
                    f"[{session_id}] Received TEXT message with {len(data_chunk)} bytes"
                )
        except Exception as e:
            logging.error(f"[{session_id}] Failed to decode Base64 text message: {e}")
            return
    else:
        logging.warning(f"[{session_id}] DATA message without attachment or payload.")
        return

    # 2. Запись статистики и обработка
    metrics = get_session_metrics(session_id)
    metrics.record_receive(len(data_chunk), success=True)

    try:
        seq = int(headers.get("MessageID", "-1"))
        part_header = headers.get("Part")

        # 3. Сборка multi-part сообщений
        if part_header:
            try:
                part_idx, total_parts = map(int, part_header.split("/"))
                if session_id not in multipart_buffers:
                    multipart_buffers[session_id] = {}
                if seq not in multipart_buffers[session_id]:
                    multipart_buffers[session_id][seq] = {}

                multipart_buffers[session_id][seq][part_idx] = data_chunk

                if len(multipart_buffers[session_id][seq]) == total_parts:
                    complete_data = b"".join(
                        multipart_buffers[session_id][seq][i]
                        for i in range(total_parts)
                    )
                    del multipart_buffers[session_id][seq]
                    await session.incoming_queue.put((seq, complete_data))
                    if verbose:
                        logging.info(
                            f"[{session_id}] Reassembled {total_parts} parts for seq {seq}, total {len(complete_data)} bytes"
                        )
                else:
                    if verbose:
                        logging.info(
                            f"[{session_id}] Buffered part {part_idx}/{total_parts} for seq {seq}"
                        )
            except (ValueError, KeyError) as e:
                logging.error(
                    f"[{session_id}] Invalid Part header: {part_header}, error: {e}"
                )
        else:
            # Обычное, не multi-part сообщение
            await session.incoming_queue.put((seq, data_chunk))

        # Запуск задачи на запись данных в сокет, если она еще не запущена
        # (Для сервера это делается в `handle_client_message` после `CONNECT`)
        # (Для клиента это делается здесь)
        if hasattr(session, "writer_task") and session.writer_task is None:
            session.writer_task = asyncio.create_task(
                # Динамический вызов нужной функции-обработчика
                globals()[session.writer_task_function_name](session_id, session)
            )

    except (ValueError, TypeError):
        logging.error(
            f"[{session_id}] Invalid MessageID received: {headers.get('MessageID')}"
        )


async def handle_shutdown(
    shutdown_tasks: List[Coroutine],
    sender_queue: Queue,
    worker_tasks: List[asyncio.Task],
):
    """Общая логика Graceful Shutdown."""
    # 1. Останавливаем задачи, принимающие новые подключения/сообщения
    logging.info("Shutting down: stopping listeners...")
    for task in shutdown_tasks:
        task.cancel()
    await asyncio.gather(*shutdown_tasks, return_exceptions=True)

    # 2. Ждем, пока очередь отправки не опустеет
    logging.info(
        f"Waiting for {sender_queue.qsize()} items in sender queue to be processed..."
    )
    await sender_queue.join()

    # 3. Останавливаем воркеров
    logging.info("Shutting down worker tasks...")
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    logging.info("All worker tasks stopped.")
