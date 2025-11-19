import asyncio
import logging
import urllib.parse
from typing import Any, Callable, Dict

import aiohttp

API_VERSION = "5.131"


class VkontakteApiError(Exception):
    """Custom exception for VK API errors."""

    pass


async def api_call(
    session: aiohttp.ClientSession,
    method: str,
    params: Dict[str, Any],
    access_token: str,
) -> Dict[str, Any]:
    """Performs a generic, authenticated call to the VK API."""
    final_params = params.copy()
    final_params["v"] = API_VERSION
    final_params["access_token"] = access_token
    encoded_params = urllib.parse.urlencode(final_params)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # Retry mechanism for API calls
    max_retries = 5
    for attempt in range(max_retries):
        try:
            async with session.post(
                f"https://api.vk.com/method/{method}",
                data=encoded_params,
                headers=headers,
            ) as response:
                data = await response.json()
                if "error" in data:
                    error_info = data["error"]
                    # Specific handling for flood control
                    if error_info.get("error_code") == 9:  # Flood control
                        logging.warning(
                            f"Flood control on API method '{method}'. Retrying in 5s..."
                        )
                        await asyncio.sleep(5)
                        continue  # Retry
                    else:
                        logging.error(
                            f"--- VK API Error in method '{method}': {error_info} ---"
                        )
                        raise VkontakteApiError(error_info)
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(
                f"Network error calling method '{method}': {e}. Attempt {attempt + 1}/{max_retries}. Retrying..."
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (attempt + 1))
            else:
                raise VkontakteApiError(
                    f"Failed to call method '{method}' after {max_retries} retries."
                )

    raise VkontakteApiError(
        f"Failed to call method '{method}' after {max_retries} retries."
    )


async def start_long_poll_listener(
    http_session: aiohttp.ClientSession,
    access_token: str,
    group_id: int,
    message_handler_callback: Callable,
    peer_ids: list[int] = None,
) -> None:
    """Starts a generic Long Poll listener for a group.

    Args:
        http_session: aiohttp session
        access_token: VK access token
        group_id: VK group ID
        message_handler_callback: callback function to handle messages
        peer_ids: список peer ID для фильтрации сообщений (если None - принимаются все)
    """
    from protocol import parse_message  # Local import to avoid circular dependency

    logging.info(f"Starting Group Long Poll listener for group_id {group_id}...")
    if peer_ids:
        logging.info(f"Listening to peer IDs: {peer_ids}")
    try:
        lp_details_resp = await api_call(
            http_session,
            "groups.getLongPollServer",
            {"group_id": group_id},
            access_token,
        )
        lp_details = lp_details_resp["response"]
        server, key, ts = lp_details["server"], lp_details["key"], lp_details["ts"]
        logging.info("Group Long Poll server details obtained.")
    except (VkontakteApiError, aiohttp.ClientError) as e:
        logging.critical(f"Could not get Group Long Poll server details: {e}. Exiting.")
        return

    while True:
        try:
            async with http_session.get(
                f"{server}?act=a_check&key={key}&ts={ts}&wait=25", timeout=30
            ) as response:
                resp_data = await response.json()

            if "updates" in resp_data:
                for update in resp_data["updates"]:
                    if update.get("type") == "message_new":
                        message_obj = update["object"]["message"]

                        # Фильтруем сообщения по peer_id, если список задан
                        if peer_ids is not None:
                            msg_peer_id = message_obj.get("peer_id")
                            if msg_peer_id not in peer_ids:
                                continue  # Пропускаем сообщения из других чатов

                        headers, payload, attachment = parse_message(message_obj)
                        asyncio.create_task(
                            message_handler_callback(
                                headers, payload, attachment, http_session
                            )
                        )
                ts = resp_data.get("ts", ts)
            elif "failed" in resp_data:
                logging.error(
                    f"Long poll error: {resp_data}. Re-requesting server details."
                )
                await asyncio.sleep(5)
                lp_details_resp = await api_call(
                    http_session,
                    "groups.getLongPollServer",
                    {"group_id": group_id},
                    access_token,
                )
                lp_details = lp_details_resp["response"]
                server, key, ts = (
                    lp_details["server"],
                    lp_details["key"],
                    lp_details["ts"],
                )

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Network error in Long Poll loop: {e}. Retrying...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Unknown error in Long Poll loop: {e}. Retrying...")
            await asyncio.sleep(10)
