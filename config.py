import os

from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()


def get_env_var(var_name: str) -> str:
    """Получает переменную окружения или вызывает исключение, если она не найдена."""
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Ошибка: переменная окружения '{var_name}' не установлена.")
    return value


# --- VK API Configuration ---

try:
    # --- Серверная часть (Community B) ---
    VK_SERVER_TOKEN = get_env_var("VK_SERVER_TOKEN")
    VK_SERVER_GROUP_ID = int(get_env_var("VK_SERVER_GROUP_ID"))

    # --- Клиентская часть (Community A) ---
    VK_CLIENT_TOKEN = get_env_var("VK_CLIENT_TOKEN")
    VK_CLIENT_GROUP_ID = int(get_env_var("VK_CLIENT_GROUP_ID"))

    # --- Общий канал (можно указать несколько через запятую) ---
    VK_CHAT_PEER_ID_RAW = get_env_var("VK_CHAT_PEER_ID")
    # Парсим список peer ID
    VK_CHAT_PEER_IDS = [
        int(peer_id.strip()) for peer_id in VK_CHAT_PEER_ID_RAW.split(",")
    ]

except ValueError as e:
    print(e)
    exit(1)
