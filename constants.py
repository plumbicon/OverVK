from enum import Enum

# --- Network and Protocol Constants ---


class MessageType(str, Enum):
    CONNECT = "connect"
    DATA = "data"
    ACK = "ack"
    CLOSE = "close"


class Target(str, Enum):
    CLIENT = "client"
    SERVER = "server"


# --- SOCKS5 Protocol Constants ---

SOCKS_VERSION = 5


class SocksCmd(int, Enum):
    CONNECT = 1
    BIND = 2
    UDP_ASSOCIATE = 3


class SocksAtyp(int, Enum):
    IPV4 = 1
    DOMAINNAME = 3
    IPV6 = 4


# --- Performance Tuning ---

# Maximum chunk size to read and send (in bytes, uncompressed)
# Максимальный размер чанка для чтения и отправки (в байтах, до сжатия)
MAX_CHUNK_SIZE = 1024 * 1024  # 1MB

# Timeout in seconds to send a partially filled buffer.
# This is crucial for interactivity on low-traffic connections.
CHUNK_TIMEOUT = 0.4

# Number of parallel sender workers for improved throughput
# Количество параллельных воркеров для отправки сообщений
NUM_SENDER_WORKERS = 8

# VK message size limit (UTF-8 characters)
# Ограничение VK на размер текстового сообщения (символов UTF-8)
VK_MESSAGE_MAX_LENGTH = 4096

# Maximum payload size for a single text message (in bytes, before Base64 encoding)
# Максимальный размер полезной нагрузки для одного текстового сообщения (байт до Base64)
# Base64 увеличивает размер на ~33%, поэтому 3000 байт -> ~4000 символов
# С учётом заголовков оставляем запас
MAX_TEXT_MESSAGE_PAYLOAD = 3000  # ~4000 chars in Base64 + headers

# Threshold for switching between text messages and documents (in bytes, uncompressed)
# Порог переключения между текстовыми сообщениями и документами (в байтах, без сжатия)
# Данные <= 30 КБ разбиваются на несколько текстовых сообщений (~10 сообщений)
# Данные > 30 КБ отправляются как документ (быстрее при больших объёмах)
TEXT_MESSAGE_THRESHOLD = 6 * 1024  # 6 KB

# Upload URL cache settings
# Настройки кэширования URL для загрузки документов
# Кэширование позволяет избежать лишних API вызовов docs.getMessagesUploadServer
UPLOAD_URL_CACHE_TTL = 300  # Time to live in seconds (5 minutes)

# Connection pooling settings
# Настройки пула соединений для aiohttp
CONNECTION_POOL_LIMIT = 100  # Maximum number of connections
CONNECTION_POOL_LIMIT_PER_HOST = 30  # Maximum connections per host

# --- Buffer and Queue Limits ---

# Maximum number of out-of-order packets to buffer before dropping oldest
MAX_PACKET_BUFFER_SIZE = 100

# Timeout in seconds for buffered packets (if not received in order)
PACKET_BUFFER_TIMEOUT = 30.0

# Maximum size of sender queue (prevents memory overflow)
SENDER_QUEUE_MAX_SIZE = 1000

# --- Local Server ---

SOCKS_SERVER_HOST = "127.0.0.1"
SOCKS_SERVER_PORT = 8888

# --- Statistics and Monitoring ---

# Enable detailed session statistics logging
ENABLE_SESSION_STATISTICS = True
