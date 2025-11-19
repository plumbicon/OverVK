import asyncio
from typing import List


class ChatRotator:
    """
    Класс для циклического переключения между чатами при отправке сообщений.
    Асинхронный и потокобезопасный.
    """

    def __init__(self, peer_ids: List[int]):
        """
        Инициализирует ротатор с списком peer ID.

        Args:
            peer_ids: Список ID чатов для циклической отправки
        """
        if not peer_ids:
            raise ValueError("Список peer_ids не может быть пустым")

        self.peer_ids = peer_ids
        self._current_index = 0
        self._lock = asyncio.Lock()

    async def get_next_peer_id(self) -> int:
        """
        Возвращает следующий peer ID в циклической последовательности.

        Returns:
            int: Следующий peer ID для отправки сообщения
        """
        async with self._lock:
            peer_id = self.peer_ids[self._current_index]
            self._current_index = (self._current_index + 1) % len(self.peer_ids)
            return peer_id

    def get_all_peer_ids(self) -> List[int]:
        """
        Возвращает список всех peer ID.

        Returns:
            List[int]: Список всех peer ID
        """
        return self.peer_ids.copy()
