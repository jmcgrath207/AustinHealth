from typing import Union

from httpx import Response

from service_logic.record import Record


class Buffer:

    def __init__(self, mb_max_size=1):
        """
        Creates a buffer object that store data

        :param mb_max_size: Max size in Megabytes
        """

        self.max_bytes = mb_max_size * 1024 ** 2

        self.buffer = []
        self.buffer_size = 0
        self.buffer_overflow = []
        self.full_buffer = False

    @property
    def is_empty(self) -> bool:
        """
        Return boolean base on if the buffer object is empty

        :return: bool
        """
        return len(self.buffer_overflow) + len(self.buffer) == 0

    @property
    def is_full(self) -> bool:
        """
        Return boolean base on if the buffer object is full

        :return:
        """
        return self.full_buffer

    @property
    def has_items(self) -> bool:
        """
        Return boolean based on if buffer has items

        :return: bool
        """
        return self.buffer_size > 0

    def add_item(self, item: Union[float, str, int, dict, list]) -> None:
        """
        Add item to the buffer list

        :param item: Item that is added to buffer
        :return: None
        """
        size = len(str(item).encode("utf-8"))

        if size + self.buffer_size < self.max_bytes:

            self.buffer.append(item)
            self.buffer_size += size
            self.buffer_overflow = []
        else:
            if self.buffer_overflow:
                self.buffer_overflow.append(item)
            else:
                self.buffer_overflow = [item]
            self.full_buffer = True

    def flush(self) -> list:
        """
        Return current buffer list and set buffer overflow list
        as the new buffer list

        :return: list
        """
        buffer = self.buffer
        self.buffer = self.buffer_overflow
        self.buffer_size = 0
        for x in self.buffer:
            self.buffer_size += len(str(x).encode("utf-8"))
        self.full_buffer = False
        return buffer

    async def to_data_queue(self, offset: int, response: Response) -> bool:
        """
        Flushes buffer, transforms data to Record to be sent to the data queue
        then returns a boolean based on if all data has been retrieved by the API.

        :param response: Response Object from API query
        :return: bool
        """
        from service_logic import app

        json_payload = response.json()
        self.add_item({'offset': offset, 'payload': json_payload})
        if self.is_full:
            record = Record(buffer_list=self.flush())
            await app.data_queue.put(record)

        if len(json_payload) < app.api_pagination_limit:
            while True:
                # Ensure buffer is completely flushed
                if app.buffer.is_empty:
                    break
                else:
                    record = Record(buffer_list=app.buffer.flush())
                    await app.data_queue.put(record)
            return True
        else:
            return False
