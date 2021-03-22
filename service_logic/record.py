import functools
import operator


class Record:

    def __init__(self, buffer_list: list):
        self._buffer_list = buffer_list
        self._payload = False
        self._checkpoint_offset = False

    def evict_buffer_list(self) -> None:
        """
        Remove self._buffer_list if all data has been transformed

        :return:
        """
        if self._buffer_list and self._checkpoint_offset:
            del self._buffer_list

    @property
    def checkpoint_offset(self) -> int:
        """
        Lazy Load the offset checkpoint if it doesn't exist

        :return:
        """
        if not self._checkpoint_offset:
            self._checkpoint_offset = max([x['offset'] for x in self._buffer_list])
            self.evict_buffer_list()
        return self._checkpoint_offset

    @property
    def payload(self) -> list:
        """
        Lazy Load the flatten payload if it doesn't exist

        :return: list
        """
        if not self._payload:
            # Reduce all payloads into one list
            self._payload = functools.reduce(operator.iconcat, [x['payload'] for x in self._buffer_list], [])
            self.evict_buffer_list()
        return self._payload
