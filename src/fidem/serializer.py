from abc import ABC, abstractmethod
from typing import Any

from fidem.intents import Intent
from fidem.operations import Operation


class Serializer(ABC):
    @abstractmethod
    def serialize(self, instance: Intent[Any] | Operation[Any]) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def deserialize[T: Intent[Any] | Operation[Any]](self, data: bytes, instance_type: type[T]) -> T:
        raise NotImplementedError
