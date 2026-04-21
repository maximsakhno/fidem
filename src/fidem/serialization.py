from abc import ABC, abstractmethod
from typing import Self


class Serializable(ABC):
    @abstractmethod
    def serialize(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        raise NotImplementedError
