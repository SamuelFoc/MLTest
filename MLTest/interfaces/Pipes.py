from abc import ABC, abstractmethod
from typing import Any

class Pipe(ABC):
    @abstractmethod
    def run(self) -> Any:
        pass