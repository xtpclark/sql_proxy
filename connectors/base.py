from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Optional, Any

class DatabaseConnector(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @abstractmethod
    @asynccontextmanager
    async def acquire(self):
        pass

    @abstractmethod
    async def log_query(self, original_query: Optional[str], translated_query: Optional[str], status: str,
                        error_message: Optional[str] = None, client_ip: Optional[str] = None,
                        proxy_connection_id: Optional[str] = None, username: Optional[str] = None,
                        query_id: Optional[str] = None, **kwargs) -> None:
        pass
