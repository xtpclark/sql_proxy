from abc import ABC, abstractmethod
import asyncio
from typing import Optional, List, Dict, Any
from ..connectors.base import DatabaseConnector

class ProtocolHandler(ABC):
    @abstractmethod
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, connector: DatabaseConnector) -> None:
        pass
