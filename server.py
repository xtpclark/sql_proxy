import asyncio
import signal
import logging
from typing import Optional
from .config import CONFIG
from .protocol_handlers.base import ProtocolHandler
from .protocol_handlers.mysql import MySQLProtocol
from .connectors.base import DatabaseConnector
from .connectors.postgresql import PostgreSQLConnector

logger = logging.getLogger("SQLProxy.Server")

class ProtocolHandlerFactory:
    handlers = {
        "mysql": MySQLProtocol,
        # Add other handlers here (e.g., "oracle": OracleProtocol)
    }

    @classmethod
    def get_handler(cls, dialect: str) -> ProtocolHandler:
        handler_class = cls.handlers.get(dialect)
        if not handler_class:
            raise ValueError(f"No protocol handler for dialect: {dialect}")
        return handler_class()

class ConnectorFactory:
    connectors = {
        "postgresql": PostgreSQLConnector,
    }

    @classmethod
    def get_connector(cls, dialect: str, config: Dict) -> DatabaseConnector:
        connector_class = cls.connectors.get(dialect)
        if not connector_class:
            raise ValueError(f"No connector for dialect: {dialect}")
        return connector_class(
            dsn=config["dsn"],
            pool_min=config.get("pool_min", 2),
            pool_max=config.get("pool_max", 10),
            log_table_name=CONFIG.proxy_config["log_table"]
        )

class ProxyServer:
    def __init__(self):
        self.server: Optional[asyncio.AbstractServer] = None
        self._stop_event = asyncio.Event()
        self.connector = ConnectorFactory.get_connector("postgresql", CONFIG.get_dialect_config("postgresql"))
        self.handler = ProtocolHandlerFactory.get_handler(CONFIG.source_dialect)

    async def start(self):
        logger.info(f"Starting Proxy Server for {CONFIG.source_dialect} on port {CONFIG.port}...")
        try:
            await self.connector.connect()
        except Exception as e:
            logger.fatal(f"Could not connect to PostgreSQL: {e}", exc_info=CONFIG.proxy_config["log_level"] == "DEBUG")
            return

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.shutdown(signal_name=s.name)))

        try:
            self.server = await asyncio.start_server(
                lambda r, w: self.handler.handle_client(r, w, self.connector),
                "0.0.0.0", CONFIG.port, backlog=100
            )
            addr = self.server.sockets[0].getsockname()
            logger.info(f"Listening for {CONFIG.source_dialect} on {addr}")
            await self._stop_event.wait()
        except Exception as e:
            logger.fatal(f"Server error: {e}", exc_info=CONFIG.proxy_config["log_level"] == "DEBUG")
        finally:
            await self.shutdown()

    async def shutdown(self, signal_name: Optional[str] = None):
        if self._stop_event.is_set():
            logger.info("Shutdown already in progress")
            return
        logger.info(f"Shutting down {'due to ' + signal_name if signal_name else ''}...")
        self._stop_event.set()
        if self.server:
            self.server.close()
            try:
                await asyncio.wait_for(self.server.wait_closed(), timeout=10.0)
                logger.info("Server closed")
            except asyncio.TimeoutError:
                logger.warning("Timeout closing server")
            self.server = None
        await self.connector.close()
        logger.info("Shutdown complete")
