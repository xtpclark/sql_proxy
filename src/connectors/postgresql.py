import asyncio
import asyncpg
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Optional
from .base import DatabaseConnector
from ..config import CONFIG

logger = logging.getLogger("SQLProxy.PostgreSQLConnector")

class PostgreSQLConnector(DatabaseConnector):
    def __init__(self, dsn: str, pool_min: int, pool_max: int, log_table_name: str):
        self.dsn = dsn
        self.pool_min = pool_min
        self.pool_max = pool_max
        self.log_table_name = log_table_name
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if self.pool:
            logger.debug("PostgreSQL pool already initialized")
            return
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn, min_size=self.pool_min, max_size=self.pool_max
        )
        await self._init_log_table()
        logger.info("PostgreSQL pool initialized")

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL pool closed")
            self.pool = None

    @asynccontextmanager
    async def acquire(self):
        if not self.pool:
            raise Exception("PostgreSQL pool not initialized")
        async with self.pool.acquire() as conn:
            yield conn

    async def _init_log_table(self):
        async with self.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.log_table_name} (
                    id BIGSERIAL PRIMARY KEY,
                    query_id UUID NOT NULL,
                    proxy_connection_id UUID,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    client_ip TEXT,
                    username TEXT,
                    source_db TEXT DEFAULT 'mysql',
                    target_db TEXT DEFAULT 'postgresql',
                    original_query TEXT,
                    translated_query TEXT,
                    status TEXT,
                    error_message TEXT,
                    explain_output TEXT,
                    execution_time_ms FLOAT,
                    num_rows_returned INT,
                    affected_rows INT
                )
            """)
        logger.info(f"Log table '{self.log_table_name}' initialized")

    async def log_query(self, original_query: Optional[str], status: str,
                        translated_query: Optional[str] = None, error_message: Optional[str] = None,
                        explain_output: Optional[str] = None, execution_time_ms: Optional[float] = None,
                        client_ip: Optional[str] = None, proxy_connection_id: Optional[str] = None,
                        username: Optional[str] = None, query_id: Optional[str] = None,
                        num_rows_returned: Optional[int] = None, affected_rows: Optional[int] = None,
                        source_db: str = "mysql", target_db: str = "postgresql"):
        if not self.pool:
            logger.warning("PostgreSQL pool not available, skipping DB log")
            return

        final_query_id = query_id or str(uuid.uuid4())
        log_entry_summary = f"QID:{final_query_id} Status:{status}"
        if original_query:
            log_entry_summary += f" Query:{original_query[:50]}..."
        if translated_query and translated_query != original_query:
            log_entry_summary += f" Trans:{translated_query[:50]}..."
        if error_message:
            log_entry_summary += f" Err:{error_message[:50]}..."
        logger.debug(f"Logging to DB: {log_entry_summary}")

        for attempt in range(CONFIG.proxy_config["log_retries"]):
            try:
                async with self.acquire() as conn:
                    await conn.execute(
                        f"""
                        INSERT INTO {self.log_table_name} 
                        (query_id, proxy_connection_id, client_ip, username, source_db, target_db, 
                         original_query, translated_query, status, error_message, explain_output, 
                         execution_time_ms, num_rows_returned, affected_rows)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        """,
                        final_query_id, proxy_connection_id, client_ip, username, source_db, target_db,
                        original_query, translated_query, status, error_message, explain_output,
                        execution_time_ms, num_rows_returned, affected_rows
                    )
                return
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to log query {final_query_id} failed: {e}")
                if attempt == CONFIG.proxy_config["log_retries"] - 1:
                    logger.error(f"Failed to log query {final_query_id} after {CONFIG.proxy_config['log_retries']} attempts: {e}", exc_info=DEBUG)
                await asyncio.sleep(CONFIG.proxy_config["log_retry_delay"])
