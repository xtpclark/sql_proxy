import asyncio
import struct
import os
import uuid
import logging
import urllib.parse
from typing import List, Dict, Any, Tuple, Optional
from .base import ProtocolHandler
from ..config import CONFIG
from ..connectors.base import DatabaseConnector
from ..translator import Translator

logger = logging.getLogger("SQLProxy.MySQLProtocol")
DEBUG = CONFIG.proxy_config["log_level"] == "DEBUG"

# MySQL Constants
CLIENT_LONG_PASSWORD = 0x00000001
CLIENT_FOUND_ROWS = 0x00000002
CLIENT_CONNECT_WITH_DB = 0x00000008
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_TRANSACTIONS = 0x00002000
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000
CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
CLIENT_DEPRECATE_EOF = 0x01000000
DEFAULT_CAPABILITY_FLAGS = (
    CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS | CLIENT_CONNECT_WITH_DB |
    CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION |
    CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_DEPRECATE_EOF
)
SERVER_STATUS_AUTOCOMMIT = 0x0002
ER_UNKNOWN_ERROR = 1105
ER_UNSUPPORTED_COMMAND = 1295
ER_ACCESS_DENIED_ERROR = 1045
CHARSET_MAP = {"utf8mb4": 45, "utf8": 33}
MYSQL_TYPE_VAR_STRING = 0xfd

class MySQLProtocol(ProtocolHandler):
    PROTOCOL_VERSION = 10
    SERVER_STATUS_FLAGS = SERVER_STATUS_AUTOCOMMIT

    def __init__(self):
        self.config = CONFIG.get_dialect_config("mysql")
        self.server_version_bytes = self.config["server_version"].encode('utf-8')
        self.auth_plugin_name = b"mysql_native_password"
        self.connection_id_counter = 0
        self._schema_cache = {}
        self.last_query_for_found_rows: Optional[str] = None
        self._mock_is_ok_only = False
        self._mock_rows_data = None
        self._mock_description = ""
        self.pg_connector: Optional[DatabaseConnector] = None

    def _pack_length_encoded_integer(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("Cannot pack negative integer as length-encoded.")
        if n < 251:
            return struct.pack("<B", n)
        elif n < (1 << 16):
            return b"\xfc" + struct.pack("<H", n)
        elif n < (1 << 24):
            return b"\xfd" + struct.pack("<I", n)[:3]
        return b"\xfe" + struct.pack("<Q", n)

    def _pack_length_encoded_string(self, s: Optional[bytes]) -> bytes:
        if s is None:
            return b"\xfb"
        return self._pack_length_encoded_integer(len(s)) + s

    def _build_packet(self, payload: bytes, seq_id: int) -> bytes:
        length = len(payload)
        if length > 0xFFFFFF:
            raise ValueError("Payload too large for a single MySQL packet.")
        return struct.pack("<I", length)[:3] + struct.pack("<B", seq_id) + payload

    def _build_ok_payload(self, affected_rows: int = 0, last_insert_id: int = 0,
                         status_flags: Optional[int] = None, warnings: int = 0) -> bytes:
        payload = bytearray(b"\x00")
        payload.extend(self._pack_length_encoded_integer(affected_rows))
        payload.extend(self._pack_length_encoded_integer(last_insert_id))
        status_flags = status_flags if status_flags is not None else self.SERVER_STATUS_FLAGS
        payload.extend(struct.pack("<H", status_flags))
        payload.extend(struct.pack("<H", warnings))
        return bytes(payload)

    def _build_error_payload(self, error_code: int, sql_state: str, message: str) -> bytes:
        sql_state_bytes = f"#{sql_state[:5]}".encode('utf-8')
        return b"\xff" + struct.pack("<H", error_code) + sql_state_bytes + message.encode('utf-8')

    def _build_eof_payload(self, warnings: int = 0, status_flags: Optional[int] = None) -> bytes:
        status_flags = status_flags if status_flags is not None else self.SERVER_STATUS_FLAGS
        return b"\xfe" + struct.pack("<H", warnings) + struct.pack("<H", status_flags)

    def _build_column_definition_packet(self, column_name: str, col_type: int = MYSQL_TYPE_VAR_STRING) -> bytes:
        payload = bytearray()
        payload.extend(self._pack_length_encoded_string(b"def"))
        payload.extend(self._pack_length_encoded_string(b"proxy_schema"))
        payload.extend(self._pack_length_encoded_string(b"proxy_table"))
        payload.extend(self._pack_length_encoded_string(b"proxy_table"))
        payload.extend(self._pack_length_encoded_string(column_name.encode('utf-8')))
        payload.extend(self._pack_length_encoded_string(column_name.encode('utf-8')))
        payload.extend(self._pack_length_encoded_integer(0x0c))
        payload.extend(struct.pack("<H", CHARSET_MAP.get(self.config["charset"], 45)))
        payload.extend(struct.pack("<I", 255 * 3))
        payload.append(col_type)
        payload.extend(struct.pack("<H", 0))
        payload.append(0)
        payload.extend(b"\0\0")
        return bytes(payload)

    def _build_result_set_packets(self, rows: List[Dict[str, Any]], client_capabilities: int,
                                  start_seq_id: int, column_names: Optional[List[str]] = None) -> List[bytes]:
        packets = []
        current_seq_id = start_seq_id
        columns = column_names if column_names else (list(rows[0].keys()) if rows else ["unknown"])
        logger.debug(f"Building result set: rows={len(rows)}, columns={columns}")
        num_columns = len(columns)
        packets.append(self._build_packet(self._pack_length_encoded_integer(num_columns), current_seq_id))
        current_seq_id = (current_seq_id + 1) % 256

        for col_name in columns:
            col_def_payload = self._build_column_definition_packet(col_name)
            packets.append(self._build_packet(col_def_payload, current_seq_id))
            current_seq_id = (current_seq_id + 1) % 256

        if not (client_capabilities & CLIENT_DEPRECATE_EOF):
            eof_payload = self._build_eof_payload(status_flags=self.SERVER_STATUS_FLAGS)
            packets.append(self._build_packet(eof_payload, current_seq_id))
            current_seq_id = (current_seq_id + 1) % 256

        for row_dict in rows:
            row_payload_parts = []
            for col_name in columns:
                val = row_dict.get(col_name)
                row_payload_parts.append(self._pack_length_encoded_string(str(val).encode('utf-8') if val is not None else None))
            packets.append(self._build_packet(b"".join(row_payload_parts), current_seq_id))
            current_seq_id = (current_seq_id + 1) % 256

        final_packet_payload = self._build_ok_payload(status_flags=self.SERVER_STATUS_FLAGS, warnings=0) if (client_capabilities & CLIENT_DEPRECATE_EOF) else self._build_eof_payload(status_flags=self.SERVER_STATUS_FLAGS)
        packets.append(self._build_packet(final_packet_payload, current_seq_id))
        return packets

    def _build_handshake_payload(self, connection_id: int) -> Tuple[bytes, bytes]:
        auth_plugin_data_part_1 = os.urandom(8)
        auth_plugin_data_part_2 = os.urandom(12)
        payload = bytearray()
        payload.append(self.PROTOCOL_VERSION)
        payload.extend(self.server_version_bytes + b"\0")
        payload.extend(struct.pack("<I", connection_id))
        payload.extend(auth_plugin_data_part_1 + b"\0")
        payload.extend(struct.pack("<H", DEFAULT_CAPABILITY_FLAGS & 0xFFFF))
        payload.append(CHARSET_MAP.get(self.config["charset"], 45))
        payload.extend(struct.pack("<H", self.SERVER_STATUS_FLAGS))
        payload.extend(struct.pack("<H", (DEFAULT_CAPABILITY_FLAGS >> 16) & 0xFFFF))
        payload.append(len(auth_plugin_data_part_1) + len(auth_plugin_data_part_2) + 1)
        payload.extend(b"\0" * 10)
        payload.extend(auth_plugin_data_part_2 + b"\0")
        if DEFAULT_CAPABILITY_FLAGS & CLIENT_PLUGIN_AUTH:
            payload.extend(self.auth_plugin_name + b"\0")
        full_auth_data = auth_plugin_data_part_1 + auth_plugin_data_part_2
        return bytes(payload), full_auth_data

    def _generate_connection_id(self) -> int:
        self.connection_id_counter += 1
        return self.connection_id_counter

    async def _authenticate_client(self, username: str, auth_payload: bytes, auth_plugin_data: bytes, client_ip: str, proxy_conn_id: str) -> bool:
        parsed_dsn = urllib.parse.urlparse(CONFIG.get_dialect_config("postgresql")["dsn"])
        dsn_username = urllib.parse.unquote(parsed_dsn.username) if parsed_dsn.username else "unknown"
        if username != dsn_username:
            logger.error(f"[{proxy_conn_id}] Authentication failed: Username '{username}' does not match DSN username '{dsn_username}'")
            return False
        logger.warning(f"[{proxy_conn_id}] Password validation bypassed for '{username}' from {client_ip}")
        return True

    async def _mock_show_create_table(self, query: str, database_name: str) -> None:
        table_name = query.split()[-1].strip('`;')
        async with self.pg_connector.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = current_schema() AND table_name = '{table_name.replace("'", "''")}'
                ORDER BY ordinal_position
            """)
        if not rows:
            self._mock_rows_data = [{"Table": table_name, "Create Table": f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY)"}]
            self._mock_description = f"Mocked SHOW CREATE TABLE (table not found)"
            return
        columns = []
        for row in rows:
            col_name = row['column_name']
            col_type = row['data_type']
            if col_type == 'integer' and row['column_default'] and 'nextval' in row['column_default']:
                col_type = 'INT AUTO_INCREMENT'
            elif col_type == 'character varying':
                col_type = 'VARCHAR(255)'
            elif col_type == 'timestamp without time zone':
                col_type = 'DATETIME'
            nullable = '' if row['is_nullable'] == 'NO' else 'NULL'
            default = f" DEFAULT {row['column_default']}" if row['column_default'] and 'nextval' not in row['column_default'] else ''
            columns.append(f"{col_name} {col_type}{default} {nullable}")
        primary_key = f", PRIMARY KEY ({rows[0]['column_name']})" if rows else ''
        create_stmt = f"CREATE TABLE {table_name} ({', '.join(columns)}{primary_key})"
        self._mock_rows_data = [{"Table": table_name, "Create Table": create_stmt}]
        self._mock_description = f"Mocked SHOW CREATE TABLE for {table_name}"

    async def _mock_found_rows(self, query_text: str) -> None:
        count = 0
        description = "Mocked FOUND_ROWS()"
        if self.last_query_for_found_rows and self.pg_connector:
            try:
                pg_base_query = Translator.translate_query(self.last_query_for_found_rows, "mysql", "postgresql", self.pg_connector, self)[0]
                count_query = f"SELECT COUNT(*) AS found_rows_count FROM ({pg_base_query}) AS count_alias"
                logger.debug(f"Executing FOUND_ROWS() count query: {count_query}")
                async with self.pg_connector.acquire() as conn:
                    result = await conn.fetchval(count_query)
                    if result is not None:
                        count = int(result)
                description = f"Calculated FOUND_ROWS() as {count}"
            except Exception as e:
                logger.error(f"Error calculating FOUND_ROWS(): {e}", exc_info=DEBUG)
                description = "Error calculating FOUND_ROWS(), returning placeholder"
                count = 0
            finally:
                self.last_query_for_found_rows = None
        else:
            logger.warning("SELECT FOUND_ROWS() called without prior SQL_CALC_FOUND_ROWS context")
            description = "Mocked FOUND_ROWS() (no prior SQL_CALC_FOUND_ROWS context)"
            count = 0
        self._mock_rows_data = [{"FOUND_ROWS()": count}]
        self._mock_description = description
        self._mock_is_ok_only = False

    async def _try_handle_special_query_locally(self, query: str, query_id: str, client_ip: str, proxy_conn_id: str,
                                               username: str, start_seq_id: int, connector: DatabaseConnector,
                                               client_capabilities: int) -> Optional[List[bytes]]:
        query_lower = query.lower().strip()
        packets = []
        mock_description = ""
        mock_rows_data = None
        mock_is_ok_only = False

        parsed_dsn = urllib.parse.urlparse(CONFIG.get_dialect_config("postgresql")["dsn"])
        database_name = parsed_dsn.path.lstrip('/') if parsed_dsn.path else "unknown"

        async def get_schemata():
            cache_key = "schemata"
            if cache_key not in self._schema_cache:
                async with connector.acquire() as conn:
                    rows = await conn.fetch("SELECT schema_name FROM information_schema.schemata")
                    self._schema_cache[cache_key] = [{"schema_name": row["schema_name"]} for row in rows]
            return self._schema_cache[cache_key]

        special_queries = {
            "set names": lambda q: setattr(self, '_mock_is_ok_only', True) or setattr(self, '_mock_description', f"Mocked SET NAMES: {q}"),
            "set character_set_results": lambda q: setattr(self, '_mock_is_ok_only', True) or setattr(self, '_mock_description', f"Mocked SET character_set_results: {q}"),
            "set character_set_client": lambda q: setattr(self, '_mock_is_ok_only', True) or setattr(self, '_mock_description', f"Mocked SET character_set_client: {q}"),
            "set character_set_connection": lambda q: setattr(self, '_mock_is_ok_only', True) or setattr(self, '_mock_description', f"Mocked SET character_set_connection: {q}"),
            "select @@version": lambda q: setattr(self, '_mock_rows_data', [{"@@version": self.config["server_version"], "@@version_comment": "MySQL Proxy"}]) or setattr(self, '_mock_description', "Mocked version"),
            "select @@version_comment": lambda q: setattr(self, '_mock_rows_data', [{"@@version": self.config["server_version"], "@@version_comment": "MySQL Proxy"}]) or setattr(self, '_mock_description', "Mocked version_comment"),
            "select @@session.sql_mode": lambda q: setattr(self, '_mock_rows_data', [{"@@session.sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"}]) or setattr(self, '_mock_description', "Mocked sql_mode"),
            "set session sql_mode": lambda q: setattr(self, '_mock_is_ok_only', True) or setattr(self, '_mock_description', f"Mocked SET SESSION sql_mode: {q}"),
            "select @@session.tx_isolation": lambda q: setattr(self, '_mock_rows_data', [{"@@session.tx_isolation": "READ-COMMITTED"}]) or setattr(self, '_mock_description', "Mocked tx_isolation"),
            "select @@session.transaction_isolation": lambda q: setattr(self, '_mock_rows_data', [{"@@session.transaction_isolation": "READ-COMMITTED"}]) or setattr(self, '_mock_description', "Mocked transaction_isolation"),
            "select database()": lambda q: setattr(self, '_mock_rows_data', [{"DATABASE()": database_name}]) or setattr(self, '_mock_description', "Mocked DATABASE()"),
            "select current_user()": lambda q: setattr(self, '_mock_rows_data', [{"CURRENT_USER()": f"{username}@{client_ip}"}]) or setattr(self, '_mock_description', "Mocked current_user"),
            "select user()": lambda q: setattr(self, '_mock_rows_data', [{"USER()": f"{username}@{client_ip}"}]) or setattr(self, '_mock_description', "Mocked user"),
            "show master logs": lambda q: setattr(self, '_mock_rows_data', []) or setattr(self, '_mock_description', "Mocked SHOW MASTER LOGS"),
            "show binary logs": lambda q: setattr(self, '_mock_rows_data', []) or setattr(self, '_mock_description', "Mocked SHOW BINARY LOGS"),
            "set lc_messages": lambda q: setattr(self, '_mock_is_ok_only', True) or setattr(self, '_mock_description', f"Mocked SET lc_messages: {q}"),
            "show warnings": lambda q: setattr(self, '_mock_rows_data', []) or setattr(self, '_mock_description', "Mocked SHOW WARNINGS"),
            "show full processlist": lambda q: setattr(self, '_mock_rows_data', [{"Id": 1, "User": username, "Host": client_ip, "db": database_name, "Command": "Query", "Time": 0, "State": "", "Info": q}]) or setattr(self, '_mock_description', "Mocked SHOW FULL PROCESSLIST"),
            "show create table": lambda q: self._mock_show_create_table(q, database_name),
            "show index from": lambda q: setattr(self, '_mock_rows_data', [{"Table": q.split()[-1].strip('`;'), "Key_name": "mocked_index"}]) or setattr(self, '_mock_description', "Mocked SHOW INDEX"),
            "show table status like": lambda q: setattr(self, '_mock_rows_data', [{"Name": q.split("'")[1], "Engine": "Mocked", "Rows": 0}]) or setattr(self, '_mock_description', "Mocked SHOW TABLE STATUS"),
            "show variables like": lambda q: setattr(self, '_mock_rows_data', [{"Variable_name": q.split("'")[1], "Value": "mocked"}]) or setattr(self, '_mock_description', "Mocked SHOW VARIABLES"),
            "show grants for current_user": lambda q: setattr(self, '_mock_rows_data', [{"Grants for wpuser@127.0.0.1": f"GRANT ALL ON {database_name}.* TO '{username}'@'{client_ip}'"}]) or setattr(self, '_mock_description', "Mocked SHOW GRANTS"),
            "show databases": lambda q: setattr(self, '_mock_rows_data', [{"Database": database_name}]) or setattr(self, '_mock_description', "Mocked SHOW DATABASES"),
            "show create database": lambda q: setattr(self, '_mock_rows_data', [{"Database": database_name, "Create Database": f"CREATE DATABASE {database_name}"}]) or setattr(self, '_mock_description', "Mocked SHOW CREATE DATABASE"),
            "select found_rows()": self._mock_found_rows,
        }

        self._mock_is_ok_only = False
        self._mock_rows_data = None
        self._mock_description = ""

        handler_found = False
        for prefix, handler_func in special_queries.items():
            if query_lower.startswith(prefix):
                potential_coro = handler_func(query)
                if asyncio.iscoroutine(potential_coro):
                    await potential_coro
                mock_is_ok_only = getattr(self, '_mock_is_ok_only', False)
                mock_rows_data = getattr(self, '_mock_rows_data', None)
                mock_description = getattr(self, '_mock_description', "")
                handler_found = True
                break

        if not handler_found:
            if "information_schema.schemata" in query_lower:
                mock_rows_data = await get_schemata()
                mock_description = "Mocked information_schema.schemata"
                handler_found = True
            elif "mysql.user" in query_lower:
                mock_rows_data = [{"user": username, "host": client_ip}]
                mock_description = "Mocked mysql.user"
                handler_found = True

        if not handler_found:
            return None

        logger.info(f"[{proxy_conn_id}] Mocking query: {mock_description}")
        if mock_is_ok_only:
            packets.append(self._build_packet(self._build_ok_payload(), start_seq_id))
        elif mock_rows_data is not None:
            packets.extend(self._build_result_set_packets(mock_rows_data, client_capabilities, start_seq_id))

        await asyncio.wait_for(connector.log_query(
            original_query=query, translated_query=f"LOCALLY_MOCKED ({mock_description})", status="mocked_locally",
            error_message=mock_description, client_ip=client_ip, proxy_connection_id=proxy_conn_id,
            username=username, query_id=query_id, num_rows_returned=len(mock_rows_data) if mock_rows_data else 0
        ), timeout=1.0)
        return packets

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, connector: DatabaseConnector):
        self.pg_connector = connector
        client_addr = writer.get_extra_info('peername')
        client_ip = client_addr[0] if client_addr else "unknown_ip"
        proxy_conn_id = str(uuid.uuid4())
        mysql_conn_id = self._generate_connection_id()
        logger.info(f"New connection from {client_ip} (MySQL conn_id: {mysql_conn_id}, Proxy conn_id: {proxy_conn_id})")

        client_capabilities = 0
        username = "unknown_user"
        current_seq_id = 0

        try:
            # Send Handshake
            handshake_payload, auth_plugin_data = self._build_handshake_payload(mysql_conn_id)
            writer.write(self._build_packet(handshake_payload, current_seq_id))
            await writer.drain()
            logger.debug(f"[{proxy_conn_id}] Sent handshake")
            current_seq_id = (current_seq_id + 1) % 256

            # Read Handshake Response
            header = await asyncio.wait_for(reader.readexactly(4), timeout=CONFIG.proxy_config["command_timeout"])
            payload_len = int.from_bytes(header[:3], 'little')
            client_seq_id = header[3]
            if client_seq_id != current_seq_id:
                raise ValueError(f"Unexpected client sequence ID: got {client_seq_id}, expected {current_seq_id}")

            auth_payload = await asyncio.wait_for(reader.readexactly(payload_len), timeout=CONFIG.proxy_config["command_timeout"])
            client_capabilities = struct.unpack("<I", auth_payload[:4])[0]
            username_offset = 4 + 4 + 1 + 23
            try:
                username_end = auth_payload.index(b'\0', username_offset)
                username = auth_payload[username_offset:username_end].decode('utf-8', errors='ignore')
            except ValueError:
                username = "parsing_failed_user"
                logger.warning(f"[{proxy_conn_id}] Could not parse username")

            logger.info(f"[{proxy_conn_id}] Client '{username}' connected from {client_ip}")
            if not await self._authenticate_client(username, auth_payload, auth_plugin_data, client_ip, proxy_conn_id):
                err_payload = self._build_error_payload(ER_ACCESS_DENIED_ERROR, "28000", f"Access denied for user '{username}'")
                writer.write(self._build_packet(err_payload, (client_seq_id + 1) % 256))
                await writer.drain()
                return

            # Send OK
            ok_payload = self._build_ok_payload(status_flags=self.SERVER_STATUS_FLAGS)
            current_seq_id = (client_seq_id + 1) % 256
            writer.write(self._build_packet(ok_payload, current_seq_id))
            await writer.drain()

            # Process Commands
            await self._process_commands_loop(reader, writer, connector, client_ip, proxy_conn_id, username, client_capabilities)

        except (asyncio.IncompleteReadError, ConnectionResetError):
            logger.info(f"[{proxy_conn_id}] Client {client_ip} disconnected")
        except asyncio.TimeoutError:
            logger.warning(f"[{proxy_conn_id}] Timeout for client {client_ip}")
        except Exception as e:
            logger.error(f"[{proxy_conn_id}] Error with client {client_ip}: {e}", exc_info=DEBUG)
            if not writer.is_closing():
                err_payload = self._build_error_payload(ER_UNKNOWN_ERROR, "HY000", f"Proxy error: {e}")
                writer.write(self._build_packet(err_payload, (current_seq_id + 1) % 256))
                await writer.drain()
            await connector.log_query(
                original_query=None, translated_query=None, status="proxy_connection_error",
                error_message=str(e), client_ip=client_ip, proxy_connection_id=proxy_conn_id, username=username
            )
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"[{proxy_conn_id}] Connection closed for {client_ip} (User: {username})")

    async def _process_commands_loop(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                     connector: DatabaseConnector, client_ip: str, proxy_conn_id: str,
                                     username: str, client_capabilities: int):
        command_seq_id = 0
        while True:
            try:
                header = await asyncio.wait_for(reader.readexactly(4), timeout=CONFIG.proxy_config["idle_timeout"])
                payload_len = int.from_bytes(header[:3], 'little')
                client_cmd_seq_id = header[3]
                if client_cmd_seq_id != command_seq_id:
                    logger.warning(f"[{proxy_conn_id}] Unexpected sequence ID: got {client_cmd_seq_id}, expected {command_seq_id}")
                    command_seq_id = client_cmd_seq_id

                if payload_len == 0:
                    logger.warning(f"[{proxy_conn_id}] Zero payload length packet from {client_ip}")
                    command_seq_id = 0
                    continue

                packet_payload = await asyncio.wait_for(reader.readexactly(payload_len), timeout=CONFIG.proxy_config["command_timeout"])
                cmd_type = packet_payload[0]
                cmd_data = packet_payload[1:]
                response_start_seq_id = (client_cmd_seq_id + 1) % 256
                query_id = str(uuid.uuid4())

                if cmd_type == 0x00:  # COM_SLEEP
                    logger.debug(f"[{proxy_conn_id}] COM_SLEEP from {client_ip}")
                    ok_payload = self._build_ok_payload()
                    writer.write(self._build_packet(ok_payload, response_start_seq_id))
                    await connector.log_query(
                        original_query="SLEEP", status="mocked_ok_com_sleep", client_ip=client_ip,
                        proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
                    )
                elif cmd_type == 0x01:  # COM_QUIT
                    logger.info(f"[{proxy_conn_id}] COM_QUIT from {client_ip}")
                    return
                elif cmd_type == 0x03:  # COM_QUERY
                    query = cmd_data.decode('utf-8', errors='ignore').strip()
                    logger.info(f"[{proxy_conn_id}] Query: {query[:200]}{'...' if len(query) > 200 else ''}")
                    await self._handle_com_query(writer, connector, query, query_id, client_ip, proxy_conn_id, username,
                                                 response_start_seq_id, client_capabilities)
                elif cmd_type == 0x02:  # COM_INIT_DB
                    db_name = cmd_data.decode('utf-8', errors='ignore')
                    logger.info(f"[{proxy_conn_id}] COM_INIT_DB: {db_name}")
                    ok_payload = self._build_ok_payload()
                    writer.write(self._build_packet(ok_payload, response_start_seq_id))
                    await connector.log_query(
                        original_query=f"USE {db_name}", status="mocked_ok_com_init_db", client_ip=client_ip,
                        proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
                    )
                elif cmd_type == 0x0e:  # COM_PING
                    logger.debug(f"[{proxy_conn_id}] COM_PING from {client_ip}")
                    ok_payload = self._build_ok_payload()
                    writer.write(self._build_packet(ok_payload, response_start_seq_id))
                    await connector.log_query(
                        original_query="PING", status="mocked_ok_com_ping", client_ip=client_ip,
                        proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
                    )
                else:
                    logger.warning(f"[{proxy_conn_id}] Unsupported command: {cmd_type:#02x}")
                    err_payload = self._build_error_payload(ER_UNSUPPORTED_COMMAND, "HY000", f"Unsupported command: {cmd_type:#02x}")
                    writer.write(self._build_packet(err_payload, response_start_seq_id))
                    await connector.log_query(
                        original_query=f"CMD_{cmd_type:#02x}", status="unsupported_command",
                        error_message=f"Unsupported command: {cmd_type:#02x}", client_ip=client_ip,
                        proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
                    )

                await writer.drain()
                command_seq_id = 0

            except asyncio.IncompleteReadError:
                logger.info(f"[{proxy_conn_id}] Client {client_ip} disconnected")
                return
            except asyncio.TimeoutError:
                logger.warning(f"[{proxy_conn_id}] Idle timeout for client {client_ip}")
                return
            except Exception as e:
                logger.error(f"[{proxy_conn_id}] Command error: {e}", exc_info=DEBUG)
                if not writer.is_closing():
                    err_payload = self._build_error_payload(ER_UNKNOWN_ERROR, "HY000", f"Command error: {e}")
                    writer.write(self._build_packet(err_payload, (client_cmd_seq_id + 1) % 256 if 'client_cmd_seq_id' in locals() else 0))
                    await writer.drain()
                command_seq_id = 0
                continue

    async def _handle_com_query(self, writer: asyncio.StreamWriter, connector: DatabaseConnector,
                                query: str, query_id: str, client_ip: str, proxy_conn_id: str, username: str,
                                start_seq_id: int, client_capabilities: int):
        if not query:
            writer.write(self._build_packet(self._build_ok_payload(), start_seq_id))
            await writer.drain()
            return

        query_lower = query.lower()

        await connector.log_query(
            original_query=query, status="received", client_ip=client_ip,
            proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
        )

        mocked_packets = await self._try_handle_special_query_locally(
            query, query_id, client_ip, proxy_conn_id, username, start_seq_id, connector, client_capabilities
        )
        if mocked_packets:
            for packet in mocked_packets:
                writer.write(packet)
            await writer.drain()
            return

        translated_query = None
        try:
            translated_query, translations = await Translator.translate_query(query, "mysql", "postgresql", connector, self)
            translation_info = f"Translations: {'; '.join(translations)}" if translations else "No translations applied"
            pretty_query = translated_query if DEBUG else translated_query
            logger.debug(f"[{proxy_conn_id}] Original: '{query}', Translated: '{pretty_query}' ({translation_info})")
            await connector.log_query(
                original_query=query, translated_query=translated_query, status="translated",
                error_message=translation_info, client_ip=client_ip, proxy_connection_id=proxy_conn_id,
                username=username, query_id=query_id
            )
        except Exception as e:
            logger.error(f"[{proxy_conn_id}] Translation failed: {e}", exc_info=DEBUG)
            err_payload = self._build_error_payload(ER_UNKNOWN_ERROR, "HY000", f"Translation error: {e}")
            writer.write(self._build_packet(err_payload, start_seq_id))
            await connector.log_query(
                original_query=query, status="translation_failed", error_message=str(e),
                client_ip=client_ip, proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
            )
            await writer.drain()
            return

        if translated_query is None or translated_query.strip() == "":
            logger.error(f"[{proxy_conn_id}] QID: {query_id}. Translation resulted in an empty query for original: '{query[:100]}...'")
            err_payload = self._build_error_payload(
                ER_UNKNOWN_ERROR, "HY000", f"Unsupported query or translation failed: {query[:60]}..."
            )
            writer.write(self._build_packet(err_payload, start_seq_id))
            await connector.log_query(
                original_query=query, status="translation_empty_error",
                error_message="Translated query was empty, likely unsupported.",
                client_ip=client_ip, proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
            )
            await writer.drain()
            return

        async with connector.acquire() as pg_conn:
            try:
                start_time = time.monotonic()
                is_select_like = translated_query.strip().upper().startswith(("SELECT", "SHOW", "DESC", "EXPLAIN", "VALUES"))
                pg_rows = None
                affected_rows = 0
                pg_status = None

                if is_select_like:
                    logger.debug(f"Executing translated query: {translated_query}")
                    pg_rows = await pg_conn.fetch(translated_query)
                else:
                    pg_status = await pg_conn.execute(translated_query)
                    if pg_status:
                        parts = pg_status.split()
                        if parts[0] in ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE") and parts[-1].isdigit():
                            affected_rows = int(parts[-1])

                execution_time_ms = (time.monotonic() - start_time) * 1000
                column_names = None
                if CONFIG.proxy_config["strict_result_sets"] and is_select_like:
                    try:
                        parsed = sqlglot.parse_one(translated_query, read="postgres")
                        if isinstance(parsed, sqlglot.expressions.Select):
                            column_names = [col.alias_or_name for col in parsed.expressions]
                        elif query_lower.startswith("show full columns from"):
                            column_names = ["Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"]
                    except Exception as e:
                        logger.warning(f"[{proxy_conn_id}] Failed to parse column names for SELECT: {e}")
                        if query_lower.startswith("select ") and " from " in query_lower:
                            select_clause = query_lower.split(" from ")[0].replace("select ", "").strip()
                            if "," in select_clause:
                                column_names = [col.strip() for col in select_clause.split(",")]
                            else:
                                column_names = [select_clause.strip()]
                        else:
                            column_names = ["unknown"]

                if pg_rows is not None:
                    result_data = [dict(row) for row in pg_rows]
                    if query_lower.startswith("show tables") and not column_names:
                        column_names = ["Tables_in_database"]
                    response_packets = self._build_result_set_packets(result_data, client_capabilities, start_seq_id, column_names)
                else:
                    if query_lower.startswith("show tables") and CONFIG.proxy_config["strict_result_sets"]:
                        response_packets = self._build_result_set_packets([], client_capabilities, start_seq_id, ["Tables_in_database"])
                    else:
                        response_packets = self._build_result_set_packets([], client_capabilities, start_seq_id, column_names or ['unknown'])
                for packet in response_packets:
                    writer.write(packet)
                await connector.log_query(
                    original_query=query, translated_query=translated_query,
                    status="pg_execution_success_select" if pg_rows is not None else "pg_execution_success_dml_ddl",
                    execution_time_ms=execution_time_ms, num_rows_returned=len(pg_rows) if pg_rows is not None else None,
                    affected_rows=affected_rows if pg_rows is None else None, error_message=pg_status,
                    client_ip=client_ip, proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
                )

            except asyncpg.PostgresError as e:
                logger.error(f"[{proxy_conn_id}] PostgreSQL error: {e}", exc_info=DEBUG)
                mysql_sql_state = getattr(e, 'sqlstate', "42000") or "HY000"
                err_payload = self._build_error_payload(ER_UNKNOWN_ERROR, mysql_sql_state, str(e))
                writer.write(self._build_packet(err_payload, start_seq_id))
                await connector.log_query(
                    original_query=query, translated_query=translated_query, status="pg_execution_failed",
                    error_message=f"PGError: {e} (SQLSTATE: {mysql_sql_state})", execution_time_ms=(time.monotonic() - start_time) * 1000,
                    client_ip=client_ip, proxy_connection_id=proxy_conn_id, username=username, query_id=query_id
                )
            finally:
                await writer.drain()
