

sql_proxy/
├── config.yaml             # Configuration file
├── config.py              # Configuration parsing
├── protocol_handlers/     # Protocol handlers
│   ├── __init__.py
│   ├── base.py           # Abstract ProtocolHandler
│   ├── mysql.py          # MySQL protocol handler
│   └── oracle.py         # Placeholder for Oracle
├── connectors/            # Database connectors
│   ├── __init__.py
│   ├── base.py           # Abstract DatabaseConnector
│   ├── postgresql.py     # PostgreSQL connector
├── translator.py          # Query translation logic
├── server.py              # Proxy server logic
└── main.py                # Entry point


pip install PyYAML asyncpg sqlglot sqlparse
