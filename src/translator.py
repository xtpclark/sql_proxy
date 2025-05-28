import sqlglot
import sqlglot.errors as sqlglot_errors
import re
import logging
import sqlparse
from typing import Tuple, List, Optional
from .config import CONFIG
from .connectors.base import DatabaseConnector

logger = logging.getLogger("SQLProxy.Translator")

class Translator:
    @staticmethod
    async def translate_query(query: str, source_dialect: str, target_dialect: str,
                             connector: DatabaseConnector, protocol_handler: Optional[Any] = None) -> Tuple[str, List[str]]:
        query = query.strip()
        translations = []
        translated_query = query
        query_upper = query.upper()
        query_lower = query.lower()

        if source_dialect.lower() == "mysql" and target_dialect.lower() == "postgresql":
            # Tokenize with sqlparse
            try:
                parsed_sqlparse = sqlparse.parse(query)
                if not parsed_sqlparse:
                    logger.warning(f"sqlparse failed to parse query: '{query[:100]}'")
                    translations.append("sqlparse failed to parse query")
                else:
                    tokens = [token.value for token in parsed_sqlparse[0].tokens if token.ttype is not sqlparse.tokens.Whitespace]
                    logger.debug(f"sqlparse tokens: {tokens[:10]}")
                    translations.append("Validated query structure with sqlparse")
            except Exception as e:
                logger.warning(f"sqlparse parsing failed: {e}")
                translations.append(f"sqlparse parsing failed: {e}")

            # Handle SHOW FULL COLUMNS FROM
            if query_lower.startswith("show full columns from"):
                match = re.match(r"show\s+full\s+columns\s+from\s+[`\"]?([a-zA-Z0-9_]+)[`\"]?(\s+.*)?", query_lower, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    translated_query = f"""
                        SELECT
                            c.column_name AS "Field",
                            CASE 
                                WHEN c.data_type = 'integer' AND c.column_default LIKE 'nextval%' THEN 'int'
                                WHEN c.data_type = 'character varying' THEN CONCAT('varchar(', COALESCE(c.character_maximum_length::text, '255'), ')')
                                WHEN c.data_type = 'timestamp without time zone' THEN 'datetime'
                                WHEN c.data_type = 'text' THEN 'text'
                                WHEN c.data_type = 'boolean' THEN 'tinyint(1)'
                                WHEN c.data_type = 'bigint' THEN 'bigint'
                                WHEN c.data_type = 'smallint' THEN 'smallint'
                                WHEN c.data_type = 'double precision' THEN 'double'
                                WHEN c.data_type = 'numeric' THEN CONCAT('decimal(', COALESCE(c.numeric_precision::text, '10'), ',', COALESCE(c.numeric_scale::text, '0'), ')')
                                ELSE c.data_type 
                            END AS "Type",
                            c.collation_name AS "Collation",
                            CASE WHEN c.is_nullable = 'YES' THEN 'YES' ELSE 'NO' END AS "Null",
                            COALESCE(conkey.key_type, '') AS "Key",
                            CASE 
                                WHEN c.column_default LIKE 'nextval%' THEN NULL
                                ELSE c.column_default
                            END AS "Default",
                            CASE 
                                WHEN c.column_default LIKE 'nextval%' THEN 'auto_increment'
                                WHEN c.is_identity = 'YES' THEN 'auto_increment identity'
                                WHEN c.is_generated = 'ALWAYS' THEN 'GENERATED ALWAYS'
                                ELSE ''
                            END AS "Extra",
                            'select,insert,update,references' AS "Privileges",
                            COALESCE(d.description, '') AS "Comment"
                        FROM information_schema.columns c
                        LEFT JOIN pg_catalog.pg_description d 
                            ON d.objoid = (
                                SELECT oid FROM pg_catalog.pg_class rel 
                                WHERE rel.relname = c.table_name 
                                AND rel.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = c.table_schema)
                            ) AND d.objsubid = c.ordinal_position
                        LEFT JOIN (
                            SELECT 
                                tc.table_schema, tc.table_name, kcu.column_name,
                                CASE 
                                    WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PRI'
                                    WHEN tc.constraint_type = 'UNIQUE' THEN 'UNI'
                                    ELSE 'MUL'
                                END AS key_type
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu 
                                ON tc.constraint_name = kcu.constraint_name 
                                AND tc.constraint_schema = kcu.constraint_schema
                            WHERE tc.table_name = '{table_name.replace("'", "''")}' 
                            AND tc.table_schema = current_schema()
                            AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                        ) conkey ON c.table_schema = conkey.table_schema 
                            AND c.table_name = conkey.table_name 
                            AND c.column_name = conkey.column_name
                        WHERE c.table_schema = current_schema() 
                        AND c.table_name = '{table_name.replace("'", "''")}'
                        ORDER BY c.ordinal_position
                    """
                    translations.append(f"Translated SHOW FULL COLUMNS FROM {table_name} to information_schema query")
                    return translated_query, translations

            # Handle DESCRIBE or DESC
            if query_lower.startswith(("describe ", "desc ")):
                parts = query.split(maxsplit=1)
                if len(parts) > 1:
                    table_name = parts[1].strip(';`')
                    translated_query = f"""
                        SELECT 
                            column_name AS "Field",
                            CASE 
                                WHEN data_type = 'integer' AND column_default LIKE 'nextval%' THEN 'int auto_increment'
                                WHEN data_type = 'character varying' THEN CONCAT('varchar(', COALESCE(character_maximum_length::text, '255'), ')')
                                WHEN data_type = 'timestamp without time zone' THEN 'datetime'
                                ELSE data_type
                            END AS "Type",
                            CASE WHEN is_nullable = 'YES' THEN 'YES' ELSE 'NO' END AS "Null",
                            CASE WHEN column_name IN (
                                SELECT column_name 
                                FROM information_schema.key_column_usage 
                                WHERE table_name = '{table_name.replace("'", "''")}' 
                                AND constraint_name LIKE '%_pkey'
                            ) THEN 'PRI' ELSE '' END AS "Key",
                            COALESCE(
                                CASE 
                                    WHEN column_default LIKE 'nextval%' THEN NULL
                                    WHEN column_default IS NOT NULL THEN column_default
                                    ELSE ''
                                END, ''
                            ) AS "Default",
                            CASE 
                                WHEN column_default LIKE 'nextval%' THEN 'auto_increment'
                                ELSE ''
                            END AS "Extra"
                        FROM information_schema.columns 
                        WHERE table_schema = current_schema() AND table_name = '{table_name.replace("'", "''")}' 
                        ORDER BY ordinal_position
                    """
                    translations.append(f"Translated DESCRIBE {table_name} to MySQL-compatible information_schema query")
                    return translated_query, translations

            # Handle SHOW TABLES
            if query_lower == "show tables":
                translated_query = "SELECT tablename AS \"Tables_in_database\" FROM pg_catalog.pg_tables WHERE schemaname = current_schema()"
                translations.append("Translated SHOW TABLES to pg_catalog.pg_tables")
                return translated_query, translations
            elif query_lower.startswith("show tables like "):
                pattern_raw = query_lower.split("show tables like ", 1)[1].strip("'")
                pattern = pattern_raw.rstrip(';').replace("''", "'")
                translated_query = f"SELECT tablename AS \"Tables_in_database\" FROM pg_catalog.pg_tables WHERE schemaname = current_schema() AND tablename LIKE '{pattern.replace("'", "''")}'"
                translations.append(f"Translated SHOW TABLES LIKE '{pattern}' to pg_catalog.pg_tables with LIKE")
                if CONFIG.proxy_config["force_ilike"]:
                    lower_query = f"SELECT tablename AS \"Tables_in_database\" FROM pg_catalog.pg_tables WHERE schemaname = current_schema() AND LOWER(tablename) LIKE LOWER('{pattern.replace("'", "''")}')"
                    logger.debug(f"Testing LOWER query: {lower_query}")
                    try:
                        async with connector.acquire() as conn:
                            test_rows = await conn.fetch(lower_query)
                        if test_rows:
                            translated_query = lower_query
                            translations.append("Used LOWER(... LIKE ...) for case-insensitive matching")
                        else:
                            translations.append("LOWER(... LIKE ...) returned no rows, using LIKE")
                    except Exception as e:
                        logger.warning(f"LOWER(... LIKE ...) test failed: {e}, using LIKE")
                logger.debug(f"Final translated SHOW TABLES LIKE '{pattern}' to: {translated_query}")
                translations.append(f"Translated SHOW TABLES LIKE '{pattern}' to pg_catalog.pg_tables")
                return translated_query, translations

            try:
                # Parse query for SELECT DISTINCT with ORDER BY
                parsed = sqlglot.parse_one(query, read="mysql")
                if isinstance(parsed, sqlglot.expressions.Select) and parsed.args.get('distinct'):
                    order_node = parsed.args.get('order')
                    order_by_expressions = order_node.expressions if order_node and hasattr(order_node, 'expressions') else []
                    select_expressions = [col.sql(dialect="mysql") for col in parsed.expressions]
                    missing_cols = []
                    for order_exp in order_by_expressions:
                        actual_expression = order_exp.this
                        order_sql = actual_expression.sql(dialect="mysql")
                        is_in_select = any(
                            order_sql == sel or
                            (hasattr(actual_expression, 'alias_or_name') and actual_expression.alias_or_name in select_expressions)
                            for sel in select_expressions
                        )
                        if not is_in_select:
                            missing_cols.append(actual_expression)
                    if missing_cols:
                        for col in missing_cols:
                            parsed.select(col.copy(), append=True)
                        translated_query = parsed.sql(dialect="postgres", pretty=False)
                        translations.append(
                            f"Added ORDER BY columns {', '.join(col.sql(dialect='mysql') for col in missing_cols)} "
                            f"to SELECT list for DISTINCT compatibility"
                        )
                    else:
                        translated_query = sqlglot.transpile(query, read="mysql", write="postgres", pretty=False)[0]
                else:
                    translated_query = sqlglot.transpile(query, read="mysql", write="postgres", pretty=False)[0]
                translations.append("Transpiled using sqlglot (MySQL to PostgreSQL)")

                # Post-process for ON DUPLICATE KEY UPDATE
                if "ON DUPLICATE KEY UPDATE" in query_upper:
                    table_match = re.search(r'INSERT INTO\s+([`"]?[^`"\s]+[`"]?)\s*\(', query, re.IGNORECASE)
                    table = table_match.group(1).strip('`"') if table_match else None
                    logger.debug(f"Table detection for INSERT: matched table='{table}' in query='{query[:100]}'")
                    if table:
                        conflict_target = None
                        if table.lower() == "wp_users":
                            conflict_target = "user_login"
                        else:
                            async with connector.acquire() as conn:
                                rows = await conn.fetch(f"""
                                    SELECT column_name
                                    FROM information_schema.key_column_usage
                                    WHERE table_name = '{table.replace("'", "''")}' 
                                    AND (constraint_name LIKE '%_pkey' OR constraint_name LIKE '%_key')
                                    LIMIT 1
                                """)
                                conflict_target = rows[0]["column_name"] if rows else None
                                logger.debug(f"Conflict target for table '{table}': {conflict_target}")

                        if conflict_target:
                            update_clause = re.search(r'\bON DUPLICATE KEY UPDATE\b\s*([^;]*?)(;)?$', query, re.IGNORECASE)
                            if update_clause:
                                assignments = update_clause.group(1).split(',')
                                column_names = []
                                for assignment in assignments:
                                    col_match = re.match(r'^\s*([`]?[^`=\s]+[`]?)\s*=\s*VALUES\s*\(\s*[`]?([^`)]+)[`]?\s*\)', assignment.strip(), re.IGNORECASE)
                                    if col_match:
                                        col_name = col_match.group(1).strip('`')
                                        column_names.append(col_name)
                                        logger.debug(f"Extracted column name: '{col_name}' from assignment: '{assignment.strip()}'")
                                if column_names:
                                    translated_query = re.sub(
                                        r'\bON DUPLICATE KEY UPDATE\b\s*([^;]*?)(;)?$',
                                        f'ON CONFLICT ("{conflict_target}") DO UPDATE SET ' + ', '.join(
                                            f'"{col}" = EXCLUDED."{col}"' for col in column_names
                                        ) + (update_clause.group(2) or ''),
                                        translated_query,
                                        flags=re.IGNORECASE
                                    )
                                    translations.append(f"Post-processed ON DUPLICATE KEY UPDATE to ON CONFLICT for table {table} with columns {column_names}")
                                else:
                                    logger.warning(f"No valid columns extracted from ON DUPLICATE KEY UPDATE clause in query: '{query[:100]}'")
                                    translations.append("No valid columns extracted for ON DUPLICATE KEY UPDATE")
                            else:
                                logger.warning(f"Failed to parse ON DUPLICATE KEY UPDATE clause in query: '{query[:100]}'")
                                translations.append("Failed to parse ON DUPLICATE KEY UPDATE clause")
                        else:
                            logger.warning(f"No unique key found for table {table}, using translated query")
                            translations.append(f"No unique key found for {table}")
                    else:
                        logger.warning(f"Could not determine table for ON DUPLICATE KEY UPDATE in query: '{query[:100]}'")
                        translations.append("Table detection failed for ON DUPLICATE KEY UPDATE")

                # Post-process for SQL_CALC_FOUND_ROWS
                if "SQL_CALC_FOUND_ROWS" in query_upper:
                    base_query = re.sub(r'\bLIMIT\b.*$', '', query, flags=re.IGNORECASE)
                    base_query = re.sub(r'\bSQL_CALC_FOUND_ROWS\b\s*', '', base_query, flags=re.IGNORECASE)
                    protocol_handler.last_query_for_found_rows = base_query
                    translated_query = re.sub(r'\bSQL_CALC_FOUND_ROWS\b\s*', '', translated_query, flags=re.IGNORECASE)
                    translations.append("Removed SQL_CALC_FOUND_ROWS from SELECT query")

                if CONFIG.proxy_config["force_ilike"]:
                    original_query = translated_query
                    translated_query = re.sub(r'\bLIKE\b', 'ILIKE', translated_query, flags=re.IGNORECASE)
                    if translated_query != original_query:
                        translations.append("Replaced LIKE with ILIKE for case-insensitive matching")

                return translated_query, translations

            except sqlglot_errors.SqlglotError as e:
                logger.warning(f"sqlglot failed to parse query: '{query[:100]}'. Falling back to sqlparse: {e}")
                try:
                    parsed_sqlparse = sqlparse.parse(query)
                    if parsed_sqlparse and len(parsed_sqlparse) == 1:
                        statement = parsed_sqlparse[0]
                        if statement.get_type() == "INSERT" and "ON DUPLICATE KEY UPDATE" in query_upper:
                            table_match = re.search(r'INSERT INTO\s+([`"]?[^`"\s]+[`"]?)\s*\(', query, re.IGNORECASE)
                            table = table_match.group(1).strip('`"') if table_match else None
                            logger.debug(f"Fallback: Table detection for INSERT: matched table='{table}' in query='{query[:100]}'")
                            if table:
                                conflict_target = "user_login" if table.lower() == "wp_users" else None
                                if not conflict_target:
                                    async with connector.acquire() as conn:
                                        rows = await conn.fetch(f"""
                                            SELECT column_name
                                            FROM information_schema.key_column_usage
                                            WHERE table_name = '{table.replace("'", "''")}' 
                                            AND (constraint_name LIKE '%_pkey' OR constraint_name LIKE '%_key')
                                            LIMIT 1
                                        """)
                                        conflict_target = rows[0]["column_name"] if rows else "id"
                                        logger.debug(f"Fallback: Conflict target for table '{table}': {conflict_target}")
                                update_clause = re.search(r'\bON DUPLICATE KEY UPDATE\b\s*([^;]*?)[0]', query, re.IGNORECASE)
                                if update_clause:
                                    assignments = update_clause.group(1).split(',')
                                    column_names = []
                                    for assignment in assignments:
                                        col_match = re.match(r'^\s*([`]?[^`=\s]+[`]?)\s*=\s*VALUES\s*\(\s*[`]?([^`)]+)[`]?\s*\)', assignment.strip(), re.IGNORECASE)
#                                        col_match = re.match(r'^\s*([`]?[^`=\s]+[`]?)\s*=\s*VALUES\s*\(\s*[`]?([^`)]+)[`]?)\s*\)', query=assignment.strip(), re.IGNORECASE)
                                        if col_match:
                                            col_name = col_match.group(1).strip('`')
                                            column_names.append(col_name)
                                            logger.debug(f"Fallback: Extracted column name: '{col_name}' from assignment: '{assignment.strip()}'")
                                    if column_names:
                                        translated_query = re.sub(
                                            r'\bON DUPLICATE KEY UPDATE\b\s*([^;]*?)(;)?$',
                                            f'ON CONFLICT ("{conflict_target}") DO UPDATE SET ' + ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in column_names)
                                            + (update_clause.group(2) or ''),
                                            query,
                                            flags=re.IGNORECASE
                                        )
                                        translated_query = sqlglot.transpile(translated_query, read="mysql", write="postgres")[0]
                                        translations.append(f"Fallback: Translated INSERT ... ON DUPLICATE KEY to PostgreSQL ON CONFLICT for table {table} with columns {column_names}")
                                        return translated_query, translations
                                    else:
                                        logger.warning(f"Fallback: No valid columns extracted from ON DUPLICATE KEY UPDATE clause in query: '{query[:100]}'")
                                        translations.append("Fallback: No valid columns extracted for ON DUPLICATE KEY UPDATE")
                                else:
                                    logger.warning(f"Fallback: Failed to parse ON DUPLICATE KEY UPDATE clause in query: '{query[:100]}'")
                                    translations.append("Fallback: Failed to parse ON DUPLICATE KEY UPDATE clause")
                        elif statement.get_type() == "SELECT" and "SQL_CALC_FOUND_ROWS" in query_upper:
                            base_query = re.sub(r'\bLIMIT\b.*$', '', query, flags=re.IGNORECASE)
                            base_query = re.sub(r'\bSQL_CALC_FOUND_ROWS\b\s*', '', base_query, flags=re.IGNORECASE)
                            protocol_handler.last_query_for_found_rows = base_query
                            translated_query = re.sub(r'\bSQL_CALC_FOUND_ROWS\b\s*', '', query, flags=re.IGNORECASE)
                            translated_query = sqlglot.transpile(translated_query, read="mysql", write="postgres")[0]
                            translations.append("Fallback: Removed SQL_CALC_FOUND_ROWS using sqlparse")
                            if CONFIG["proxy"]["force_i"]:
                                original_query = translated_query
                                translated_query = re.sub(r'\bLIKE\b', 'ILIKE', translated_query, flags=re.IGNORECASE)
                                if translated_query != original_query:
                                    translations.append("Replaced LIKE with ILIKE for case-insensitive matching")
                            return translated_query, translations
                except Exception as parse_e:
                    logger.error(f"sqlparse fallback failed: {parse_e}")
                    raise sqlglot_errors.SqlglotError(f"Translation failed after sqlparse fallback: {parse_e}")

                raise
            except Exception as e:
                logger.warning(f"sqlglot failed to transpile query: '{query[:100]}'. Error: {e}")
                raise sqlglot_errors.SqlglotError(f"sqlglot transpilation error: {e}")

        return translated_query, translations

