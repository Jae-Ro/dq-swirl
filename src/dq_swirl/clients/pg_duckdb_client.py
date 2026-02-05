import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Generator, List, Optional, Type, Union

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from dq_swirl.utils.log_utils import get_custom_logger

logger = get_custom_logger()


@dataclass
class PGConfig:
    host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: str | int = field(default_factory=lambda: os.getenv("POSTGRES_PORT", "5432"))
    user: str = field(
        default_factory=lambda: os.getenv("POSTGRES_USER", "app_developer")
    )
    password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "password")
    )
    db: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "store"))

    def __post_init__(self):
        self.port = int(self.port)

    def __repr__(self) -> str:
        return (
            f"PostgresConfig(host='{self.host}', port='{self.port}', ",
            f"user='{self.user}', password='***', db='{self.db}')",
        )

    def __str__(self) -> str:
        return self.__repr__()

    def get_conn_str(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class PGDuckDBClient:
    def __init__(self, config: PGConfig, pool: Optional[ConnectionPool] = None) -> None:
        """_summary_

        :param config: _description_
        """
        self.config = config
        self.pool = pool
        if not self.pool:
            logger.info(f"Initializing Postgres ConnectionPool")
            self.pool = ConnectionPool(
                conninfo=self.config.get_conn_str(),
                min_size=2,
                max_size=4,
                configure=self._configure_connection,
                open=True,
            )

    def _configure_connection(self, conn: psycopg.Connection):
        """
        Configures EVERY new connection.
        Note: We set autocommit=True temporarily to allow CREATE EXTENSION.
        """
        old_autocommit = conn.autocommit

        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_duckdb;")
                cur.execute("SET duckdb.force_execution = true;")
        except Exception as e:
            logger.warning(f"Postgres Configuration Error: {e}")
        finally:
            conn.autocommit = old_autocommit

    def is_healthy(self, timeout: float = 2.0) -> bool:
        """_summary_

        :param timeout: _description_, defaults to 2.0
        :return: _description_
        """
        try:
            with self.pool.connection(timeout=timeout) as conn:
                conn.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    def create_table_from_model(
        self, model_class: Type[BaseModel], table_name: str = None
    ):
        """_summary_

        :param model_class: _description_
        :param table_name: _description_, defaults to None
        """
        table_name = table_name or model_class.__name__.lower()

        type_map = {
            str: "TEXT",
            int: "BIGINT",
            float: "DOUBLE PRECISION",
            bool: "BOOLEAN",
            dict: "JSONB",
            list: "JSONB",
            Any: "JSONB",
        }

        column_defs = []

        for field_name, field_info in model_class.model_fields.items():
            python_type = field_info.annotation

            if hasattr(python_type, "__origin__") and python_type.__origin__ is Union:
                args = [t for t in python_type.__args__ if t is not type(None)]
                actual_type = args[0] if args else str
            else:
                actual_type = getattr(python_type, "__origin__", python_type)

            pg_type = type_map.get(actual_type, "TEXT")

            parts = [sql.Identifier(field_name), sql.SQL(pg_type)]

            if field_name.lower() in ("id", "signature_hash"):
                parts.append(sql.SQL("PRIMARY KEY"))

            if field_info.is_required():
                parts.append(sql.SQL("NOT NULL"))

            column_defs.append(sql.SQL(" ").join(parts))

        create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({fields})").format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(column_defs),
        )

        with self.pool.connection() as conn:
            conn.execute(create_query)
            for field_name, field_info in model_class.model_fields.items():
                if field_info.description:
                    comment_query = sql.SQL(
                        "COMMENT ON COLUMN {table}.{column} IS {comment}"
                    ).format(
                        table=sql.Identifier(table_name),
                        column=sql.Identifier(field_name),
                        comment=sql.Literal(field_info.description),
                    )
                    conn.execute(comment_query)

            logger.info(f"Successfully verified/created table: {table_name}")

    def insert_model(self, table_name: str, model: BaseModel):
        data = model.model_dump()
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                data[key] = Jsonb(value)

        columns = data.keys()
        placeholders = [f"%({col})s" for col in columns]
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        with self.pool.connection() as conn:
            conn.execute(query, data)

    def batch_insert_models(
        self,
        table_name: str,
        models: List[BaseModel],
        chunk_size: int = 5000,
    ):
        """_summary_

        :param table_name: _description_
        :param models: _description_
        :param chunk_size: _description_, defaults to 5000
        """
        if not models:
            return

        columns = list(models[0].model_dump().keys())
        sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN"

        for i in range(0, len(models), chunk_size):
            chunk = models[i : i + chunk_size]
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    with cur.copy(sql) as copy:
                        for m in chunk:
                            row = []
                            for v in m.model_dump().values():
                                row.append(
                                    Jsonb(v) if isinstance(v, (list, dict)) else v
                                )
                            copy.write_row(row)
            logger.info(f"Finished chunk: {i + len(chunk)} records total.")

    def query(
        self,
        sql: str,
        params: Any = None,
        peek: bool = True,
    ) -> Any:
        """_summary_

        :param sql: _description_
        :param params: _description_, defaults to None
        :param peek: _description_, defaults to True
        :return: _description_
        """

        with self.pool.connection() as conn:
            # (column_name: value)
            conn.row_factory = dict_row

            cur = conn.execute(sql, params)

            # do the real deal
            if not peek and cur.description is not None:
                return cur.fetchall()

            # just take a peek
            return cur.rowcount

    def close(self):
        """Shut down the pool when the app exits"""
        self.pool.close()
