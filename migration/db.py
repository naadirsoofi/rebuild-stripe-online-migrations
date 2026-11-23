"""Connections and database setup."""
from __future__ import annotations

import os
import re
from pathlib import Path

import pyodbc

# Dropping and recreating databases leaves pooled connections pointing at nothing.
pyodbc.pooling = False

DEFAULT_SERVER_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;"
    "Trusted_Connection=yes;TrustServerCertificate=yes"
)
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
SCRIPT_DATABASE = "RebuildOnlineMigrations"


def server_conn_str() -> str:
    """ODBC connection string for the server, without a database. Override with REBUILD_SQL_CONN."""
    return os.environ.get("REBUILD_SQL_CONN", DEFAULT_SERVER_CONN)


def connect(database: str, autocommit: bool = False) -> pyodbc.Connection:
    return pyodbc.connect(f"{server_conn_str()};DATABASE={database}", autocommit=autocommit)


def create_database(database: str) -> None:
    """Drop and recreate the database, then run sql/schema.sql against it."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise ValueError(f"Unsafe database name: {database!r}")

    script = SCHEMA_PATH.read_text(encoding="utf-8").replace(f"[{SCRIPT_DATABASE}]", f"[{database}]")
    conn = pyodbc.connect(server_conn_str(), autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(
            f"IF DB_ID(N'{database}') IS NOT NULL "
            f"BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE [{database}]; END"
        )
        cur.execute(f"CREATE DATABASE [{database}]")
        for batch in re.split(r"^\s*GO\s*$", script, flags=re.MULTILINE | re.IGNORECASE):
            if batch.strip():
                cur.execute(batch)
    finally:
        conn.close()
