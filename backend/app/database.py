"""
VTagger Database Module.

Handles SQLite database initialization, migrations, and query execution.
Ported from BPVtagger with BP-specific tables replaced by generic dimension handling.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from app.config import settings


def _get_db_path() -> str:
    """Get the database file path, ensuring parent directory exists."""
    db_path = Path(settings.database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return str(db_path)


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(_get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(query: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT query and return results as list of dicts."""
    with get_db() as conn:
        cursor = conn.execute(query, params)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def execute_write(query: str, params: tuple = ()) -> int:
    """Execute an INSERT/UPDATE/DELETE query and return lastrowid or rowcount."""
    with get_db() as conn:
        cursor = conn.execute(query, params)
        conn.commit()
        return cursor.lastrowid if cursor.lastrowid else cursor.rowcount


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def _has_unique_constraint(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    """Check if a column has a UNIQUE constraint (via index)."""
    cursor = conn.execute(f"PRAGMA index_list({table_name})")
    for row in cursor.fetchall():
        if row[2] == 1:  # unique index
            idx_cursor = conn.execute(f"PRAGMA index_info({row[1]})")
            idx_columns = [idx_row[2] for idx_row in idx_cursor.fetchall()]
            if column_name in idx_columns:
                return True
    return False


def _run_migrations(conn: sqlite3.Connection):
    """Run database migrations for schema changes."""

    # Migration: Add dimension_matches column to daily_stats if missing
    if _table_exists(conn, "daily_stats"):
        if not _column_exists(conn, "daily_stats", "dimension_matches"):
            conn.execute(
                "ALTER TABLE daily_stats ADD COLUMN dimension_matches INTEGER DEFAULT 0"
            )
            print("  Migration: Added dimension_matches column to daily_stats")

        # Migration: Add missing columns to daily_stats
        for col, col_type, default in [
            ("unmatched_statements", "INTEGER", "0"),
            ("match_rate", "REAL", "0.0"),
            ("api_calls", "INTEGER", "0"),
            ("errors", "INTEGER", "0"),
        ]:
            if not _column_exists(conn, "daily_stats", col):
                conn.execute(
                    f"ALTER TABLE daily_stats ADD COLUMN {col} {col_type} DEFAULT {default}"
                )
                print(f"  Migration: Added {col} column to daily_stats")

    # Migration: Remove UNIQUE constraint from tagging_jobs.job_date if present
    if _table_exists(conn, "tagging_jobs"):
        if _has_unique_constraint(conn, "tagging_jobs", "job_date"):
            print("  Migration: Removing UNIQUE constraint from tagging_jobs.job_date")
            # SQLite doesn't support DROP CONSTRAINT, so we recreate the table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tagging_jobs_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_date TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    total_statements INTEGER DEFAULT 0,
                    processed_statements INTEGER DEFAULT 0,
                    matched_statements INTEGER DEFAULT 0,
                    unmatched_statements INTEGER DEFAULT 0,
                    dimensions_applied INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                INSERT INTO tagging_jobs_new
                SELECT id, job_date, status, total_statements, processed_statements,
                       matched_statements, unmatched_statements, dimensions_applied,
                       error_message, started_at, completed_at, created_at, updated_at
                FROM tagging_jobs
            """)
            conn.execute("DROP TABLE tagging_jobs")
            conn.execute("ALTER TABLE tagging_jobs_new RENAME TO tagging_jobs")
            print("  Migration: UNIQUE constraint removed from tagging_jobs.job_date")

    # Migration: Add UNIQUE constraint on daily_stats.stat_date for upsert support
    if _table_exists(conn, "daily_stats"):
        if not _has_unique_constraint(conn, "daily_stats", "stat_date"):
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(stat_date)")
            print("  Migration: Added UNIQUE index on daily_stats.stat_date")

    # Migration: Add missing columns to tagging_jobs
    if _table_exists(conn, "tagging_jobs"):
        for col, col_type, default in [
            ("dimensions_applied", "INTEGER", "0"),
            ("unmatched_statements", "INTEGER", "0"),
        ]:
            if not _column_exists(conn, "tagging_jobs", col):
                conn.execute(
                    f"ALTER TABLE tagging_jobs ADD COLUMN {col} {col_type} DEFAULT {default}"
                )
                print(f"  Migration: Added {col} column to tagging_jobs")


def init_database():
    """Initialize the database with all required tables."""
    conn = sqlite3.connect(_get_db_path())
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    try:
        # 1. api_keys table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                key TEXT NOT NULL,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                last_used_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. dimensions table (replaces bizmapping files)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dimensions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vtag_name TEXT NOT NULL UNIQUE,
                index_number INTEGER,
                kind TEXT DEFAULT 'TAG_MAPPING',
                default_value TEXT DEFAULT 'Unallocated',
                source TEXT DEFAULT 'TAGS',
                content TEXT,
                statement_count INTEGER DEFAULT 0,
                checksum TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 3. dimension_history table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dimension_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vtag_name TEXT NOT NULL,
                action TEXT NOT NULL,
                previous_content TEXT,
                new_content TEXT,
                source TEXT DEFAULT 'web',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 4. discovered_tags table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discovered_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag_key TEXT NOT NULL UNIQUE,
                sample_values TEXT,
                first_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
                occurrence_count INTEGER DEFAULT 1
            )
        """)

        # 5. tagging_jobs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tagging_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_date TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                total_statements INTEGER DEFAULT 0,
                processed_statements INTEGER DEFAULT 0,
                matched_statements INTEGER DEFAULT 0,
                unmatched_statements INTEGER DEFAULT 0,
                dimensions_applied INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 6. daily_stats table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stat_date TEXT NOT NULL UNIQUE,
                total_statements INTEGER DEFAULT 0,
                tagged_statements INTEGER DEFAULT 0,
                dimension_matches INTEGER DEFAULT 0,
                unmatched_statements INTEGER DEFAULT 0,
                match_rate REAL DEFAULT 0.0,
                api_calls INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 7. vtag_uploads table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vtag_uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_date TEXT NOT NULL,
                file_name TEXT,
                vtag_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                api_response TEXT,
                error_message TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 8. month_syncs table (for sync operations)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS month_syncs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                month TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                total_weeks INTEGER DEFAULT 0,
                completed_weeks INTEGER DEFAULT 0,
                total_statements INTEGER DEFAULT 0,
                fetched_statements INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 9. month_sync_weeks table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS month_sync_weeks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_id INTEGER NOT NULL,
                week_start TEXT NOT NULL,
                week_end TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                statement_count INTEGER DEFAULT 0,
                error_message TEXT,
                started_at TEXT,
                completed_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sync_id) REFERENCES month_syncs(id)
            )
        """)

        # 10. umbrella_accounts table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS umbrella_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL UNIQUE,
                account_name TEXT,
                account_type TEXT,
                currency TEXT DEFAULT 'USD',
                is_active INTEGER DEFAULT 1,
                last_synced_at TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 11. config table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL UNIQUE,
                value TEXT,
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Run migrations for existing databases
        _run_migrations(conn)

        conn.commit()
        print("  Database initialized successfully")

    except Exception as e:
        conn.rollback()
        print(f"  Database initialization error: {e}")
        raise
    finally:
        conn.close()


def get_config_value(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get a configuration value from the config table."""
    try:
        results = execute_query(
            "SELECT value FROM config WHERE key = ?", (key,)
        )
        if results:
            return results[0]["value"]
        return default
    except Exception:
        return default


def set_config_value(key: str, value: str, description: str = "") -> None:
    """Set a configuration value in the config table."""
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO config (key, value, description, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                description = COALESCE(excluded.description, config.description),
                updated_at = CURRENT_TIMESTAMP
            """,
            (key, value, description),
        )
