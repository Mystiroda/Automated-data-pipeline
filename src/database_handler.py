"""
Database Handler Module
Manages SQLite database operations for storing and querying data.
"""

import sys
import sqlite3
from pathlib import Path
import pandas as pd
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import DATABASE_PATH
logger = logging.getLogger(__name__)


def setup_database():
    """Initialize database and ensure tables exist"""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DATABASE_PATH)


def dataframe_to_sqlite(df, table_name, if_exists='replace'):
    """
    Save DataFrame to SQLite table
    Args:
        df: DataFrame to save
        table_name: Name for SQL table
        if_exists: 'fail', 'replace', or 'append'
    """
    try:
        with setup_database() as conn:
            table_name = clean_table_name(table_name)
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn)
            logger.info(f"Saved {count.iloc[0]['count']} rows to table '{table_name}'")

            return table_name

    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        return None


def clean_table_name(name):
    """
    Clean table name for SQL compatibility
    Args:
        name: Name of table
    Returns:
          cleaned table name
    """
    filtered_chars = []
    for characters in name:
        if characters.isalnum() or characters == '_':
            filtered_chars.append(characters)
    result = ''.join(filtered_chars).lower()
    return result


def run_sql_query(query):
    """
    Run SQL query and return results as DataFrame
    Args:
        query: SQL query
    """
    try:
        with setup_database() as conn:
            df = pd.read_sql_query(query, conn)
            return df
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return None


def get_table_info(table_name):
    """
    Get information about a table
    Args:
        table_name: Name of table
    Returns:
        table information
    """
    query = f"PRAGMA table_info({table_name})"
    return run_sql_query(query)


def list_tables():
    """List all tables in database"""
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    result = run_sql_query(query)
    return result['name'].tolist() if result is not None else []

