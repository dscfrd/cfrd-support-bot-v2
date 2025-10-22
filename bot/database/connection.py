"""Database connection and setup"""

import sqlite3
import logging
from config import DATABASE_NAME

logger = logging.getLogger(__name__)

_connection = None


def get_connection():
    """Get or create database connection"""
    global _connection
    if _connection is None:
        _connection = setup_database()
    return _connection


def setup_database():
    """Setup database and create tables"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Создаем таблицу клиентов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS clients (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        username TEXT,
        first_contact TIMESTAMP,
        last_contact TIMESTAMP,
        message_count INTEGER DEFAULT 1,
        thread_id INTEGER DEFAULT NULL,
        custom_id TEXT DEFAULT NULL
    )
    ''')

    # Создаем таблицу сообщений
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        message_text TEXT,
        timestamp TIMESTAMP,
        is_from_user BOOLEAN,
        FOREIGN KEY (user_id) REFERENCES clients(user_id)
    )
    ''')

    # Создаем таблицу менеджеров
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS managers (
        manager_id INTEGER PRIMARY KEY,
        emoji TEXT,
        name TEXT,
        position TEXT,
        extension TEXT,
        photo_file_id TEXT,
        auth_date TEXT,
        username TEXT,
        photo_path TEXT,
        photo_storage_msg_id INTEGER
    )
    ''')

    # Создаем таблицу первых ответов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS first_replies (
        thread_id INTEGER,
        client_id INTEGER,
        manager_id INTEGER,
        timestamp TEXT,
        PRIMARY KEY (thread_id, manager_id),
        FOREIGN KEY (client_id) REFERENCES clients (user_id),
        FOREIGN KEY (manager_id) REFERENCES managers (manager_id)
    )
    ''')

    # Создаем таблицу ответственных менеджеров
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS duty_managers (
        thread_id INTEGER PRIMARY KEY,
        manager_username TEXT,
        assigned_by INTEGER,
        assigned_at TIMESTAMP
    )
    ''')

    # Создаем таблицу для отслеживания статуса ответов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS thread_status (
        thread_id INTEGER PRIMARY KEY,
        last_client_message TIMESTAMP,
        last_manager_reply TIMESTAMP,
        is_notified BOOLEAN DEFAULT 0,
        last_notification TIMESTAMP,
        notification_disabled BOOLEAN DEFAULT 0
    )
    ''')

    # Создаем таблицу для групп
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS group_threads (
        group_id INTEGER PRIMARY KEY,
        group_title TEXT,
        thread_id INTEGER,
        created_at TIMESTAMP
    )
    ''')

    # Создаем таблицу для хранения файлов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS storage_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT UNIQUE,
        file_id TEXT,
        message_id INTEGER,
        file_type TEXT,
        upload_date TIMESTAMP,
        uploaded_by INTEGER
    )
    ''')

    # Создаем таблицу для хранения истории версий файлов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        file_id TEXT,
        message_id INTEGER,
        version_date TIMESTAMP,
        created_by INTEGER
    )
    ''')

    conn.commit()
    logger.info(f"Database {DATABASE_NAME} initialized successfully")
    return conn
