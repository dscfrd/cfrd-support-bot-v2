#!/usr/bin/env python3
"""
Скрипт миграции базы данных со старого бота на новый.

Что делает:
1. Копирует старую БД
2. Добавляет новые колонки в существующие таблицы
3. Создаёт новые таблицы
4. Проверяет целостность данных
"""

import sqlite3
import shutil
import os
from datetime import datetime

# === ПУТИ ===
OLD_DB_PATH = "/bot/clients_main.db"
NEW_DB_PATH = "/root/cfrd-support-bot-v2/clients.db"
BACKUP_PATH = f"/root/cfrd-support-bot-v2/backups/clients_main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def create_backup():
    """Создать бэкап старой БД"""
    backup_dir = os.path.dirname(BACKUP_PATH)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    shutil.copy2(OLD_DB_PATH, BACKUP_PATH)
    log(f"Бэкап создан: {BACKUP_PATH}")

def copy_database():
    """Скопировать старую БД в новое расположение"""
    shutil.copy2(OLD_DB_PATH, NEW_DB_PATH)
    log(f"БД скопирована в: {NEW_DB_PATH}")

def add_new_columns(conn):
    """Добавить новые колонки в существующие таблицы"""
    cursor = conn.cursor()

    # clients: +custom_id, +company_name, +tier
    new_columns_clients = [
        ("custom_id", "TEXT DEFAULT NULL"),
        ("company_name", "TEXT DEFAULT NULL"),
        ("tier", "INTEGER DEFAULT NULL"),
    ]

    for col_name, col_def in new_columns_clients:
        try:
            cursor.execute(f"ALTER TABLE clients ADD COLUMN {col_name} {col_def}")
            log(f"  + clients.{col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                log(f"  = clients.{col_name} (уже есть)")
            else:
                raise

    # managers: +photo_channel_message_id, +custom_emoji_id
    new_columns_managers = [
        ("photo_channel_message_id", "INTEGER"),
        ("custom_emoji_id", "INTEGER"),
    ]

    for col_name, col_def in new_columns_managers:
        try:
            cursor.execute(f"ALTER TABLE managers ADD COLUMN {col_name} {col_def}")
            log(f"  + managers.{col_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                log(f"  = managers.{col_name} (уже есть)")
            else:
                raise

    conn.commit()

def create_new_tables(conn):
    """Создать новые таблицы"""
    cursor = conn.cursor()

    # message_mapping - маппинг сообщений клиент <-> группа
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS message_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_message_id INTEGER NOT NULL,
            group_message_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_text TEXT
        )
    """)
    log("  + message_mapping")

    # Индексы для message_mapping
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_message_mapping_client
        ON message_mapping(client_message_id, user_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_message_mapping_group
        ON message_mapping(group_message_id, thread_id)
    """)
    log("  + индексы message_mapping")

    # manager_substitutions - замещения менеджеров
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS manager_substitutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_manager TEXT NOT NULL,
            substitute_manager TEXT NOT NULL,
            thread_id INTEGER NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(original_manager, thread_id)
        )
    """)
    log("  + manager_substitutions")

    # file_templates - хранилище файлов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS file_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            channel_message_id INTEGER NOT NULL,
            template_text TEXT,
            file_type TEXT,
            uploaded_by INTEGER,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    log("  + file_templates")

    conn.commit()

def verify_migration(conn):
    """Проверить результат миграции"""
    cursor = conn.cursor()

    log("\n=== Проверка данных ===")

    # Подсчёт записей
    tables = ["clients", "messages", "managers", "duty_managers", "thread_status", "first_replies"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        log(f"  {table}: {count} записей")

    # Проверка новых колонок
    cursor.execute("PRAGMA table_info(clients)")
    columns = [row[1] for row in cursor.fetchall()]
    assert "custom_id" in columns, "Нет колонки custom_id в clients"
    assert "company_name" in columns, "Нет колонки company_name в clients"
    assert "tier" in columns, "Нет колонки tier в clients"
    log("  clients: новые колонки OK")

    cursor.execute("PRAGMA table_info(managers)")
    columns = [row[1] for row in cursor.fetchall()]
    assert "photo_channel_message_id" in columns, "Нет колонки photo_channel_message_id в managers"
    assert "custom_emoji_id" in columns, "Нет колонки custom_emoji_id в managers"
    log("  managers: новые колонки OK")

    # Проверка новых таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    assert "message_mapping" in tables, "Нет таблицы message_mapping"
    assert "manager_substitutions" in tables, "Нет таблицы manager_substitutions"
    assert "file_templates" in tables, "Нет таблицы file_templates"
    log("  новые таблицы OK")

    log("\n=== Миграция успешна! ===")

def main():
    log("=== Миграция БД ===\n")

    # Проверяем существование старой БД
    if not os.path.exists(OLD_DB_PATH):
        log(f"ОШИБКА: Старая БД не найдена: {OLD_DB_PATH}")
        return False

    # Проверяем, не существует ли уже новая БД
    if os.path.exists(NEW_DB_PATH):
        log(f"ВНИМАНИЕ: Новая БД уже существует: {NEW_DB_PATH}")
        response = input("Перезаписать? (y/n): ")
        if response.lower() != 'y':
            log("Отмена миграции")
            return False

    # Шаг 1: Бэкап
    log("Шаг 1: Создание бэкапа...")
    create_backup()

    # Шаг 2: Копирование
    log("\nШаг 2: Копирование БД...")
    copy_database()

    # Шаг 3: Добавление колонок
    log("\nШаг 3: Добавление новых колонок...")
    conn = sqlite3.connect(NEW_DB_PATH)
    add_new_columns(conn)

    # Шаг 4: Создание таблиц
    log("\nШаг 4: Создание новых таблиц...")
    create_new_tables(conn)

    # Шаг 5: Проверка
    verify_migration(conn)

    conn.close()

    log(f"\nНовая БД: {NEW_DB_PATH}")
    log(f"Бэкап: {BACKUP_PATH}")

    return True

if __name__ == "__main__":
    main()
