"""
Модуль работы с базой данных SQLite
"""

import sqlite3
import datetime
import logging

from config import DATABASE_NAME

logger = logging.getLogger(__name__)


def setup_database():
    """Инициализация базы данных и создание таблиц"""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Таблица клиентов
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

    # Добавляем колонку custom_id, если её нет
    try:
        cursor.execute('SELECT custom_id FROM clients LIMIT 1')
    except sqlite3.OperationalError:
        cursor.execute('ALTER TABLE clients ADD COLUMN custom_id TEXT DEFAULT NULL')
        logger.info("Добавлена колонка custom_id в таблицу clients")

    # Добавляем колонку company_name, если её нет
    try:
        cursor.execute('SELECT company_name FROM clients LIMIT 1')
    except sqlite3.OperationalError:
        cursor.execute('ALTER TABLE clients ADD COLUMN company_name TEXT DEFAULT NULL')
        logger.info("Добавлена колонка company_name в таблицу clients")

    # Добавляем колонку tier (тир клиента: 1, 2, 3), если её нет
    try:
        cursor.execute('SELECT tier FROM clients LIMIT 1')
    except sqlite3.OperationalError:
        cursor.execute('ALTER TABLE clients ADD COLUMN tier INTEGER DEFAULT NULL')
        logger.info("Добавлена колонка tier в таблицу clients")

    # Таблица сообщений
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

    # Таблица менеджеров
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS managers (
        manager_id INTEGER PRIMARY KEY,
        emoji TEXT,
        name TEXT,
        position TEXT,
        extension TEXT,
        photo_file_id TEXT,
        auth_date TEXT
    )
    ''')

    # Добавляем колонку username в managers, если её нет
    try:
        cursor.execute('SELECT username FROM managers LIMIT 1')
    except sqlite3.OperationalError:
        cursor.execute('ALTER TABLE managers ADD COLUMN username TEXT')
        logger.info("Добавлена колонка username в таблицу managers")

    # Таблица первых ответов
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

    # Таблица ответственных менеджеров
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS duty_managers (
        thread_id INTEGER PRIMARY KEY,
        manager_username TEXT,
        assigned_by INTEGER,
        assigned_at TIMESTAMP
    )
    ''')

    # Таблица статусов тредов
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

    # Таблица групп
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS group_threads (
        group_id INTEGER PRIMARY KEY,
        group_title TEXT,
        thread_id INTEGER,
        created_at TIMESTAMP
    )
    ''')

    # Таблица маппинга сообщений (клиент ↔ группа)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS message_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_message_id INTEGER NOT NULL,
        group_message_id INTEGER NOT NULL,
        thread_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        message_text TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Добавляем колонку message_text в message_mapping, если её нет
    try:
        cursor.execute('SELECT message_text FROM message_mapping LIMIT 1')
    except sqlite3.OperationalError:
        cursor.execute('ALTER TABLE message_mapping ADD COLUMN message_text TEXT')
        logger.info("Добавлена колонка message_text в таблицу message_mapping")

    # Таблица замещений менеджеров (отпуск/больничный)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS manager_substitutions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_manager TEXT NOT NULL,
        substitute_manager TEXT NOT NULL,
        thread_id INTEGER NOT NULL,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(original_manager, thread_id)
    )
    ''')

    # Индексы для быстрого поиска
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_message_mapping_client
    ON message_mapping(client_message_id, user_id)
    ''')

    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_message_mapping_group
    ON message_mapping(group_message_id, thread_id)
    ''')

    conn.commit()
    return conn


# === Клиенты ===

def save_client(conn, user):
    """Сохранить/обновить информацию о клиенте"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('SELECT message_count, thread_id FROM clients WHERE user_id = ?', (user.id,))
    result = cursor.fetchone()

    if result:
        message_count = result[0] + 1
        thread_id = result[1]
        cursor.execute('''
        UPDATE clients
        SET last_contact = ?, message_count = ?
        WHERE user_id = ?
        ''', (current_time, message_count, user.id))
    else:
        thread_id = None
        cursor.execute('''
        INSERT INTO clients (user_id, first_name, last_name, username, first_contact, last_contact, thread_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user.id, user.first_name, user.last_name, user.username, current_time, current_time, thread_id))

    conn.commit()
    return thread_id


def update_client_thread(conn, user_id, thread_id):
    """Обновить thread_id клиента"""
    cursor = conn.cursor()
    cursor.execute('UPDATE clients SET thread_id = ? WHERE user_id = ?', (thread_id, user_id))
    conn.commit()


def get_client_by_thread(conn, thread_id):
    """Получить клиента по thread_id"""
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM clients WHERE thread_id = ?', (thread_id,))
    return cursor.fetchone()


def get_all_active_threads(conn):
    """Получить список всех активных тредов"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT
        c.thread_id,
        c.user_id,
        c.first_name,
        c.last_name,
        c.username,
        dm.manager_username AS assigned_manager,
        (SELECT MAX(timestamp) FROM messages WHERE user_id = c.user_id) AS last_message_time
    FROM
        clients c
    LEFT JOIN
        duty_managers dm ON c.thread_id = dm.thread_id
    WHERE
        c.thread_id IS NOT NULL
    ORDER BY
        last_message_time DESC
    ''')
    return cursor.fetchall()


# === Сообщения ===

def save_message(conn, user_id, message_text, is_from_user=True, media_type=None):
    """Сохранить сообщение"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    media_info = f" [{media_type}]" if media_type else ""
    full_message = f"{message_text}{media_info}"

    cursor.execute('''
    INSERT INTO messages (user_id, message_text, timestamp, is_from_user)
    VALUES (?, ?, ?, ?)
    ''', (user_id, full_message, current_time, is_from_user))

    conn.commit()


# === Менеджеры ===

def save_manager(conn, manager_id, emoji, name, position, extension, photo_file_id=None, username=None):
    """Сохранить/обновить менеджера"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    INSERT OR REPLACE INTO managers (manager_id, emoji, name, position, extension, photo_file_id, auth_date, username)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (manager_id, emoji, name, position, extension, photo_file_id, current_time, username))

    conn.commit()


def get_manager(conn, manager_id):
    """Получить данные менеджера"""
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM managers WHERE manager_id = ?', (manager_id,))
    return cursor.fetchone()


def update_manager_photo(conn, manager_id, photo_file_id):
    """Обновить фото менеджера"""
    cursor = conn.cursor()
    cursor.execute('UPDATE managers SET photo_file_id = ? WHERE manager_id = ?', (photo_file_id, manager_id))
    conn.commit()


def get_all_managers(conn):
    """Получить список всех менеджеров"""
    cursor = conn.cursor()
    cursor.execute('SELECT manager_id, username, name FROM managers')
    return cursor.fetchall()


# === Первые ответы ===

def save_first_reply(conn, thread_id, client_id, manager_id):
    """Сохранить первый ответ менеджера"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    SELECT * FROM first_replies WHERE thread_id = ? AND manager_id = ?
    ''', (thread_id, manager_id))

    if not cursor.fetchone():
        cursor.execute('''
        INSERT INTO first_replies (thread_id, client_id, manager_id, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (thread_id, client_id, manager_id, current_time))
        conn.commit()
        return True
    return False


def is_first_reply(conn, thread_id, manager_id):
    """Проверить, является ли ответ первым для менеджера"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * FROM first_replies WHERE thread_id = ? AND manager_id = ?
    ''', (thread_id, manager_id))
    return cursor.fetchone() is None


def get_managers_replied_to_client(conn, thread_id):
    """Получить список менеджеров, отвечавших клиенту"""
    cursor = conn.cursor()
    try:
        cursor.execute('''
        SELECT m.manager_id, m.username
        FROM first_replies fr
        JOIN managers m ON fr.manager_id = m.manager_id
        WHERE fr.thread_id = ?
        ''', (thread_id,))
    except sqlite3.OperationalError:
        cursor.execute('''
        SELECT m.manager_id, NULL
        FROM first_replies fr
        JOIN managers m ON fr.manager_id = m.manager_id
        WHERE fr.thread_id = ?
        ''', (thread_id,))
    return cursor.fetchall()


# === Custom ID клиентов ===

def set_custom_id(conn, user_id, custom_id):
    """Установить произвольный ID клиенту (задаётся менеджером)"""
    import re
    cursor = conn.cursor()

    # Проверяем формат: только русские буквы и цифры, минимум одна русская буква
    if not re.match(r'^[А-Яа-я0-9]+$', custom_id):
        return None, "ID может содержать только русские буквы и цифры"

    # Должна быть хотя бы одна русская буква (чтобы отличать от thread_id)
    if not re.search(r'[А-Яа-я]', custom_id):
        return None, "ID должен содержать хотя бы одну русскую букву"

    # Проверяем, не занят ли этот ID другим клиентом
    cursor.execute('SELECT user_id FROM clients WHERE custom_id = ? AND user_id != ?', (custom_id, user_id))
    existing = cursor.fetchone()
    if existing:
        return None, f"ID #{custom_id} уже занят другим клиентом"

    # Устанавливаем ID
    cursor.execute('UPDATE clients SET custom_id = ? WHERE user_id = ?', (custom_id, user_id))
    conn.commit()

    return custom_id, None


def get_custom_id(conn, user_id):
    """Получить текущий custom_id клиента"""
    cursor = conn.cursor()
    cursor.execute('SELECT custom_id FROM clients WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_thread_id_by_custom_id(conn, custom_id):
    """Получить thread_id и user_id по custom_id клиента"""
    cursor = conn.cursor()
    cursor.execute('SELECT thread_id, user_id FROM clients WHERE custom_id = ?', (custom_id,))
    result = cursor.fetchone()
    if result:
        return result[0], result[1]
    return None, None


def get_custom_id_by_thread(conn, thread_id):
    """Получить custom_id по thread_id"""
    cursor = conn.cursor()
    cursor.execute('SELECT custom_id FROM clients WHERE thread_id = ?', (thread_id,))
    result = cursor.fetchone()
    return result[0] if result else None


# === Название компании ===

def set_company_name(conn, thread_id, company_name):
    """Установить название компании для клиента по thread_id"""
    cursor = conn.cursor()
    cursor.execute('UPDATE clients SET company_name = ? WHERE thread_id = ?', (company_name, thread_id))
    conn.commit()
    return cursor.rowcount > 0


def get_company_name(conn, thread_id):
    """Получить название компании по thread_id"""
    cursor = conn.cursor()
    cursor.execute('SELECT company_name FROM clients WHERE thread_id = ?', (thread_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_client_info_for_thread_title(conn, thread_id):
    """Получить информацию о клиенте для заголовка треда: (first_name, last_name, company_name)"""
    cursor = conn.cursor()
    cursor.execute('SELECT first_name, last_name, company_name FROM clients WHERE thread_id = ?', (thread_id,))
    return cursor.fetchone()


# === Ответственные менеджеры ===

def get_duty_manager(conn, thread_id):
    """Получить ответственного менеджера для треда"""
    cursor = conn.cursor()
    cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def assign_duty_manager(conn, thread_id, manager_username, assigned_by):
    """Назначить ответственного менеджера"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    INSERT OR REPLACE INTO duty_managers (thread_id, manager_username, assigned_by, assigned_at)
    VALUES (?, ?, ?, ?)
    ''', (thread_id, manager_username, assigned_by, current_time))

    conn.commit()


def remove_duty_manager(conn, thread_id):
    """Удалить ответственного менеджера"""
    cursor = conn.cursor()
    cursor.execute('DELETE FROM duty_managers WHERE thread_id = ?', (thread_id,))
    conn.commit()


# === Статусы тредов ===

def update_client_message_time(conn, thread_id):
    """Обновить время последнего сообщения клиента"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    INSERT OR REPLACE INTO thread_status (thread_id, last_client_message, is_notified, notification_disabled)
    VALUES (?, ?, 0, COALESCE((SELECT notification_disabled FROM thread_status WHERE thread_id = ?), 0))
    ''', (thread_id, current_time, thread_id))

    conn.commit()


def update_manager_reply_time(conn, thread_id):
    """Обновить время последнего ответа менеджера"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    UPDATE thread_status
    SET last_manager_reply = ?, is_notified = 0
    WHERE thread_id = ?
    ''', (current_time, thread_id))

    conn.commit()


def get_unanswered_threads(conn):
    """Получить треды без ответа менеджера"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT
        ts.thread_id,
        ts.last_client_message,
        ts.last_manager_reply,
        ts.is_notified,
        ts.last_notification,
        ts.notification_disabled,
        c.first_name,
        c.username,
        dm.manager_username
    FROM thread_status ts
    LEFT JOIN clients c ON ts.thread_id = c.thread_id
    LEFT JOIN duty_managers dm ON ts.thread_id = dm.thread_id
    WHERE (ts.last_manager_reply IS NULL OR ts.last_client_message > ts.last_manager_reply)
    AND ts.notification_disabled = 0
    ''')
    return cursor.fetchall()


def mark_thread_notified(conn, thread_id):
    """Пометить тред как уведомленный"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    UPDATE thread_status
    SET is_notified = 1, last_notification = ?
    WHERE thread_id = ?
    ''', (current_time, thread_id))

    conn.commit()


def disable_thread_notifications(conn, thread_id):
    """Отключить уведомления для треда"""
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE thread_status
    SET notification_disabled = 1
    WHERE thread_id = ?
    ''', (thread_id,))
    conn.commit()


def enable_thread_notifications(conn, thread_id):
    """Включить уведомления для треда"""
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE thread_status
    SET notification_disabled = 0, is_notified = 0
    WHERE thread_id = ?
    ''', (thread_id,))
    conn.commit()


# === Группы ===

def save_group_thread(conn, group_id, group_title, thread_id):
    """Сохранить тред группы"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    cursor.execute('''
    INSERT OR REPLACE INTO group_threads (group_id, group_title, thread_id, created_at)
    VALUES (?, ?, ?, ?)
    ''', (group_id, group_title, thread_id, current_time))

    conn.commit()


def get_group_thread(conn, group_id):
    """Получить тред группы"""
    cursor = conn.cursor()
    cursor.execute('SELECT thread_id, group_title FROM group_threads WHERE group_id = ?', (group_id,))
    return cursor.fetchone()


def get_group_by_thread(conn, thread_id):
    """Получить группу по thread_id"""
    cursor = conn.cursor()
    cursor.execute('SELECT group_id, group_title FROM group_threads WHERE thread_id = ?', (thread_id,))
    return cursor.fetchone()


# === Маппинг сообщений ===

def save_message_mapping(conn, client_message_id, group_message_id, thread_id, user_id, message_text=None):
    """Сохранить связь между сообщением клиента и сообщением в группе"""
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO message_mapping (client_message_id, group_message_id, thread_id, user_id, message_text)
    VALUES (?, ?, ?, ?, ?)
    ''', (client_message_id, group_message_id, thread_id, user_id, message_text))
    conn.commit()


def get_group_message_id(conn, client_message_id, user_id):
    """Получить ID сообщения в группе по ID сообщения клиента"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT group_message_id, thread_id, message_text FROM message_mapping
    WHERE client_message_id = ? AND user_id = ?
    ORDER BY id DESC LIMIT 1
    ''', (client_message_id, user_id))
    return cursor.fetchone()


def update_message_text(conn, client_message_id, user_id, new_text):
    """Обновить текст сообщения в маппинге"""
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE message_mapping SET message_text = ?
    WHERE client_message_id = ? AND user_id = ?
    ''', (new_text, client_message_id, user_id))
    conn.commit()
    return cursor.rowcount > 0


def get_client_message_id(conn, group_message_id, thread_id):
    """Получить ID сообщения клиента по ID сообщения в группе"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT client_message_id, user_id FROM message_mapping
    WHERE group_message_id = ? AND thread_id = ?
    ORDER BY id DESC LIMIT 1
    ''', (group_message_id, thread_id))
    return cursor.fetchone()


def cleanup_old_mappings(conn, days=30):
    """Удалить старые маппинги (старше N дней)"""
    cursor = conn.cursor()
    cursor.execute('''
    DELETE FROM message_mapping
    WHERE created_at < datetime('now', '-' || ? || ' days')
    ''', (days,))
    conn.commit()
    return cursor.rowcount


# === Управление менеджерами ===

def get_manager_threads(conn, manager_username):
    """Получить все треды, назначенные менеджеру"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT dm.thread_id, c.first_name, c.last_name, c.username
    FROM duty_managers dm
    LEFT JOIN clients c ON dm.thread_id = c.thread_id
    WHERE dm.manager_username = ?
    ''', (manager_username,))
    return cursor.fetchall()


def reassign_all_threads(conn, old_manager_username, new_manager_username, assigned_by):
    """Переназначить все треды от одного менеджера другому"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    # Получаем количество тредов для переназначения
    cursor.execute('SELECT COUNT(*) FROM duty_managers WHERE manager_username = ?', (old_manager_username,))
    count = cursor.fetchone()[0]

    if count == 0:
        return 0

    # Переназначаем все треды
    cursor.execute('''
    UPDATE duty_managers
    SET manager_username = ?, assigned_by = ?, assigned_at = ?
    WHERE manager_username = ?
    ''', (new_manager_username, assigned_by, current_time, old_manager_username))

    conn.commit()
    return count


def unassign_all_threads(conn, manager_username):
    """Снять ответственность со всех тредов менеджера"""
    cursor = conn.cursor()

    # Получаем количество тредов
    cursor.execute('SELECT COUNT(*) FROM duty_managers WHERE manager_username = ?', (manager_username,))
    count = cursor.fetchone()[0]

    if count == 0:
        return 0

    # Удаляем все назначения
    cursor.execute('DELETE FROM duty_managers WHERE manager_username = ?', (manager_username,))
    conn.commit()
    return count


def remove_manager(conn, manager_username):
    """Полностью удалить менеджера из системы"""
    cursor = conn.cursor()

    # Сначала находим manager_id по username
    cursor.execute('SELECT manager_id FROM managers WHERE username = ?', (manager_username,))
    result = cursor.fetchone()

    if not result:
        return False, 0

    manager_id = result[0]

    # Снимаем со всех тредов
    cursor.execute('SELECT COUNT(*) FROM duty_managers WHERE manager_username = ?', (manager_username,))
    threads_count = cursor.fetchone()[0]
    cursor.execute('DELETE FROM duty_managers WHERE manager_username = ?', (manager_username,))

    # Удаляем записи о первых ответах
    cursor.execute('DELETE FROM first_replies WHERE manager_id = ?', (manager_id,))

    # Удаляем менеджера
    cursor.execute('DELETE FROM managers WHERE manager_id = ?', (manager_id,))

    conn.commit()
    return True, threads_count


def get_manager_by_username(conn, username):
    """Получить менеджера по username"""
    cursor = conn.cursor()
    # username может быть с @ или без
    clean_username = username.lstrip('@')
    cursor.execute('SELECT manager_id, name, username FROM managers WHERE username = ?', (clean_username,))
    return cursor.fetchone()


# === Тиры клиентов ===

def set_client_tier(conn, thread_id, tier):
    """Установить тир клиента (1, 2, 3 или None для сброса)"""
    cursor = conn.cursor()
    if tier is not None and tier not in (1, 2, 3):
        return False, "Тир должен быть 1, 2 или 3"
    cursor.execute('UPDATE clients SET tier = ? WHERE thread_id = ?', (tier, thread_id))
    conn.commit()
    return cursor.rowcount > 0, None


def get_client_tier(conn, thread_id):
    """Получить тир клиента"""
    cursor = conn.cursor()
    cursor.execute('SELECT tier FROM clients WHERE thread_id = ?', (thread_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_threads_by_tier(conn, manager_username, tiers=None):
    """Получить треды менеджера, опционально фильтруя по тирам"""
    cursor = conn.cursor()
    if tiers:
        placeholders = ','.join('?' * len(tiers))
        cursor.execute(f'''
        SELECT dm.thread_id, c.first_name, c.last_name, c.tier
        FROM duty_managers dm
        LEFT JOIN clients c ON dm.thread_id = c.thread_id
        WHERE dm.manager_username = ? AND c.tier IN ({placeholders})
        ''', (manager_username, *tiers))
    else:
        cursor.execute('''
        SELECT dm.thread_id, c.first_name, c.last_name, c.tier
        FROM duty_managers dm
        LEFT JOIN clients c ON dm.thread_id = c.thread_id
        WHERE dm.manager_username = ?
        ''', (manager_username,))
    return cursor.fetchall()


# === Замещения менеджеров (отпуск/больничный) ===

def start_vacation(conn, original_manager, substitute_manager, assigned_by, tiers=None):
    """
    Начать замещение - передать треды заместителю.
    Возвращает количество переданных тредов.
    """
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    # Получаем треды оригинального менеджера (с фильтром по тирам если указаны)
    if tiers:
        placeholders = ','.join('?' * len(tiers))
        cursor.execute(f'''
        SELECT dm.thread_id FROM duty_managers dm
        LEFT JOIN clients c ON dm.thread_id = c.thread_id
        WHERE dm.manager_username = ? AND c.tier IN ({placeholders})
        ''', (original_manager, *tiers))
    else:
        cursor.execute('SELECT thread_id FROM duty_managers WHERE manager_username = ?', (original_manager,))

    threads = cursor.fetchall()
    if not threads:
        return 0

    count = 0
    for (thread_id,) in threads:
        # Сохраняем информацию о замещении
        try:
            cursor.execute('''
            INSERT OR REPLACE INTO manager_substitutions (original_manager, substitute_manager, thread_id, started_at)
            VALUES (?, ?, ?, ?)
            ''', (original_manager, substitute_manager, thread_id, current_time))

            # Переназначаем тред заместителю
            cursor.execute('''
            UPDATE duty_managers SET manager_username = ?, assigned_by = ?, assigned_at = ?
            WHERE thread_id = ?
            ''', (substitute_manager, assigned_by, current_time, thread_id))
            count += 1
        except Exception as e:
            logger.error(f"Ошибка при замещении треда {thread_id}: {e}")

    conn.commit()
    return count


def end_vacation(conn, original_manager, assigned_by):
    """
    Завершить замещение - вернуть треды оригинальному менеджеру.
    Возвращает количество возвращённых тредов.
    """
    cursor = conn.cursor()
    current_time = datetime.datetime.now()

    # Получаем все замещения для этого менеджера
    cursor.execute('''
    SELECT thread_id, substitute_manager FROM manager_substitutions
    WHERE original_manager = ?
    ''', (original_manager,))

    substitutions = cursor.fetchall()
    if not substitutions:
        return 0

    count = 0
    for thread_id, substitute_manager in substitutions:
        # Проверяем, что тред всё ещё у заместителя (не был переназначен кому-то другому)
        cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
        current = cursor.fetchone()

        if current and current[0] == substitute_manager:
            # Возвращаем тред оригинальному менеджеру
            cursor.execute('''
            UPDATE duty_managers SET manager_username = ?, assigned_by = ?, assigned_at = ?
            WHERE thread_id = ?
            ''', (original_manager, assigned_by, current_time, thread_id))
            count += 1

    # Удаляем записи о замещении
    cursor.execute('DELETE FROM manager_substitutions WHERE original_manager = ?', (original_manager,))
    conn.commit()
    return count


def get_vacation_info(conn, manager_username):
    """Получить информацию о замещении менеджера"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT substitute_manager, COUNT(*) as thread_count, MIN(started_at) as started
    FROM manager_substitutions
    WHERE original_manager = ?
    GROUP BY substitute_manager
    ''', (manager_username,))
    return cursor.fetchall()


def is_on_vacation(conn, manager_username):
    """Проверить, находится ли менеджер в отпуске"""
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM manager_substitutions WHERE original_manager = ?', (manager_username,))
    return cursor.fetchone()[0] > 0
