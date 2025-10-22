"""Database query functions"""

import datetime
import logging

logger = logging.getLogger(__name__)


def save_client(db_connection, user):
    """Save or update client information"""
    cursor = db_connection.cursor()

    # Проверяем, существует ли клиент
    cursor.execute('SELECT user_id, thread_id FROM clients WHERE user_id = ?', (user.id,))
    existing_client = cursor.fetchone()

    if existing_client:
        # Обновляем информацию о клиенте
        cursor.execute('''
        UPDATE clients
        SET last_contact = ?, message_count = message_count + 1,
            first_name = ?, last_name = ?, username = ?
        WHERE user_id = ?
        ''', (
            datetime.datetime.now(),
            user.first_name,
            user.last_name,
            user.username,
            user.id
        ))
        db_connection.commit()
        return existing_client[1]  # Возвращаем thread_id
    else:
        # Добавляем нового клиента
        cursor.execute('''
        INSERT INTO clients (user_id, first_name, last_name, username, first_contact, last_contact)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            user.id,
            user.first_name,
            user.last_name,
            user.username,
            datetime.datetime.now(),
            datetime.datetime.now()
        ))
        db_connection.commit()
        return None  # Новый клиент, треда пока нет


def get_client_by_thread(db_connection, thread_id):
    """Get client information by thread_id"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT user_id, first_name, last_name, username, first_contact,
           last_contact, message_count, thread_id, custom_id
    FROM clients WHERE thread_id = ?
    ''', (thread_id,))
    return cursor.fetchone()


def update_client_thread(db_connection, user_id, thread_id):
    """Update client's thread_id"""
    cursor = db_connection.cursor()
    cursor.execute('''
    UPDATE clients SET thread_id = ? WHERE user_id = ?
    ''', (thread_id, user_id))
    db_connection.commit()


def save_message(db_connection, user_id, message_text, is_from_user=True, media_type=None):
    """Save message to database"""
    cursor = db_connection.cursor()

    # Если есть media_type, добавляем его к тексту сообщения
    if media_type:
        full_text = f"[{media_type}] {message_text}" if message_text else f"[{media_type}]"
    else:
        full_text = message_text

    cursor.execute('''
    INSERT INTO messages (user_id, message_text, timestamp, is_from_user)
    VALUES (?, ?, ?, ?)
    ''', (user_id, full_text, datetime.datetime.now(), is_from_user))
    db_connection.commit()


def save_manager(db_connection, manager_id, emoji, name, position, extension, username=None):
    """Save or update manager information"""
    cursor = db_connection.cursor()

    # Проверяем, существует ли менеджер
    cursor.execute('SELECT manager_id FROM managers WHERE manager_id = ?', (manager_id,))
    existing_manager = cursor.fetchone()

    if existing_manager:
        # Обновляем информацию о менеджере
        cursor.execute('''
        UPDATE managers
        SET emoji = ?, name = ?, position = ?, extension = ?, username = ?, auth_date = ?
        WHERE manager_id = ?
        ''', (emoji, name, position, extension, username, datetime.datetime.now(), manager_id))
    else:
        # Добавляем нового менеджера
        cursor.execute('''
        INSERT INTO managers (manager_id, emoji, name, position, extension, username, auth_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (manager_id, emoji, name, position, extension, username, datetime.datetime.now()))

    db_connection.commit()


def get_manager(db_connection, manager_id):
    """Get manager information by manager_id"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT manager_id, emoji, name, position, extension, photo_file_id, auth_date, username
    FROM managers WHERE manager_id = ?
    ''', (manager_id,))
    return cursor.fetchone()


def update_manager_photo(db_connection, manager_id, photo_file_id):
    """Update manager's photo"""
    cursor = db_connection.cursor()
    cursor.execute('''
    UPDATE managers SET photo_file_id = ? WHERE manager_id = ?
    ''', (photo_file_id, manager_id))
    db_connection.commit()


def get_all_active_threads(db_connection):
    """Get all active threads with client information"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT
        c.thread_id,
        c.user_id,
        c.first_name,
        c.last_name,
        c.username,
        dm.manager_username,
        c.last_contact
    FROM clients c
    LEFT JOIN duty_managers dm ON c.thread_id = dm.thread_id
    WHERE c.thread_id IS NOT NULL
    ORDER BY c.last_contact DESC
    ''')
    return cursor.fetchall()


def save_first_reply(db_connection, thread_id, client_id, manager_id):
    """Save information about manager's first reply"""
    cursor = db_connection.cursor()
    cursor.execute('''
    INSERT OR IGNORE INTO first_replies (thread_id, client_id, manager_id, timestamp)
    VALUES (?, ?, ?, ?)
    ''', (thread_id, client_id, manager_id, datetime.datetime.now()))
    db_connection.commit()


def is_first_reply(db_connection, thread_id, manager_id):
    """Check if this is manager's first reply to the thread"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT thread_id FROM first_replies
    WHERE thread_id = ? AND manager_id = ?
    ''', (thread_id, manager_id))
    return cursor.fetchone() is None


def get_managers_replied_to_client(db_connection, client_id):
    """Get list of managers who replied to client"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT m.name, m.position, m.emoji, m.extension
    FROM first_replies fr
    JOIN managers m ON fr.manager_id = m.manager_id
    WHERE fr.client_id = ?
    ORDER BY fr.timestamp
    ''', (client_id,))
    return cursor.fetchall()


def get_client_info_by_thread(db_connection, thread_id):
    """Get detailed client information by thread_id"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT
        c.user_id,
        c.first_name,
        c.last_name,
        c.username,
        c.first_contact,
        c.last_contact,
        c.message_count,
        c.custom_id,
        dm.manager_username
    FROM clients c
    LEFT JOIN duty_managers dm ON c.thread_id = dm.thread_id
    WHERE c.thread_id = ?
    ''', (thread_id,))
    return cursor.fetchone()


def update_client_message_time(db_connection, thread_id):
    """Update last client message time"""
    cursor = db_connection.cursor()
    current_time = datetime.datetime.now()

    # Проверяем, существует ли запись в thread_status
    cursor.execute('SELECT thread_id FROM thread_status WHERE thread_id = ?', (thread_id,))

    if cursor.fetchone():
        # Обновляем существующую запись
        cursor.execute('''
        UPDATE thread_status
        SET last_client_message = ?, is_notified = 0
        WHERE thread_id = ?
        ''', (current_time, thread_id))
    else:
        # Создаем новую запись
        cursor.execute('''
        INSERT INTO thread_status (thread_id, last_client_message, is_notified, notification_disabled)
        VALUES (?, ?, 0, 0)
        ''', (thread_id, current_time))

    db_connection.commit()


def update_manager_reply_time(db_connection, thread_id):
    """Update last manager reply time"""
    cursor = db_connection.cursor()
    current_time = datetime.datetime.now()

    # Проверяем, существует ли запись
    cursor.execute('SELECT thread_id FROM thread_status WHERE thread_id = ?', (thread_id,))

    if cursor.fetchone():
        # Обновляем существующую запись
        cursor.execute('''
        UPDATE thread_status
        SET last_manager_reply = ?, is_notified = 0
        WHERE thread_id = ?
        ''', (current_time, thread_id))
    else:
        # Создаем новую запись
        cursor.execute('''
        INSERT INTO thread_status (thread_id, last_manager_reply, is_notified, notification_disabled)
        VALUES (?, ?, 0, 0)
        ''', (thread_id, current_time))

    db_connection.commit()


def assign_duty_manager(db_connection, thread_id, manager_username, assigned_by):
    """Assign duty manager to thread"""
    cursor = db_connection.cursor()
    cursor.execute('''
    INSERT OR REPLACE INTO duty_managers (thread_id, manager_username, assigned_by, assigned_at)
    VALUES (?, ?, ?, ?)
    ''', (thread_id, manager_username, assigned_by, datetime.datetime.now()))
    db_connection.commit()


def get_duty_manager(db_connection, thread_id):
    """Get duty manager for thread"""
    cursor = db_connection.cursor()
    cursor.execute('''
    SELECT manager_username FROM duty_managers WHERE thread_id = ?
    ''', (thread_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def reset_thread_notification(db_connection, thread_id):
    """Reset notification flag for thread"""
    cursor = db_connection.cursor()
    cursor.execute('''
    UPDATE thread_status
    SET is_notified = 0, last_notification = NULL
    WHERE thread_id = ?
    ''', (thread_id,))
    db_connection.commit()


def unpack_manager_data(manager_tuple):
    """Safely unpack manager data tuple"""
    if manager_tuple and len(manager_tuple) >= 8:
        return manager_tuple
    # Возвращаем кортеж с значениями по умолчанию
    return (None, "", "", "", "", None, None, None)
