from pyrogram import Client, filters
import pyrogram
import sqlite3
import datetime
import logging
import random
import re
import os
import asyncio
import asyncio
group_locks = {}
media_groups_data = {}
manager_media_groups = {}  # Инициализированные группы с известным thread_id
pending_media_groups = {}  # Временное хранилище для файлов, пока не найден файл с командой
client_media_groups = {}  # Глобальный словарь для хранения медиа-групп от клиентов

from config import API_ID, API_HASH, PHONE_NUMBER, SUPPORT_GROUP_ID
from config import URGENT_WAIT_TIME, FIRST_NOTIFICATION_DELAY, NOTIFICATION_INTERVAL, CHECK_INTERVAL
from config import API_ID, API_HASH, PHONE_NUMBER, SUPPORT_GROUP_ID, STORAGE_CHANNEL_ID

# Константы для настройки интервалов (в начале файла)
URGENT_WAIT_TIME = 10         # Сообщение становится срочным через 10 минут
FIRST_NOTIFICATION_DELAY = 0  # Уведомление отправляется сразу
NOTIFICATION_INTERVAL = 20     # Повторные уведомления каждые 20 минут
CHECK_INTERVAL = 3          # Проверка каждые 3 минуты

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Бизнес-аккаунт с дополнительными настройками
business = Client(
    "business_account",
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE_NUMBER,
    parse_mode=pyrogram.enums.ParseMode.MARKDOWN,
    workers=16
)


# Настройка базы данных SQLite
def setup_database():
    conn = sqlite3.connect('clients_main_v2.db')
    cursor = conn.cursor()
    
    # Создаем таблицу клиентов, если она не существует
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS clients (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        username TEXT,
        first_contact TIMESTAMP,
        last_contact TIMESTAMP,
        message_count INTEGER DEFAULT 1,
        thread_id INTEGER DEFAULT NULL
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
        auth_date TEXT
    )
    ''')
    
    # Добавляем колонку username, если ее нет
    try:
        cursor.execute('SELECT username FROM managers LIMIT 1')
    except sqlite3.OperationalError:
        # Если колонки нет, добавляем ее
        cursor.execute('ALTER TABLE managers ADD COLUMN username TEXT')
        logger.info("Добавлена колонка username в таблицу managers")
    
    # Добавляем колонку photo_path, если её нет
    try:
        cursor.execute('SELECT photo_path FROM managers LIMIT 1')
    except sqlite3.OperationalError:
        # Если колонки нет, добавляем ее
        cursor.execute('ALTER TABLE managers ADD COLUMN photo_path TEXT')
        logger.info("Добавлена колонка photo_path в таблицу managers")
    
    # Создаем таблицу первых ответов (для отслеживания первого ответа менеджера)
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
    
    # Добавляем таблицу для хранения файлов
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
    
    # Добавляем таблицу для хранения истории версий файлов
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
    return conn
    
# Функция для обновления структуры базы данных
def update_database_schema(conn):
    cursor = conn.cursor()
    
    # Добавляем колонку для хранения message_id фото менеджера в канале
    try:
        cursor.execute('SELECT photo_storage_msg_id FROM managers LIMIT 1')
        logger.info("Колонка photo_storage_msg_id уже существует в таблице managers")
    except sqlite3.OperationalError:
        # Если колонки нет, добавляем ее
        cursor.execute('ALTER TABLE managers ADD COLUMN photo_storage_msg_id INTEGER')
        logger.info("Добавлена колонка photo_storage_msg_id в таблицу managers")
    
    # Создаем таблицу для хранения файлов, если её нет
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
    return conn
 
# Инициализация базы данных
db_connection = setup_database()
db_connection = update_database_schema(db_connection)  # Добавляем эту строку после инициализации
 
    
# Глобальное соединение с базой данных
db_connection = setup_database()

# Глобальное хранилище для процесса авторизации менеджеров
manager_auth_state = {}

# Функция для сохранения/обновления информации о клиенте
def save_client(conn, user):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    # Проверяем, существует ли пользователь
    cursor.execute('SELECT message_count, thread_id FROM clients WHERE user_id = ?', (user.id,))
    result = cursor.fetchone()
    
    if result:
        # Обновляем существующего пользователя
        message_count = result[0] + 1
        thread_id = result[1]
        cursor.execute('''
        UPDATE clients 
        SET last_contact = ?, message_count = ? 
        WHERE user_id = ?
        ''', (current_time, message_count, user.id))
    else:
        # Добавляем нового пользователя
        thread_id = None
        cursor.execute('''
        INSERT INTO clients (user_id, first_name, last_name, username, first_contact, last_contact, thread_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user.id, user.first_name, user.last_name, user.username, current_time, current_time, thread_id))
    
    conn.commit()
    return thread_id

# Функция для обновления thread_id клиента
def update_client_thread(conn, user_id, thread_id):
    cursor = conn.cursor()
    cursor.execute('UPDATE clients SET thread_id = ? WHERE user_id = ?', (thread_id, user_id))
    conn.commit()
    

    
# Функция для создания треда с произвольным названием
async def create_support_thread(client, thread_title_base):
    try:
        # Пробуем создать тему в форуме
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        random_id = random.randint(1, 999999)
        
        # Сначала создаем с временным названием
        result = await client.invoke(
            pyrogram.raw.functions.channels.CreateForumTopic(
                channel=peer,
                title=thread_title_base,
                random_id=random_id,
                icon_color=random.randint(0, 7)
            )
        )
        
        # Ищем ID треда в структуре ответа
        thread_id = None
        
        # Ищем в updates
        if hasattr(result, 'updates'):
            for update in result.updates:
                if hasattr(update, 'message') and hasattr(update.message, 'action'):
                    # Это сервисное сообщение о создании темы
                    if update.message.id:
                        thread_id = update.message.id
                        logger.info(f"Найден ID треда в обновлении: {thread_id}")
                        break
        
        if not thread_id:
            logger.error("Не удалось извлечь ID треда из ответа API")
            return None
            
        logger.info(f"Тред успешно создан с ID: {thread_id}")
        
        # Обновляем название треда, чтобы включить его ID
        thread_title = f"{thread_id}: {thread_title_base}"
        try:
            await client.invoke(
                pyrogram.raw.functions.channels.EditForumTopic(
                    channel=peer,
                    topic_id=thread_id,
                    title=thread_title
                )
            )
            logger.info(f"Название треда обновлено на '{thread_title}'")
        except Exception as e:
            logger.error(f"Ошибка при обновлении названия треда: {e}")
        
        return thread_id
    except Exception as e:
        logger.error(f"Ошибка при создании треда: {e}")
        return None
    
# Проверка, является ли группа форумом и имеет ли бот права на создание тем
async def check_forum_capabilities(client):
    try:
        # Получаем информацию о группе
        chat = await client.get_chat(SUPPORT_GROUP_ID)
        logger.info(f"Информация о группе: {chat.title}, тип: {chat.type}")
        
        # Проверяем, имеет ли бот нужные права в группе
        me = await client.get_me()
        bot_member = await client.get_chat_member(SUPPORT_GROUP_ID, me.id)
        logger.info(f"Права бота в группе: {bot_member.status}")
        
        # Проверяем, является ли группа форумом
        is_forum = getattr(chat, 'is_forum', False)
        logger.info(f"Группа является форумом: {is_forum}")
        
        return is_forum
    except Exception as e:
        logger.error(f"Ошибка при проверке форума: {e}")
        return False

# Создание треда для клиента
async def create_thread_for_client(client, user):
    try:
        # Формируем имя клиента для заголовка треда
        client_name = f"{user.first_name}"
        if user.last_name:
            client_name += f" {user.last_name}"
        if user.username:
            client_name += f" (@{user.username})"
        
        # Получаем количество клиентов в базе данных для нумерации
        cursor = db_connection.cursor()
        cursor.execute('SELECT COUNT(*) FROM clients')
        client_count = cursor.fetchone()[0] + 1  # +1, чтобы начать с 1, а не с 0
        
        # Генерируем уникальный номер треда на основе даты и порядкового номера
        current_date = datetime.datetime.now().strftime('%y%m%d')
        thread_number = f"{current_date}-{client_count}"
        
        thread_title = f"{thread_number}: {client_name} (ID: {user.id})"
        logger.info(f"Создание треда с названием: {thread_title}")
        
        # Пробуем создать тему в форуме
        try:
            # Используем правильный метод вызова API Pyrogram
            peer = await client.resolve_peer(SUPPORT_GROUP_ID)
            random_id = random.randint(1, 999999)
            
            result = await client.invoke(
                pyrogram.raw.functions.channels.CreateForumTopic(
                    channel=peer,
                    title=thread_title,
                    random_id=random_id,
                    icon_color=random.randint(0, 7)
                )
            )
            
            # Обработка ответа API
            # Выводим полный ответ для отладки
            logger.info(f"Получен ответ API: {result}")
            
            # Ищем ID треда в структуре ответа
            thread_id = None
            
            # Ищем в updates
            if hasattr(result, 'updates'):
                for update in result.updates:
                    if hasattr(update, 'message') and hasattr(update.message, 'action'):
                        # Это сервисное сообщение о создании темы
                        if update.message.id:
                            thread_id = update.message.id
                            logger.info(f"Найден ID треда в обновлении: {thread_id}")
                            break
            
            if not thread_id:
                logger.error("Не удалось извлечь ID треда из ответа API")
                return None
            
            logger.info(f"Тред успешно создан с ID: {thread_id}")
            
            # Формируем новый заголовок с номером треда
            new_thread_title = f"{thread_id}: {client_name}"
            
            # Пробуем изменить заголовок треда
            await edit_thread_title(client, thread_id, new_thread_title)
            
            # Отправляем карточку клиента в тред
            card_text = f"**Карточка клиента**\n"
            card_text += f"**Имя**: {client_name}\n"
            card_text += f"**ID**: {user.id}\n"
            card_text += f"**Дата обращения**: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=card_text,
                reply_to_message_id=thread_id
            )
      
            return thread_id
        except Exception as e:
            logger.error(f"Ошибка при создании треда через API: {e}")
            return None
    except Exception as e:
        logger.error(f"Общая ошибка при создании треда: {e}")
        return None
        
# Функция для генерации client_id
def generate_client_id(conn, user_id, manager_id=None):
    cursor = conn.cursor()
    
    # Проверяем, есть ли уже назначенный ID для этого клиента
    cursor.execute('SELECT custom_id FROM clients WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    
    if result and result[0]:
        return result[0]  # Возвращаем существующий ID
    
    # Получаем имя клиента для генерации префикса
    cursor.execute('SELECT first_name, last_name FROM clients WHERE user_id = ?', (user_id,))
    name_result = cursor.fetchone()
    
    prefix = ""
    if name_result:
        first_name, last_name = name_result
        
        # Берем первую букву имени и первую букву фамилии (если есть)
        if first_name:
            # Убедимся, что берем только кириллицу или латиницу
            first_char = first_name[0].upper()
            # Проверяем, что это буква (не символ, не цифра)
            if re.match(r'[А-ЯA-Z]', first_char):
                prefix += first_char
        if last_name:
            last_char = last_name[0].upper()
            if re.match(r'[А-ЯA-Z]', last_char):
                prefix += last_char
    
    if not prefix or len(prefix) < 2:
        # Если недостаточно символов, дополняем до двух букв
        prefix = prefix.ljust(2, 'C')
    
    # Ограничиваем префикс двумя символами
    prefix = prefix[:2]
    
    # Генерируем числовую часть ID с датой
    current_date = datetime.datetime.now().strftime('%y%m')  # Год и месяц (например, 2504)
    
    # Получаем счетчик клиентов для текущего месяца с этим префиксом
    cursor.execute('SELECT COUNT(*) FROM clients WHERE custom_id LIKE ?', (f"{prefix}{current_date}%",))
    count = cursor.fetchone()[0] + 1  # +1 для нового клиента
    
    # Форматируем числовую часть как трехзначное число с ведущими нулями
    number_part = f"{count:03d}"
    
    # Собираем итоговый ID: префикс + дата + порядковый номер
    custom_id = f"{prefix}{current_date}{number_part}"
    
    # Обновляем запись клиента с новым custom_id
    cursor.execute('UPDATE clients SET custom_id = ? WHERE user_id = ?', (custom_id, user_id))
    
    # Если указан ID менеджера, назначаем его ответственным
    if manager_id:
        # Получаем thread_id клиента
        cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (user_id,))
        thread_result = cursor.fetchone()
        
        if thread_result and thread_result[0]:
            thread_id = thread_result[0]
            
            # Получаем username менеджера
            cursor.execute('SELECT username FROM managers WHERE manager_id = ?', (manager_id,))
            manager_result = cursor.fetchone()
            
            if manager_result and manager_result[0]:
                manager_username = manager_result[0]
                
                # Назначаем менеджера ответственным
                assign_duty_manager(conn, thread_id, manager_username, manager_id)
                logger.info(f"Менеджер {manager_username} (ID: {manager_id}) назначен ответственным за клиента {custom_id}")
    
    conn.commit()
    return custom_id

# Функция для получения thread_id по custom_id    
def get_thread_id_by_custom_id(conn, custom_id):
    cursor = conn.cursor()
    
    # Добавим логирование для отладки
    logger.info(f"Поиск thread_id и user_id для custom_id={custom_id}")
    
    # Выполним SQL-запрос и выведем результат
    cursor.execute('SELECT thread_id, user_id FROM clients WHERE custom_id = ?', (custom_id,))
    result = cursor.fetchone()
    
    if result:
        logger.info(f"Найдено: thread_id={result[0]}, user_id={result[1]} для custom_id={custom_id}")
        return result[0], result[1]  # Возвращаем thread_id и user_id
    
    logger.error(f"Не найден клиент с custom_id={custom_id}")
    
    # Проверим, есть ли вообще записи с заполненным custom_id
    cursor.execute('SELECT custom_id FROM clients WHERE custom_id IS NOT NULL LIMIT 5')
    samples = cursor.fetchall()
    if samples:
        logger.info(f"Примеры существующих custom_id: {', '.join([s[0] for s in samples if s[0]])}")
    else:
        logger.error(f"В базе нет клиентов с заполненным custom_id")
    
    return None, None
  
# Вспомогательная функция для асинхронной блокировки      
async def acquire_group_lock(group_id):
    if group_id not in group_locks:
        group_locks[group_id] = asyncio.Lock()
    
    lock = group_locks[group_id]
    await lock.acquire()
    return lock

# Функция для получения списка всех активных тредов
def get_all_active_threads(conn):
    cursor = conn.cursor()
    
    # Получаем информацию о тредах, объединяя данные из нескольких таблиц
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
    
    threads = cursor.fetchall()
    return threads
  
# Функция для сохранения сообщения
def save_message(conn, user_id, message_text, is_from_user=True, media_type=None):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    # Добавляем тип медиа, если он указан
    media_info = f" [{media_type}]" if media_type else ""
    full_message = f"{message_text}{media_info}"
    
    cursor.execute('''
    INSERT INTO messages (user_id, message_text, timestamp, is_from_user)
    VALUES (?, ?, ?, ?)
    ''', (user_id, full_message, current_time, is_from_user))
    
    conn.commit()

# Функция для сохранения менеджера
def save_manager(conn, manager_id, emoji, name, position, extension, photo_file_id=None, username=None, photo_path=None):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    cursor.execute('''
    INSERT OR REPLACE INTO managers (manager_id, emoji, name, position, extension, photo_file_id, auth_date, username, photo_path)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (manager_id, emoji, name, position, extension, photo_file_id, current_time, username, photo_path))
    
    conn.commit()

# Функция для получения данных менеджера
def get_manager(conn, manager_id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM managers WHERE manager_id = ?', (manager_id,))
    manager = cursor.fetchone()
    return manager

# Функция для обновления фотографии менеджера
def update_manager_photo(conn, manager_id, photo_file_id, photo_path=None):
    cursor = conn.cursor()
    cursor.execute('UPDATE managers SET photo_file_id = ?, photo_path = ? WHERE manager_id = ?', 
                  (photo_file_id, photo_path, manager_id))
    conn.commit()

def unpack_manager_data(manager):
    """Безопасно распаковывает данные менеджера из базы данных"""
    if not manager:
        return None
    
    # Базовые поля (обязательные)
    manager_id = manager[0] if len(manager) > 0 else None
    emoji = manager[1] if len(manager) > 1 else ""
    name = manager[2] if len(manager) > 2 else ""
    position = manager[3] if len(manager) > 3 else ""
    extension = manager[4] if len(manager) > 4 else ""
    photo_file_id = manager[5] if len(manager) > 5 else None
    auth_date = manager[6] if len(manager) > 6 else None
    username = manager[7] if len(manager) > 7 else None
    
    return manager_id, emoji, name, position, extension, photo_file_id, auth_date, username

# Функция для получения клиента по thread_id
def get_client_by_thread(conn, thread_id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM clients WHERE thread_id = ?', (thread_id,))
    client = cursor.fetchone()
    return client

# Функция для отслеживания первого ответа менеджера
def save_first_reply(conn, thread_id, client_id, manager_id):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    # Проверяем, есть ли уже запись
    cursor.execute('''
    SELECT * FROM first_replies WHERE thread_id = ? AND manager_id = ?
    ''', (thread_id, manager_id))
    
    if not cursor.fetchone():
        # Если это первый ответ, создаем запись
        cursor.execute('''
        INSERT INTO first_replies (thread_id, client_id, manager_id, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (thread_id, client_id, manager_id, current_time))
        conn.commit()
        return True
    
    return False

# Функция для проверки, является ли ответ первым для менеджера
def is_first_reply(conn, thread_id, manager_id):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * FROM first_replies WHERE thread_id = ? AND manager_id = ?
    ''', (thread_id, manager_id))
    return cursor.fetchone() is None

# Функция для получения списка менеджеров, отвечавших клиенту
def get_managers_replied_to_client(conn, thread_id):
    cursor = conn.cursor()
    
    # Сначала проверяем, есть ли колонка username
    try:
        cursor.execute('''
        SELECT m.manager_id, m.username 
        FROM first_replies fr
        JOIN managers m ON fr.manager_id = m.manager_id
        WHERE fr.thread_id = ?
        ''', (thread_id,))
        managers = cursor.fetchall()
    except sqlite3.OperationalError:
        # Если колонки username нет, используем только ID
        cursor.execute('''
        SELECT m.manager_id, NULL 
        FROM first_replies fr
        JOIN managers m ON fr.manager_id = m.manager_id
        WHERE fr.thread_id = ?
        ''', (thread_id,))
        managers = cursor.fetchall()
    
    return managers
        
# Функция для отправки медиа-группы клиенту
async def send_manager_media_group_to_client(client, manager_id, client_id, media_group_data):
    try:
        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False
        
        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
        
        # Формируем подпись менеджера
        signature = f"\n—\n{emoji} {name}, {position}, доб. {extension}"
        
        # Полная подпись с текстом сообщения
        caption = media_group_data.get("caption", "")
        full_caption = f"{caption}{signature}"
        
        # Создаем массив медиа для отправки
        media_group = []
        
        # Добавляем все файлы группы
        first_item = True
        for msg in media_group_data["messages"]:
            if hasattr(msg, 'photo') and msg.photo:
                if first_item:
                    media_group.append(pyrogram.types.InputMediaPhoto(msg.photo.file_id, caption=full_caption))
                    first_item = False
                else:
                    media_group.append(pyrogram.types.InputMediaPhoto(msg.photo.file_id))
            elif hasattr(msg, 'document') and msg.document:
                if first_item:
                    media_group.append(pyrogram.types.InputMediaDocument(msg.document.file_id, caption=full_caption))
                    first_item = False
                else:
                    media_group.append(pyrogram.types.InputMediaDocument(msg.document.file_id))
            elif hasattr(msg, 'video') and msg.video:
                if first_item:
                    media_group.append(pyrogram.types.InputMediaVideo(msg.video.file_id, caption=full_caption))
                    first_item = False
                else:
                    media_group.append(pyrogram.types.InputMediaVideo(msg.video.file_id))
        
        # Проверяем, есть ли медиа для отправки
        if not media_group:
            logger.error(f"Нет медиафайлов для отправки клиенту {client_id}")
            return False
        
        # Отправляем медиа-группу клиенту
        await client.send_media_group(
            chat_id=client_id,
            media=media_group
        )
        
        logger.info(f"Медиа-группа от менеджера отправлена клиенту {client_id}")
        
        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, f"{caption or '[Медиафайлы]'}{signature}", is_from_user=False, media_type="MEDIA_GROUP")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке медиа-группы от менеджера: {e}")
        return False


# Функция для периодической очистки устаревших групп
async def cleanup_processing_groups():
    """Периодически проверяет и обрабатывает зависшие медиа-группы"""
    while True:
        try:
            await asyncio.sleep(30)  # Проверка каждые 30 секунд
            
            # Проверяем, инициализирован ли словарь групп
            if not hasattr(process_manager_media_group_after_delay, "processing_groups"):
                continue
            
            current_time = datetime.datetime.now()
            groups_to_process = []
            
            # Находим группы, которые не обновлялись более 5 секунд
            for group_id, group_data in manager_media_groups.items():
                if not group_data.get("processed", False):
                    time_since_update = (current_time - group_data.get("timestamp", current_time)).total_seconds()
                    if time_since_update > 5:
                        # Если прошло более 5 секунд с последнего обновления, считаем, что все файлы получены
                        groups_to_process.append(group_id)
                        logger.info(f"Группа {group_id} не обновлялась {time_since_update:.1f} секунд - добавлена в очередь обработки")
            
            # Обрабатываем найденные группы
            for group_id in groups_to_process:
                if group_id in manager_media_groups:
                    group_data = manager_media_groups[group_id]
                    
                    # Запускаем обработку, если не запущена
                    if (not group_data.get("processed", False) and 
                        (not group_data.get("processing_task") or group_data.get("processing_task").done())):
                        
                        # Запускаем обработку группы
                        asyncio.create_task(process_manager_media_group_after_delay(
                            client, group_id, 1  # 1 секунда задержки
                        ))
                        
        except Exception as e:
            logger.error(f"Ошибка в процессе очистки групп: {e}")
            await asyncio.sleep(10)  # При ошибке делаем короткую паузу

# Функция для обработки медиа-групп от клиента
async def handle_client_media_group(client, message, thread_id=None):
    """
    Обрабатывает медиа-группы от клиента, собирая их и отправляя в группу поддержки
    так, чтобы они выглядели как единая группа
    """
    # Инициализируем хранилище медиа-групп клиентов
    if not hasattr(handle_client_media_group, "client_media_groups"):
        handle_client_media_group.client_media_groups = {}
    
    media_group_id = message.media_group_id
    user_id = message.from_user.id
    
    # Создаем ключ группы, включающий thread_id для уникальности
    group_key = f"{media_group_id}_{thread_id}_{user_id}"
    
    # Проверяем, существует ли уже запись для этой группы
    if group_key not in handle_client_media_group.client_media_groups:
        # Создаем новую запись
        handle_client_media_group.client_media_groups[group_key] = {
            "messages": [],
            "user_id": user_id,
            "thread_id": thread_id,
            "timestamp": datetime.datetime.now(),
            "processed": False
        }
        logger.info(f"Создана новая запись для медиа-группы клиента {group_key}")
    
    # Добавляем сообщение в группу
    handle_client_media_group.client_media_groups[group_key]["messages"].append(message)
    logger.info(f"Добавлено сообщение в медиа-группу клиента {group_key}, всего: {len(handle_client_media_group.client_media_groups[group_key]['messages'])}")
    
    # Если это первое сообщение, запускаем таймер для обработки
    if len(handle_client_media_group.client_media_groups[group_key]["messages"]) == 1:
        async def process_client_group():
            await asyncio.sleep(2)  # Ждем 2 секунды для сбора всех файлов
            
            if group_key in handle_client_media_group.client_media_groups and not handle_client_media_group.client_media_groups[group_key]["processed"]:
                # Получаем данные группы
                group_data = handle_client_media_group.client_media_groups[group_key]
                group_data["processed"] = True
                
                try:
                    # Если есть thread_id, отправляем информационное сообщение
                    if thread_id:
                        # Обновляем время последнего сообщения клиента
                        update_client_message_time(db_connection, thread_id)
                        
                        # Определяем имя отправителя
                        user_name = f"{message.from_user.first_name}"
                        if message.from_user.last_name:
                            user_name += f" {message.from_user.last_name}"
                        if message.from_user.username:
                            user_name += f" @{message.from_user.username}"
                        
                        # Получаем ответственного менеджера
                        duty_manager = get_duty_manager(db_connection, thread_id)
                        manager_mention = ""
                        if duty_manager:
                            manager_mention = f"\n—\n@{duty_manager}"
                        
                        # Формируем общее информационное сообщение
                        info_text = f"**{user_name}** отправил(а) группу из {len(group_data['messages'])} файлов{manager_mention}"
                        
                        # Отправляем сообщение
                        await client.send_message(
                            chat_id=SUPPORT_GROUP_ID,
                            text=info_text,
                            reply_to_message_id=thread_id,
                            parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                        )
                    
                    # Подготавливаем медиа-группу для отправки
                    media_list = []
                    
                    # Добавляем все файлы в список
                    for i, msg in enumerate(group_data["messages"]):
                        # Подготавливаем подпись для файла
                        caption = ""
                        if msg.caption:
                            caption = msg.caption
                        
                        # В первый файл добавляем информацию о номере
                        if i == 0 and len(group_data["messages"]) > 1:
                            if caption:
                                caption = f"Медиа-группа из {len(group_data['messages'])} файлов\n\n{caption}"
                            else:
                                caption = f"Медиа-группа из {len(group_data['messages'])} файлов"
                        
                        # Создаем соответствующий InputMedia объект в зависимости от типа медиа
                        if hasattr(msg, 'photo') and msg.photo:
                            media_list.append(pyrogram.types.InputMediaPhoto(
                                media=msg.photo.file_id,
                                caption=caption if i == 0 else None
                            ))
                        elif hasattr(msg, 'document') and msg.document:
                            media_list.append(pyrogram.types.InputMediaDocument(
                                media=msg.document.file_id,
                                caption=caption if i == 0 else None
                            ))
                        elif hasattr(msg, 'video') and msg.video:
                            media_list.append(pyrogram.types.InputMediaVideo(
                                media=msg.video.file_id,
                                caption=caption if i == 0 else None
                            ))
                        elif hasattr(msg, 'audio') and msg.audio:
                            media_list.append(pyrogram.types.InputMediaAudio(
                                media=msg.audio.file_id,
                                caption=caption if i == 0 else None
                            ))
                    
                    # Если список не пуст, отправляем медиа-группу
                    if media_list:
                        kwargs = {
                            "chat_id": SUPPORT_GROUP_ID,
                            "media": media_list
                        }
                        
                        if thread_id:
                            kwargs["reply_to_message_id"] = thread_id
                        
                        # Отправляем медиа-группу
                        await client.send_media_group(**kwargs)
                        logger.info(f"Отправлена медиа-группа с {len(media_list)} файлами в группу поддержки" + 
                                  (f", тред {thread_id}" if thread_id else ""))
                    else:
                        logger.warning(f"Не удалось подготовить медиа-файлы для отправки в группу {group_key}")
                    
                    # Сохраняем сообщение в базу данных
                    caption_text = ""
                    for msg in group_data["messages"]:
                        if msg.caption:
                            caption_text = msg.caption
                            break
                    
                    save_message(db_connection, user_id, 
                               f"Группа медиафайлов ({len(group_data['messages'])} шт.)" + 
                               (f": {caption_text}" if caption_text else ""),
                               is_from_user=True, media_type="MEDIA_GROUP")
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке медиа-группы клиента {group_key}: {e}")
                    
                    # Если произошла ошибка, отправляем каждый файл отдельно как запасной вариант
                    try:
                        if thread_id:
                            for msg in group_data["messages"]:
                                try:
                                    await client.copy_message(
                                        chat_id=SUPPORT_GROUP_ID,
                                        from_chat_id=msg.chat.id,
                                        message_id=msg.id,
                                        reply_to_message_id=thread_id
                                    )
                                    logger.info(f"Отправлен отдельный файл из медиа-группы в тред {thread_id} (запасной вариант)")
                                except Exception as copy_error:
                                    logger.error(f"Ошибка при отправке отдельного файла: {copy_error}")
                    except Exception as fallback_error:
                        logger.error(f"Ошибка при запасном отправлении файлов: {fallback_error}")
                
                # Удаляем данные группы через 30 секунд
                asyncio.create_task(delete_client_media_group_after_delay(group_key, 30))
        
        # Запускаем обработку
        asyncio.create_task(process_client_group())
    
    return True
 
# Обновленная функция для пересылки сообщений клиента в группу поддержки
async def forward_message_to_support(client, message, thread_id=None):
    try:
        # Проверяем, является ли сообщение частью медиа-группы
        if hasattr(message, 'media_group_id') and message.media_group_id:
            # Обрабатываем медиа-группу от клиента
            return await handle_client_media_group(client, message, thread_id)
        
        # Дальше идет обычная обработка одиночных сообщений (оставить как было)
        if thread_id:
            # Пробуем отправить сообщение в тред
            logger.info(f"Пересылка сообщения в тред {thread_id}")
            
            try:
                # Получаем ответственного менеджера для этого треда
                duty_manager = get_duty_manager(db_connection, thread_id)
                manager_mention = ""
                
                # Если есть ответственный менеджер, добавляем его упоминание
                if duty_manager:
                    manager_mention = f"\n—\n@{duty_manager}"
                
                # Определяем имя отправителя
                user_name = f"{message.from_user.first_name}"
                if message.from_user.last_name:
                    user_name += f" {message.from_user.last_name}"
                if message.from_user.username:
                    user_name += f" @{message.from_user.username}"
                
                # Определяем индикаторы специального форматирования (явно выводим все параметры)
                special_format = []
                
                # Проверяем наличие reply_to_message
                if hasattr(message, 'reply_to_message') and message.reply_to_message:
                    reply_sender = "неизвестного отправителя"
                    if hasattr(message.reply_to_message, 'from_user') and message.reply_to_message.from_user:
                        reply_sender = message.reply_to_message.from_user.first_name or "пользователя"
                    
                    reply_text = "..."
                    if hasattr(message.reply_to_message, 'text') and message.reply_to_message.text:
                        reply_text = message.reply_to_message.text
                        if len(reply_text) > 50:
                            reply_text = reply_text[:47] + "..."
                    
                    special_format.append(f"↩️ Ответ на сообщение от {reply_sender}: \"{reply_text}\"")
                
                # Проверяем пересланные сообщения
                is_forwarded = False
                forward_info = ""
                
                if hasattr(message, 'forward_from') and message.forward_from:
                    is_forwarded = True
                    forward_name = f"{message.forward_from.first_name or ''}"
                    if hasattr(message.forward_from, 'last_name') and message.forward_from.last_name:
                        forward_name += f" {message.forward_from.last_name}"
                    forward_info = f"↪️ Переслано от: {forward_name}"
                
                elif hasattr(message, 'forward_sender_name') and message.forward_sender_name:
                    is_forwarded = True
                    forward_info = f"↪️ Переслано от: {message.forward_sender_name}"
                
                elif hasattr(message, 'forward_from_chat') and message.forward_from_chat:
                    is_forwarded = True
                    chat_name = message.forward_from_chat.title or "канала/группы"
                    forward_info = f"↪️ Переслано из: {chat_name}"
                
                if is_forwarded:
                    special_format.append(forward_info)
                
                # Проверяем наличие форматирования текста
                has_formatting = False
                if hasattr(message, 'entities') and message.entities:
                    for entity in message.entities:
                        if hasattr(entity, 'type') and entity.type in ["bold", "italic", "underline", "strikethrough", "spoiler", "code", "pre", "blockquote"]:
                            has_formatting = True
                            break
                
                if has_formatting:
                    special_format.append("🔠 Текст содержит форматирование (жирный, курсив, спойлер и т.д.)")
                
                # Проверяем наличие медиа
                media_type = None
                if hasattr(message, 'photo') and message.photo:
                    media_type = "📷 Фото"
                elif hasattr(message, 'video') and message.video:
                    media_type = "🎬 Видео"
                elif hasattr(message, 'voice') and message.voice:
                    media_type = "🎤 Голосовое сообщение"
                elif hasattr(message, 'audio') and message.audio:
                    media_type = "🎵 Аудио"
                elif hasattr(message, 'document') and message.document:
                    media_type = "📎 Документ"
                elif hasattr(message, 'sticker') and message.sticker:
                    media_type = "🎭 Стикер"
                elif hasattr(message, 'animation') and message.animation:
                    media_type = "🎞️ Анимация"
                
                if media_type:
                    special_format.append(f"{media_type}")
                
                # Формируем заголовок сообщения
                message_header = f"**{user_name}:**"
                
                # Если есть специальное форматирование, добавляем информацию о нем
                format_info = ""
                if special_format:
                    format_info = "\n\n**Дополнительная информация:**\n"
                    format_info += "\n".join([f"• {item}" for item in special_format])
                
                # Получаем основной текст сообщения
                message_content = ""
                if hasattr(message, 'text') and message.text:
                    message_content = message.text
                elif hasattr(message, 'caption') and message.caption:
                    message_content = message.caption
                
                if message_content:
                    message_content = f"\n\n{message_content}"
                
                # Собираем все части сообщения
                full_message = f"{message_header}{format_info}{message_content}{manager_mention}"
                
                # Отправляем информационное сообщение в тред
                await client.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=full_message,
                    reply_to_message_id=thread_id,
                    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                )
                
                # Если это медиа, пробуем скопировать его отдельно без подписи
                if media_type and not is_forwarded:
                    try:
                        await client.copy_message(
                            chat_id=SUPPORT_GROUP_ID,
                            from_chat_id=message.chat.id,
                            message_id=message.id,
                            reply_to_message_id=thread_id,
                            caption=""  # Пустая подпись, т.к. текст уже отправлен
                        )
                        logger.info(f"Медиа успешно скопировано в тред {thread_id}")
                    except Exception as media_error:
                        logger.error(f"Ошибка при копировании медиа: {media_error}")
                
                logger.info(f"Сообщение клиента отправлено в тред {thread_id}")
                return True
            except Exception as e:
                # Если это ошибка TOPIC_DELETED, нужно сообщить вызывающему коду
                if "TOPIC_DELETED" in str(e):
                    logger.warning(f"Тред {thread_id} был удален. Создаем новый тред.")
                    # Обнуляем thread_id в базе данных для этого клиента
                    update_client_thread(db_connection, message.from_user.id, None)
                    # Возвращаем специальный код ошибки
                    return "TOPIC_DELETED"
                else:
                    logger.error(f"Ошибка при отправке в тред: {e}")
                    return False
        
        # Если треда нет или была ошибка выше, пересылаем в основную группу
        try:
            forwarded = await client.forward_messages(
                chat_id=SUPPORT_GROUP_ID,
                from_chat_id=message.chat.id,
                message_ids=message.id
            )
            logger.info("Сообщение пересылано в группу поддержки (без треда)")
            return True
        except Exception as forward_error:
            logger.error(f"Ошибка при пересылке сообщения: {forward_error}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при пересылке сообщения: {e}")
        return False
    
# Функция для отложенного удаления группы клиента
async def delete_client_media_group_after_delay(group_key, delay_seconds):
    """Удаляет данные о медиа-группе клиента после указанной задержки"""
    await asyncio.sleep(delay_seconds)
    if hasattr(handle_client_media_group, "client_media_groups") and group_key in handle_client_media_group.client_media_groups:
        del handle_client_media_group.client_media_groups[group_key]
        logger.info(f"Удалены данные о медиа-группе клиента {group_key}")
    
# Функция для отложенного удаления группы 
async def delete_media_group_after_delay(media_group_id, delay):
    await asyncio.sleep(delay)
    if media_group_id in media_groups_data:
        del media_groups_data[media_group_id]

# Функция для периодической очистки старых медиа-групп
async def cleanup_manager_media_groups():
    """Периодически очищает устаревшие записи о медиа-группах менеджеров"""
    while True:
        try:
            await asyncio.sleep(60)  # Проверяем каждую минуту
            
            current_time = datetime.datetime.now()
            groups_to_remove = []
            
            # Ищем группы старше 5 минут
            for group_id, group_data in manager_media_groups.items():
                time_diff = (current_time - group_data["timestamp"]).total_seconds()
                if time_diff > 300:  # 5 минут
                    groups_to_remove.append(group_id)
            
            # Удаляем устаревшие группы
            for group_id in groups_to_remove:
                if group_id in manager_media_groups:
                    del manager_media_groups[group_id]
                    logger.info(f"Удалена устаревшая медиа-группа {group_id}")
        except Exception as e:
            logger.error(f"Ошибка при очистке медиа-групп: {e}")
            await asyncio.sleep(30)

# Отправка сообщения от менеджера клиенту
async def send_manager_reply_to_client(client, manager_id, client_id, message_text):
    try:
        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False
        
        # Безопасная распаковка данных менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = unpack_manager_data(manager)
        
        # Формируем подпись менеджера
        signature = f"\n—\n{emoji} {name}, {position}, доб. {extension}"
        
        # Полное сообщение с подписью
        full_message = f"{message_text}{signature}"
        
        # Отправляем сообщение клиенту
        await client.send_message(
            chat_id=client_id,
            text=full_message
        )
        logger.info(f"Ответ менеджера отправлен клиенту {client_id}")
        
        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, full_message, is_from_user=False)
        
        # Автоматически назначаем ответственного менеджера, если у менеджера есть username
        if username:
            # Сначала нужно получить thread_id для этого клиента
            cursor = db_connection.cursor()
            cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (client_id,))
            result = cursor.fetchone()
            
            if result and result[0]:
                thread_id = result[0]
                # Назначаем менеджера ответственным
                assign_duty_manager(db_connection, thread_id, username, manager_id)
                logger.info(f"Менеджер @{username} автоматически назначен ответственным за клиента {client_id} (тред {thread_id})")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке ответа менеджера: {e}")
        return False


# Отправка улучшенной карточки менеджера клиенту (при первом ответе)
async def send_manager_card_to_client(client, manager_id, client_id):
    try:
        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False
        
        # Логируем все данные о менеджере для отладки
        logger.info(f"Данные менеджера для отправки карточки: {manager}")
        logger.info(f"Длина массива данных менеджера: {len(manager)}")

        # Используем функцию unpack_manager_data для безопасной распаковки
        unpacked_data = unpack_manager_data(manager)
        if not unpacked_data:
            logger.error(f"Не удалось распаковать данные менеджера {manager_id}")
            return False

        manager_id_value, emoji, name, position, extension, photo_file_id, auth_date, username = unpacked_data

        # Получаем дополнительные поля безопасно
        photo_path = manager[8] if len(manager) > 8 else None
        photo_storage_msg_id = manager[9] if len(manager) > 9 else None
        
        # Логируем file_id фотографии
        logger.info(f"Photo file_id: {photo_file_id}, photo_path: {photo_path}, photo_storage_msg_id: {photo_storage_msg_id}")
        
        # Определяем приветствие в зависимости от времени суток
        current_hour = datetime.datetime.now().hour
        greeting = "Добрый день"
        
        if current_hour < 6:
            greeting = "Доброй ночи"
        elif current_hour < 12:
            greeting = "Доброе утро"
        elif current_hour >= 18:
            greeting = "Добрый вечер"
        
        # Получаем информацию о клиенте
        cursor = db_connection.cursor()
        cursor.execute('SELECT first_name, last_name FROM clients WHERE user_id = ?', (client_id,))
        client_data = cursor.fetchone()
        
        client_name = "Уважаемый клиент"
        if client_data:
            if client_data[0]:
                client_name = client_data[0]
                if client_data[1]:
                    client_name += f" {client_data[1]}"
        
        # Формируем текст карточки с персональным обращением
        card_text = f"{greeting}!\n\n"
        card_text += f"**Ваш менеджер {name}**\n"
        card_text += f"Должность: **{position}**\n\n"
        card_text += f"Для звонка используйте многоканальный номер из аккаунта и наберите добавочный: **{extension}**\n\n"
        card_text += "С большим интересом займемся вашим проектом"
        
        logger.info(f"Подготовлен текст карточки для клиента {client_id}")

        # Сначала попробуем отправить текстовое сообщение без фото, чтобы исключить проблемы с текстом
        try:
            await client.send_message(
                chat_id=client_id,
                text="Подготовка карточки менеджера..."
            )
            logger.info(f"Тестовое сообщение отправлено клиенту {client_id}")
        except Exception as text_error:
            logger.error(f"Ошибка при отправке тестового сообщения: {text_error}")
            return False

        # Если есть фото менеджера
        sent_with_photo = False
        
        # Сначала пробуем отправить по message_id из канала хранилища, если он есть
        if photo_storage_msg_id:
            try:
                logger.info(f"Попытка отправки карточки с фото из канала хранилища, message_id: {photo_storage_msg_id}")
                # Получаем сообщение из канала хранилища
                storage_message = await client.get_messages(STORAGE_CHANNEL_ID, photo_storage_msg_id)
                
                if storage_message and hasattr(storage_message, 'photo') and storage_message.photo:
                    # Отправляем фото из канала хранилища
                    sent_message = await client.send_photo(
                        chat_id=client_id,
                        photo=storage_message.photo.file_id,
                        caption=card_text,
                        parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                    )
                    logger.info(f"Карточка менеджера с фото из хранилища отправлена клиенту {client_id}")
                    sent_with_photo = True
                else:
                    logger.error(f"Не удалось получить фото из сообщения {photo_storage_msg_id} в канале хранилища")
            except Exception as storage_error:
                logger.error(f"Ошибка при отправке фото из хранилища: {storage_error}")
        
        # Если не удалось отправить из хранилища, пробуем по file_id
        if not sent_with_photo and photo_file_id:
            try:
                # Пробуем отправить по file_id
                logger.info(f"Попытка отправки карточки с фото менеджера {manager_id}, photo_id: {photo_file_id}")
                sent_message = await client.send_photo(
                    chat_id=client_id,
                    photo=photo_file_id,
                    caption=card_text,
                    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                )
                logger.info(f"Карточка менеджера с фото отправлена клиенту {client_id}, message_id: {sent_message.id}")
                sent_with_photo = True
            except Exception as e:
                logger.error(f"Ошибка при отправке фото по file_id: {e}")
                
                # Если с file_id проблема и есть локальный путь, пробуем отправить по пути
                if photo_path and os.path.exists(photo_path):
                    try:
                        logger.info(f"Попытка отправки фото по локальному пути: {photo_path}")
                        sent_message = await client.send_photo(
                            chat_id=client_id,
                            photo=photo_path,
                            caption=card_text,
                            parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                        )
                        logger.info(f"Карточка менеджера с локальным фото отправлена клиенту {client_id}")
                        sent_with_photo = True
                    except Exception as local_photo_error:
                        logger.error(f"Ошибка при отправке локального фото: {local_photo_error}")
        
        # Если фото не удалось отправить ни одним из способов, отправляем только текст
        if not sent_with_photo:
            try:
                logger.info(f"Отправка текстовой карточки без фото клиенту {client_id}")
                sent_message = await client.send_message(
                    chat_id=client_id,
                    text=card_text,
                    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                )
                logger.info(f"Текстовая карточка отправлена клиенту {client_id}")
            except Exception as text_card_error:
                logger.error(f"Ошибка при отправке текстовой карточки: {text_card_error}")
                return False
       
        # Автоматически назначаем ответственного менеджера
        if username:
            # Получаем thread_id клиента
            cursor = db_connection.cursor()
            cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (client_id,))
            thread_result = cursor.fetchone()
            
            if thread_result and thread_result[0]:
                thread_id = thread_result[0]
                # Назначаем менеджера ответственным
                assign_duty_manager(db_connection, thread_id, username, manager_id_value)
                logger.info(f"Менеджер @{username} автоматически назначен ответственным за клиента {client_id} (тред {thread_id})")
       
        return True
    except Exception as e:
        logger.error(f"Критическая ошибка при отправке карточки менеджера: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
        return False

# Функция для отправки медиафайла от менеджера клиенту
async def send_manager_media_to_client(client, manager_id, client_id, file_id, caption=None, media_type="photo"):
    try:
        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False
        
        # Безопасная распаковка данных менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = unpack_manager_data(manager)
        
        # Формируем подпись менеджера
        signature = f"\n—\n{emoji} {name}, {position}, доб. {extension}"
        
        # Полная подпись с текстом сообщения
        full_caption = f"{caption or ''}{signature}"
        
        # Отправляем медиафайл в зависимости от типа
        if media_type == "photo":
            await client.send_photo(
                chat_id=client_id,
                photo=file_id,
                caption=full_caption
            )
        elif media_type == "document":
            await client.send_document(
                chat_id=client_id,
                document=file_id,
                caption=full_caption
            )
        elif media_type == "video":
            await client.send_video(
                chat_id=client_id,
                video=file_id,
                caption=full_caption
            )
        elif media_type == "audio":
            await client.send_audio(
                chat_id=client_id,
                audio=file_id,
                caption=full_caption
            )
        elif media_type == "voice":
            await client.send_voice(
                chat_id=client_id,
                voice=file_id,
                caption=full_caption
            )
        
        logger.info(f"Медиафайл типа {media_type} от менеджера отправлен клиенту {client_id}")
        
        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, f"{caption or '[Медиафайл]'}{signature}", is_from_user=False, media_type=media_type.upper())
        
        # Автоматически назначаем ответственного менеджера, если у менеджера есть username
        if username:
            # Сначала нужно получить thread_id для этого клиента
            cursor = db_connection.cursor()
            cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (client_id,))
            result = cursor.fetchone()
            
            if result and result[0]:
                thread_id = result[0]
                # Назначаем менеджера ответственным
                assign_duty_manager(db_connection, thread_id, username, manager_id)
                logger.info(f"Менеджер @{username} автоматически назначен ответственным за клиента {client_id} (тред {thread_id})")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке медиафайла от менеджера: {e}")
        return False
                
# Вспомогательная функция для обработки медиа от менеджера
async def process_manager_media(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            if not hasattr(message, 'media_group_id') or not message.media_group_id:
                await message.reply_text(
                    "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
                )
            return False
        
        # Извлекаем номер треда из первого слова текста, удаляя префикс "/"
        caption_text = message.caption or ""
        if not caption_text:
            if not hasattr(message, 'media_group_id') or not message.media_group_id:
                await message.reply_text(
                    "Пожалуйста, добавьте подпись к медиафайлу в формате: /{thread_id} [текст подписи]"
                )
            return False
            
        first_word = caption_text.split()[0]
        try:
            thread_id = int(first_word[1:])  # Отрезаем "/" и преобразуем в число
        except ValueError:
            if not hasattr(message, 'media_group_id') or not message.media_group_id:
                await message.reply_text("Неверный формат команды. Используйте: /{thread_id} [текст подписи]")
            return False
            
        logger.info(f"Получен медиафайл с командой /{thread_id} от менеджера {manager_id}")
        
        # Парсим текст подписи для получения сообщения
        caption = None
        if " " in caption_text:
            caption = caption_text.split(" ", 1)[1]
        
        # Проверяем, существует ли клиент с таким thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            if not hasattr(message, 'media_group_id') or not message.media_group_id:
                await message.reply_text(
                    f"Не удалось найти клиента, связанного с темой {thread_id}."
                )
            return False
            
        # Получаем ID клиента
        client_id = client_data[0]  # Первый элемент - user_id
        
        # Определяем тип медиафайла и получаем его file_id
        file_id = None
        media_type = None
        
        if hasattr(message, 'photo') and message.photo:
            file_id = message.photo.file_id
            media_type = "photo"
        elif hasattr(message, 'document') and message.document:
            file_id = message.document.file_id
            media_type = "document"
        elif hasattr(message, 'video') and message.video:
            file_id = message.video.file_id
            media_type = "video"
        elif hasattr(message, 'audio') and message.audio:
            file_id = message.audio.file_id
            media_type = "audio"
        elif hasattr(message, 'voice') and message.voice:
            file_id = message.voice.file_id
            media_type = "voice"
        else:
            # Если тип медиа не определен, пропускаем
            return False
        
        # Отправляем медиафайл клиенту
        success = await send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type)
        
        if success:
            # Обновляем время последнего ответа менеджера
            update_manager_reply_time(db_connection, thread_id)
            
            # Пытаемся убрать индикатор срочности из заголовка треда
            try:
                await mark_thread_urgent(client, thread_id, is_urgent=False)
            except Exception as e:
                logger.error(f"Ошибка при изменении визуального индикатора треда {thread_id}: {e}")
            
            # Не отправляем подтверждение в группу, только записываем в лог
            logger.info(f"Медиафайл от менеджера {manager_id} отправлен клиенту {client_id}, статус треда обновлен")
            
            # Сохраняем информацию о первом ответе менеджера, если это первый ответ
            if is_first_reply(db_connection, thread_id, manager_id):
                save_first_reply(db_connection, thread_id, client_id, manager_id)
                logger.info(f"Сохранена информация о первом ответе менеджера {manager_id} для треда {thread_id}")
            
            return True
        else:
            # Только в случае ошибки уведомляем менеджера и только для первого сообщения в группе
            if not hasattr(message, 'media_group_id') or not message.media_group_id:
                await message.reply_text(
                    "❌ Не удалось отправить медиафайл клиенту."
                )
            return False
        
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла с командой: {e}")
        if not hasattr(message, 'media_group_id') or not message.media_group_id:
            await message.reply_text(f"Произошла ошибка: {e}")
        return False        
        
# Функция для отложенной обработки медиа-группы (c исправленными отступами)
async def process_manager_media_group_after_delay(client, media_group_id, delay_seconds):
    """Обрабатывает медиа-группу от менеджера после задержки для сбора всех файлов"""
    try:
        # Ждем, пока соберутся все файлы группы
        await asyncio.sleep(delay_seconds)
        
        # Проверяем, есть ли группа в словаре
        if media_group_id not in manager_media_groups:
            logger.error(f"Медиа-группа {media_group_id} не найдена для обработки")
            return
            
        group_data = manager_media_groups[media_group_id]
        thread_id = group_data["thread_id"]
        client_id = group_data["client_id"]
        manager_id = group_data["manager_id"]
        caption = group_data["caption"]
        files = group_data["files"]
        
        # Логируем для отладки
        logger.info(f"Начинаю отправку медиа-группы {media_group_id} клиенту {client_id}. Файлов в группе: {len(files)}")
        
        # Получаем данные менеджера
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return
            
        # Безопасная распаковка данных менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = unpack_manager_data(manager)
        
        # Формируем подпись менеджера
        signature = f"\n—\n{emoji} {name}, {position}, доб. {extension}"
        
        # Попробуем сначала отправить как настоящую медиа-группу
        try:
            # Если в группе больше одного файла, пробуем отправить как медиа-группу
            if len(files) > 1:
                # Подготавливаем медиа-группу для отправки клиенту
                media_list = []
                
                # Подготавливаем полную подпись для последнего файла
                full_caption = ""
                if caption:
                    full_caption = f"{caption}{signature}"
                else:
                    full_caption = f"{signature}"
                
                # Добавляем все файлы в группу
                for i, message in enumerate(files):
                    # Подпись только для последнего файла
                    is_last_file = i == len(files) - 1
                    file_caption = full_caption if is_last_file else None
                    
                    if hasattr(message, 'photo') and message.photo:
                        media_list.append(pyrogram.types.InputMediaPhoto(
                            media=message.photo.file_id,
                            caption=file_caption
                        ))
                    elif hasattr(message, 'document') and message.document:
                        media_list.append(pyrogram.types.InputMediaDocument(
                            media=message.document.file_id,
                            caption=file_caption
                        ))
                    elif hasattr(message, 'video') and message.video:
                        media_list.append(pyrogram.types.InputMediaVideo(
                            media=message.video.file_id,
                            caption=file_caption
                        ))
                    elif hasattr(message, 'audio') and message.audio:
                        media_list.append(pyrogram.types.InputMediaAudio(
                            media=message.audio.file_id,
                            caption=file_caption
                        ))
                    elif hasattr(message, 'voice') and message.voice:
                        # Голосовые сообщения нельзя включать в медиа-группу,
                        # но мы добавляем их для совместимости - они будут отправлены отдельно
                        continue
                
                # Если есть файлы для отправки в медиа-группе, отправляем
                if media_list:
                    await client.send_media_group(
                        chat_id=client_id,
                        media=media_list
                    )
                    logger.info(f"Отправлена медиа-группа из {len(media_list)} файлов клиенту {client_id}")
                    
                    # Отправляем отдельно файлы, которые не поддерживаются в медиа-группе
                    for message in files:
                        # Отправляем голосовые сообщения отдельно, так как они не могут быть в медиа-группе
                        if hasattr(message, 'voice') and message.voice:
                            await client.send_voice(
                                chat_id=client_id,
                                voice=message.voice.file_id,
                                caption=signature  # Только подпись менеджера
                            )
                            logger.info(f"Отправлено дополнительное голосовое сообщение клиенту {client_id}")
                    
                    # Все успешно отправлено как медиа-группа
                    success = True
                else:
                    # Если не удалось подготовить медиа-группу, используем запасной вариант
                    logger.warning(f"Не удалось подготовить медиа-группу для клиента {client_id}, использую запасной метод")
                    success = False
            else:
                # Для одного файла не используем медиа-группу
                success = False
                
            # Если не удалось отправить как медиа-группу или если это один файл,
            # используем запасной вариант с отправкой по отдельности
            if not success or len(files) == 1:
                # Если только один файл, отправляем его напрямую
                if len(files) == 1:
                    message = files[0]
                    file_caption = ""
                    if caption:
                        file_caption = caption
                    
                    # Добавляем подпись менеджера
                    file_caption += signature
                    
                    # Определяем тип медиа и отправляем
                    if hasattr(message, 'photo') and message.photo:
                        await client.send_photo(
                            chat_id=client_id,
                            photo=message.photo.file_id,
                            caption=file_caption
                        )
                        logger.info(f"Отправлено фото клиенту {client_id}")
                    elif hasattr(message, 'document') and message.document:
                        await client.send_document(
                            chat_id=client_id,
                            document=message.document.file_id,
                            caption=file_caption
                        )
                        logger.info(f"Отправлен документ клиенту {client_id}")
                    elif hasattr(message, 'video') and message.video:
                        await client.send_video(
                            chat_id=client_id,
                            video=message.video.file_id,
                            caption=file_caption
                        )
                        logger.info(f"Отправлено видео клиенту {client_id}")
                    elif hasattr(message, 'audio') and message.audio:
                        await client.send_audio(
                            chat_id=client_id,
                            audio=message.audio.file_id,
                            caption=file_caption
                        )
                        logger.info(f"Отправлено аудио клиенту {client_id}")
                    elif hasattr(message, 'voice') and message.voice:
                        await client.send_voice(
                            chat_id=client_id,
                            voice=message.voice.file_id,
                            caption=file_caption
                        )
                        logger.info(f"Отправлено голосовое сообщение клиенту {client_id}")
                    
                else:
                    # Для нескольких файлов отправляем каждый по отдельности, 
                    # подпись добавляем к последнему
                    for i, message in enumerate(files):
                        try:
                            # Формируем подпись (только для последнего файла полная)
                            is_last_file = i == len(files) - 1
                            
                            if is_last_file:
                                # Последний файл получает полную подпись с текстом и подписью менеджера
                                file_caption = ""
                                if caption:
                                    file_caption = f"{caption}{signature}"
                                else:
                                    file_caption = signature
                            else:
                                # Остальные файлы без подписи
                                file_caption = None
                            
                            # Определяем тип медиа и отправляем соответствующим методом
                            if hasattr(message, 'photo') and message.photo:
                                await client.send_photo(
                                    chat_id=client_id,
                                    photo=message.photo.file_id,
                                    caption=file_caption
                                )
                            elif hasattr(message, 'document') and message.document:
                                await client.send_document(
                                    chat_id=client_id,
                                    document=message.document.file_id,
                                    caption=file_caption
                                )
                            elif hasattr(message, 'video') and message.video:
                                await client.send_video(
                                    chat_id=client_id,
                                    video=message.video.file_id,
                                    caption=file_caption
                                )
                            elif hasattr(message, 'audio') and message.audio:
                                await client.send_audio(
                                    chat_id=client_id,
                                    audio=message.audio.file_id,
                                    caption=file_caption
                                )
                            elif hasattr(message, 'voice') and message.voice:
                                await client.send_voice(
                                    chat_id=client_id,
                                    voice=message.voice.file_id,
                                    caption=file_caption
                                )
                            
                            logger.info(f"Отправлен файл {i+1} из {len(files)} клиенту {client_id}")
                            
                            # Делаем небольшую паузу между отправками
                            await asyncio.sleep(0.5)
                            
                        except Exception as e:
                            logger.error(f"Ошибка при отправке файла {i+1} из {len(files)}: {e}")
        
        except Exception as e:
            logger.error(f"Ошибка при отправке медиа-группы: {e}")
            # Если произошла ошибка при отправке как медиа-группы, 
            # используем запасной вариант с отдельными сообщениями
            
            # Отправляем каждый файл по отдельности
            success_count = 0
            for i, message in enumerate(files):
                try:
                    # Формируем подпись (только для последнего файла полная)
                    is_last_file = i == len(files) - 1
                    
                    if is_last_file:
                        # Последний файл получает полную подпись
                        file_caption = ""
                        if caption:
                            file_caption = f"{caption}{signature}"
                        else:
                            file_caption = signature
                    else:
                        # Остальные файлы без подписи
                        file_caption = None
                    
                    # Определяем тип медиа и отправляем соответствующим методом
                    if hasattr(message, 'photo') and message.photo:
                        await client.send_photo(
                            chat_id=client_id,
                            photo=message.photo.file_id,
                            caption=file_caption
                        )
                        media_type = "фото"
                    elif hasattr(message, 'document') and message.document:
                        await client.send_document(
                            chat_id=client_id,
                            document=message.document.file_id,
                            caption=file_caption
                        )
                        media_type = "документ"
                    elif hasattr(message, 'video') and message.video:
                        await client.send_video(
                            chat_id=client_id,
                            video=message.video.file_id,
                            caption=file_caption
                        )
                        media_type = "видео"
                    elif hasattr(message, 'audio') and message.audio:
                        await client.send_audio(
                            chat_id=client_id,
                            audio=message.audio.file_id,
                            caption=file_caption
                        )
                        media_type = "аудио"
                    elif hasattr(message, 'voice') and message.voice:
                        await client.send_voice(
                            chat_id=client_id,
                            voice=message.voice.file_id,
                            caption=file_caption
                        )
                        media_type = "голосовое"
                    
                    logger.info(f"Отправлен файл {i+1} из {len(files)} ({media_type}) клиенту {client_id} (после ошибки)")
                    success_count += 1
                    
                    # Делаем небольшую паузу между отправками
                    await asyncio.sleep(0.5)
                    
                except Exception as file_error:
                    logger.error(f"Ошибка при отправке файла {i+1} из {len(files)}: {file_error}")
        
        # Обновляем время последнего ответа менеджера
        update_manager_reply_time(db_connection, thread_id)
        
        # Сбрасываем индикатор срочности
        await mark_thread_urgent(client, thread_id, is_urgent=False)
        
        # Сохраняем информацию о первом ответе менеджера
        if is_first_reply(db_connection, thread_id, manager_id):
            save_first_reply(db_connection, thread_id, client_id, manager_id)
        
        # Автоматически назначаем ответственного менеджера
        if username:
            # Назначаем менеджера ответственным
            assign_duty_manager(db_connection, thread_id, username, manager_id)
            logger.info(f"Менеджер @{username} автоматически назначен ответственным за клиента {client_id} (тред {thread_id})")
        
        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, 
                    f"Группа файлов: {len(files)} шт. {caption or ''}",
                    is_from_user=False, media_type="MEDIA_GROUP")
        
        # Запись в лог об успешной отправке
        logger.info(f"Медиа-группа из {len(files)} файлов успешно отправлена клиенту {client_id}")
        
        # Отвечаем на первое сообщение группы с подтверждением
        init_message_id = group_data.get("initialized_by_message_id")
        if init_message_id:
            for msg in files:
                if msg.id == init_message_id:
                    try:
                        await msg.reply_text(f"✅ Все файлы группы ({len(files)} шт.) успешно отправлены клиенту")
                        break
                    except Exception as e:
                        logger.error(f"Ошибка при отправке подтверждения: {e}")
        
        # Удаляем информацию о группе
        del manager_media_groups[media_group_id]
        
    except Exception as e:
        logger.error(f"Ошибка при обработке медиа-группы {media_group_id}: {e}")
        
        # В случае ошибки пытаемся отправить уведомление
        try:
            if media_group_id in manager_media_groups and manager_media_groups[media_group_id]["files"]:
                init_message_id = manager_media_groups[media_group_id].get("initialized_by_message_id")
                if init_message_id:
                    for msg in manager_media_groups[media_group_id]["files"]:
                        if msg.id == init_message_id:
                            await msg.reply_text(f"❌ Ошибка при отправке группы файлов: {e}")
                            break
        except Exception as reply_error:
            logger.error(f"Не удалось отправить сообщение об ошибке: {reply_error}")
        
        # Удаляем информацию о группе в любом случае
        if media_group_id in manager_media_groups:
            del manager_media_groups[media_group_id]


# Функция для очистки устаревших ожидающих групп
async def cleanup_pending_group_after_delay(media_group_id, delay_seconds):
    """Удаляет ожидающую группу после задержки, если она не была инициализирована"""
    await asyncio.sleep(delay_seconds)
    if media_group_id in pending_media_groups:
        # Проверяем, инициализирована ли группа
        if media_group_id not in manager_media_groups:
            # Если группа не инициализирована, удаляем из ожидания
            # Но перед удалением логируем содержимое
            files_count = len(pending_media_groups[media_group_id]["files"])
            logger.warning(f"Удаляю ожидающую медиа-группу {media_group_id} после таймаута. Файлов: {files_count}")
            
            # Удаляем группу
            del pending_media_groups[media_group_id]

# Функция для периодической очистки старых медиа-групп
# Обновленная функция для периодической очистки медиа-групп
async def cleanup_media_groups():
    """Периодически очищает устаревшие записи о медиа-группах и ожидающих группах"""
    while True:
        try:
            await asyncio.sleep(60)  # Проверяем каждую минуту
            
            current_time = datetime.datetime.now()
            groups_to_remove = []
            pending_groups_to_remove = []
            
            # Проверяем инициализированные группы
            for group_id, group_data in manager_media_groups.items():
                time_diff = (current_time - group_data["timestamp"]).total_seconds()
                if time_diff > 300:  # 5 минут
                    groups_to_remove.append(group_id)
            
            # Проверяем ожидающие группы
            for group_id, group_data in pending_media_groups.items():
                time_diff = (current_time - group_data["timestamp"]).total_seconds()
                if time_diff > 60:  # 1 минута
                    pending_groups_to_remove.append(group_id)
            
            # Удаляем устаревшие группы
            for group_id in groups_to_remove:
                if group_id in manager_media_groups:
                    files_count = len(manager_media_groups[group_id]["files"]) 
                    del manager_media_groups[group_id]
                    logger.info(f"Удалена устаревшая медиа-группа {group_id} с {files_count} файлами")
            
            for group_id in pending_groups_to_remove:
                if group_id in pending_media_groups:
                    files_count = len(pending_media_groups[group_id]["files"])
                    del pending_media_groups[group_id]
                    logger.info(f"Удалена устаревшая ожидающая группа {group_id} с {files_count} файлами")
            
            # Проверяем группы от клиентов, если такие есть
            if hasattr(handle_client_media_group, "client_media_groups"):
                client_groups_to_remove = []
                
                for group_key, group_data in handle_client_media_group.client_media_groups.items():
                    time_diff = (current_time - group_data["timestamp"]).total_seconds()
                    if time_diff > 300:  # 5 минут
                        client_groups_to_remove.append(group_key)
                
                for group_key in client_groups_to_remove:
                    if group_key in handle_client_media_group.client_media_groups:
                        files_count = len(handle_client_media_group.client_media_groups[group_key]["messages"])
                        del handle_client_media_group.client_media_groups[group_key]
                        logger.info(f"Удалена устаревшая группа клиента {group_key} с {files_count} сообщениями")
                        
        except Exception as e:
            logger.error(f"Ошибка в процессе очистки групп: {e}")
            await asyncio.sleep(30)  # При ошибке делаем короткую паузу

# Функция для назначения ответственного менеджера на тред
def assign_duty_manager(conn, thread_id, manager_username, assigned_by):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    cursor.execute('''
    INSERT OR REPLACE INTO duty_managers (thread_id, manager_username, assigned_by, assigned_at)
    VALUES (?, ?, ?, ?)
    ''', (thread_id, manager_username, assigned_by, current_time))
    
    conn.commit()
    return True

# Функция для получения ответственного менеджера для треда
def get_duty_manager(conn, thread_id):
    cursor = conn.cursor()
    cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    return None

# Функция для обновления времени последнего сообщения клиента
def update_client_message_time(conn, thread_id):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    # Проверяем, существует ли запись для этого треда
    cursor.execute('SELECT thread_id FROM thread_status WHERE thread_id = ?', (thread_id,))
    exists = cursor.fetchone()
    
    if exists:
        # Обновляем время последнего сообщения клиента
        cursor.execute('''
        UPDATE thread_status 
        SET last_client_message = ?, is_notified = 0
        WHERE thread_id = ?
        ''', (current_time, thread_id))
    else:
        # Создаем новую запись
        cursor.execute('''
        INSERT INTO thread_status (thread_id, last_client_message, is_notified)
        VALUES (?, ?, 0)
        ''', (thread_id, current_time))
    
    conn.commit()

# Функция для обновления времени последнего ответа менеджера
def update_manager_reply_time(conn, thread_id):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    cursor.execute('''
    UPDATE thread_status 
    SET last_manager_reply = ?, is_notified = 0, notification_disabled = 0
    WHERE thread_id = ?
    ''', (current_time, thread_id))
    conn.commit()

# Обновленная функция для сброса текущего уведомления (не отключает навсегда)
def reset_thread_notification(conn, thread_id):
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    
    # Обновляем время последнего ответа менеджера и сбрасываем статус уведомления
    cursor.execute('''
    UPDATE thread_status 
    SET is_notified = 0, 
        last_notification = NULL, 
        last_manager_reply = ?
    WHERE thread_id = ?
    ''', (current_time, thread_id))
    conn.commit()
    
# Упрощенная функция для изменения заголовка треда
async def update_thread_title(client, thread_id, title):
    try:
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        
        await client.invoke(
            pyrogram.raw.functions.channels.EditForumTopic(
                channel=peer,
                topic_id=thread_id,
                title=title
            )
        )
        logger.info(f"Заголовок треда {thread_id} изменен на '{title}'")
        return True
    except Exception as e:
        logger.error(f"Ошибка при изменении заголовка треда {thread_id}: {e}")
        return False

# Функция для получения информации о клиенте по ID треда
def get_client_info_by_thread(conn, thread_id):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT first_name, last_name, username
    FROM clients 
    WHERE thread_id = ?
    ''', (thread_id,))
    
    return cursor.fetchone()
    
# Декоратор для автоматической обработки FloodWait
def handle_flood_wait(max_retries=3, initial_delay=1):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            
            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except pyrogram.errors.FloodWait as e:
                    wait_time = e.x
                    logger.warning(f"FloodWait: ожидание {wait_time} секунд (попытка {retries+1}/{max_retries+1})")
                    
                    if retries == max_retries:
                        raise
                    
                    await asyncio.sleep(wait_time)
                    retries += 1
                    delay *= 2  # Экспоненциальная задержка
        
        return wrapper
    return decorator

# Упрощенная функция для хранения текущего состояния тредов
thread_title_states = {}  # thread_id -> {"has_alert": bool, "title": str}

# Функция для изменения индикатора тредов
async def mark_thread_urgent(client, thread_id, is_urgent=True):
    try:
        # Проверяем, известно ли нам текущее состояние треда
        current_state = thread_title_states.get(thread_id, {"has_alert": False, "title": None})
        
        # Если уже в нужном состоянии, ничего не делаем
        if (is_urgent and current_state["has_alert"]) or (not is_urgent and not current_state["has_alert"]):
            logger.info(f"Тред {thread_id} уже в нужном состоянии (alert={is_urgent})")
            return True
            
        # Получаем информацию о клиенте для заголовка
        client_info = get_client_info_by_thread(db_connection, thread_id)
        
        if not client_info:
            logger.error(f"Не удалось найти клиента для треда {thread_id}")
            return False
            
        # Формируем имя клиента
        first_name, last_name, username = client_info
        client_name = f"{first_name or ''}"
        if last_name:
            client_name += f" {last_name}"
        if username:
            client_name += f" (@{username})"
            
        # Формируем базовый и полный заголовок с индикатором
        base_title = f"{thread_id}: {client_name}"
        alert_title = f"🔥{base_title}"
        
        # Выбираем заголовок в зависимости от нужного состояния
        new_title = alert_title if is_urgent else base_title
            
        # Обновляем заголовок
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        await client.invoke(
            pyrogram.raw.functions.channels.EditForumTopic(
                channel=peer,
                topic_id=thread_id,
                title=new_title
            )
        )
        
        # Запоминаем новое состояние
        thread_title_states[thread_id] = {"has_alert": is_urgent, "title": new_title}
        
        logger.info(f"Заголовок треда {thread_id} изменен на '{new_title}'")
        return True
            
    except Exception as e:
        logger.error(f"Ошибка при обновлении статуса треда {thread_id}: {e}")
        return False

# Функция для проверки неотвеченных сообщений и отправки уведомлений
async def check_unanswered_messages(client):
    try:
        logger.info("Проверка неотвеченных сообщений...")
        cursor = db_connection.cursor()
        current_time = datetime.datetime.now()
        
        logger.info(f"Параметры: URGENT_WAIT_TIME={URGENT_WAIT_TIME}, FIRST_NOTIFICATION_DELAY={FIRST_NOTIFICATION_DELAY}, NOTIFICATION_INTERVAL={NOTIFICATION_INTERVAL}")
        
        # Получаем список тредов с неотвеченными сообщениями
        cursor.execute('''
        SELECT 
            ts.thread_id,
            ts.last_client_message,
            ts.last_manager_reply,
            ts.is_notified,
            ts.last_notification,
            ts.notification_disabled,
            c.user_id,
            c.first_name,
            c.last_name,
            c.username,
            dm.manager_username,
            c.custom_id  -- Добавляем custom_id клиента
        FROM thread_status ts
        JOIN clients c ON ts.thread_id = c.thread_id
        LEFT JOIN duty_managers dm ON ts.thread_id = dm.thread_id
        WHERE 
            (ts.last_manager_reply IS NULL OR ts.last_client_message > ts.last_manager_reply)
            AND ts.notification_disabled = 0
        ''')
        
        unanswered_threads = cursor.fetchall()
        logger.info(f"Найдено {len(unanswered_threads)} тредов с неотвеченными сообщениями")
        
        # Получаем список всех активных менеджеров для массовых уведомлений
        cursor.execute('''
        SELECT manager_id, username FROM managers
        WHERE username IS NOT NULL
        ORDER BY auth_date DESC
        ''')
        active_managers = cursor.fetchall()
        
        for thread in unanswered_threads:
            thread_id, last_client_msg, last_manager_reply, is_notified, last_notification, disabled, \
            user_id, first_name, last_name, username, manager_username, custom_id = thread
            
            logger.info(f"Обработка треда {thread_id}: последнее сообщение клиента: {last_client_msg}, last_manager_reply: {last_manager_reply}")
            logger.info(f"Тред {thread_id}: is_notified={is_notified}, last_notification={last_notification}, disabled={disabled}")
            
            # Преобразуем строки дат в объекты datetime
            if isinstance(last_client_msg, str):
                last_client_msg = datetime.datetime.strptime(last_client_msg, '%Y-%m-%d %H:%M:%S.%f')
                logger.info(f"Тред {thread_id}: преобразовано время последнего сообщения клиента: {last_client_msg}")
            
            # Вычисляем, сколько времени прошло с последнего сообщения клиента
            time_since_message = current_time - last_client_msg
            minutes_passed = time_since_message.total_seconds() / 60
            
            logger.info(f"Тред {thread_id}: прошло {minutes_passed:.2f} минут с последнего сообщения клиента")
            
            # Проверяем, прошло ли достаточно времени для отметки сообщения как срочного
            if minutes_passed >= URGENT_WAIT_TIME:
                logger.info(f"Тред {thread_id}: прошло достаточно времени для оповещения ({minutes_passed:.2f} мин. >= {URGENT_WAIT_TIME} мин.)")
                
                # Меняем иконку треда на красную
                try:
                    await mark_thread_urgent(client, thread_id, is_urgent=True)
                except Exception as e:
                    logger.error(f"Ошибка при маркировке треда {thread_id} как срочного: {e}")
                
                # Определяем, нужно ли отправлять уведомление
                send_notification = False
                
                # Если уведомление еще не отправлялось
                if not is_notified:
                    send_notification = True
                    logger.info(f"Тред {thread_id}: первое уведомление, так как is_notified={is_notified}")
                
                # Или если прошло достаточно времени после последнего уведомления
                elif last_notification:
                    if isinstance(last_notification, str):
                        last_notification = datetime.datetime.strptime(last_notification, '%Y-%m-%d %H:%M:%S.%f')
                        logger.info(f"Тред {thread_id}: преобразовано время последнего уведомления: {last_notification}")
                    
                    time_since_notification = current_time - last_notification
                    minutes_since_notification = time_since_notification.total_seconds() / 60
                    
                    logger.info(f"Тред {thread_id}: прошло {minutes_since_notification:.2f} минут с последнего уведомления")
                    
                    if minutes_since_notification >= NOTIFICATION_INTERVAL:
                        send_notification = True
                        logger.info(f"Тред {thread_id}: повторное уведомление, так как прошло {minutes_since_notification:.2f} мин. >= {NOTIFICATION_INTERVAL} мин.")
                    else:
                        logger.info(f"Тред {thread_id}: отложено повторное уведомление, так как прошло {minutes_since_notification:.2f} мин. < {NOTIFICATION_INTERVAL} мин.")
                
                # Отправляем уведомление, если необходимо
                if send_notification:
                    logger.info(f"Тред {thread_id}: подготовка уведомления")
                    
                    # Формируем текст уведомления
                    client_name = f"{first_name or ''}"
                    if last_name:
                        client_name += f" {last_name}"
                    if username:
                        client_name += f" (@{username})"
                    
                    hours_waiting = int(minutes_passed / 60)
                    remaining_minutes = int(minutes_passed % 60)
                    waiting_time = f"{hours_waiting} ч {remaining_minutes} мин" if hours_waiting > 0 else f"{remaining_minutes} мин"
                    
                    # Добавляем информацию о custom_id, если он есть
                    client_id_info = ""
                    if custom_id:
                        client_id_info = f"🆔 ID клиента: **{custom_id}**\n"
                        reply_command = f"#{custom_id}"
                    else:
                        reply_command = f"{thread_id}"
                    
                    notification_text = f"⚠️ **ВНИМАНИЕ!** ⚠️\n\n"
                    notification_text += f"🔴 Неотвеченное сообщение в треде #{thread_id}!\n"
                    notification_text += f"👤 Клиент: {client_name}\n"
                    notification_text += client_id_info
                    notification_text += f"⏱ Ожидание: {waiting_time}\n\n"
                    
                    if manager_username:
                        notification_text += f"📌 Ответственный: @{manager_username}\n\n"
                    else:
                        notification_text += f"📌 Ответственный менеджер не назначен\n\n"
                    
                    notification_text += f"🔗 [Перейти к треду](https://t.me/c/{str(SUPPORT_GROUP_ID)[4:]}/{thread_id})\n"
                    notification_text += f"✏️ Чтобы ответить, используйте: `/{reply_command} текст ответа`\n\n"
                    notification_text += f"📵 Чтобы отключить текущее уведомление для этого треда, используйте команду /ok {thread_id}"
                    
                    try:
                        logger.info(f"Тред {thread_id}: отправка уведомления в группу")
                        # Отправляем уведомление в группу
                        await client.send_message(
                            chat_id=SUPPORT_GROUP_ID,
                            text=notification_text,
                            disable_web_page_preview=True,
                            parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                        )
                        logger.info(f"Тред {thread_id}: уведомление отправлено в группу")
                    except Exception as e:
                        logger.error(f"Ошибка при отправке уведомления в группу для треда {thread_id}: {e}")
                    
                    # Если есть ответственный менеджер, отправляем ему личное сообщение
                    if manager_username:
                        logger.info(f"Тред {thread_id}: подготовка личного уведомления менеджеру {manager_username}")
                        # Сначала нужно получить ID менеджера по его username
                        cursor.execute('SELECT manager_id FROM managers WHERE username = ?', (manager_username,))
                        manager_data = cursor.fetchone()
                        
                        if manager_data:
                            manager_id = manager_data[0]
                            # Отправляем личное сообщение менеджеру
                            try:
                                logger.info(f"Тред {thread_id}: отправка личного уведомления менеджеру {manager_username} (ID: {manager_id})")
                                await client.send_message(
                                    chat_id=manager_id,
                                    text=notification_text,
                                    disable_web_page_preview=True,
                                    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                                )
                                logger.info(f"Личное уведомление отправлено менеджеру {manager_username} (ID: {manager_id})")
                            except Exception as e:
                                logger.error(f"Ошибка при отправке личного уведомления менеджеру {manager_username}: {e}")
                    else:
                        # Если ответственный не назначен, отправляем уведомления всем активным менеджерам
                        for manager_id, manager_name in active_managers:
                            try:
                                # Избегаем отправки множественных уведомлений одному и тому же менеджеру
                                if is_first_reply(db_connection, thread_id, manager_id):
                                    logger.info(f"Тред {thread_id}: отправка уведомления менеджеру {manager_name} (ID: {manager_id})")
                                    await client.send_message(
                                        chat_id=manager_id,
                                        text=f"⚠️ Требуется ответ!\n\n" + notification_text,
                                        disable_web_page_preview=True,
                                        parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                                    )
                            except Exception as e:
                                logger.error(f"Ошибка при отправке уведомления менеджеру {manager_name}: {e}")
                    
                    # Обновляем статус уведомления в базе данных
                    cursor.execute('''
                    UPDATE thread_status 
                    SET is_notified = 1, last_notification = ?
                    WHERE thread_id = ?
                    ''', (current_time, thread_id))
                    db_connection.commit()
                    
                    logger.info(f"Отправлено уведомление о неотвеченном сообщении в треде {thread_id}")
                    
                else:
                    logger.info(f"Тред {thread_id}: уведомление не требуется")
            else:
                logger.info(f"Тред {thread_id}: прошло недостаточно времени для оповещения ({minutes_passed:.2f} мин. < {URGENT_WAIT_TIME} мин.)")
        
    except Exception as e:
        logger.error(f"Ошибка при проверке неотвеченных сообщений: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
# Функция для отправки списка участников группы
async def send_member_list(client, chat_id, thread_id):
    try:
        logger.info(f"Запрошен список участников для группы {chat_id}")
        
        # Получаем список участников
        members = []
        
        # Используем более надежный метод
        async for member in client.get_chat_members(chat_id):
            user = member.user
            if user.is_deleted:
                member_info = "Удаленный аккаунт"
            else:
                member_info = f"{user.first_name or ''}"
                if user.last_name:
                    member_info += f" {user.last_name}"
                if user.username:
                    member_info += f" (@{user.username})"
            
            # Определяем статус
            if member.status == "creator":
                member_info += " 👑 владелец"
            elif member.status == "administrator":
                member_info += " 🛡️ администратор"
            
            members.append(member_info)
        
        logger.info(f"Получено {len(members)} участников группы {chat_id}")
        
        # Если список пуст, отправляем уведомление
        if not members:
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text="⚠️ Не удалось получить список участников группы. Возможно, у бота недостаточно прав.",
                reply_to_message_id=thread_id
            )
            return
        
        # Если слишком много участников, отправляем частями
        if len(members) > 50:
            chunks = [members[i:i+50] for i in range(0, len(members), 50)]
            
            for i, chunk in enumerate(chunks):
                member_text = f"👥 **Участники группы (часть {i+1}/{len(chunks)}):**\n\n"
                member_text += "\n".join([f"• {m}" for m in chunk])
                
                await client.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=member_text,
                    reply_to_message_id=thread_id
                )
        else:
            member_text = f"👥 **Участники группы ({len(members)}):**\n\n"
            member_text += "\n".join([f"• {m}" for m in members])
            
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=member_text,
                reply_to_message_id=thread_id
            )
            
        logger.info(f"Список участников для группы {chat_id} отправлен в тред {thread_id}")
    except Exception as e:
        logger.error(f"Ошибка при отправке списка участников: {e}")
        # Пробуем отправить сообщение об ошибке
        try:
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=f"⚠️ Ошибка при получении списка участников: {e}",
                reply_to_message_id=thread_id
            )
        except:
            pass

# Функция загрузки файла в хранилище
async def upload_file_to_storage(client, message, file_name):
    try:
        # Проверяем, есть ли файл с таким названием
        cursor = db_connection.cursor()
        cursor.execute('SELECT * FROM storage_files WHERE file_name = ?', (file_name,))
        existing_file = cursor.fetchone()
        
        if existing_file:
            # Файл уже существует
            await message.reply_text(f"⚠️ Файл '{file_name}' уже существует в хранилище.\n\nЧтобы заменить его, используйте команду:\n/replace {file_name}")
            return False, "EXISTS"
        
        # Определяем тип файла
        file_type = None
        file_id = None
        
        if message.document:
            file_type = "document"
            file_id = message.document.file_id
        elif message.photo:
            file_type = "photo"
            file_id = message.photo.file_id
        elif message.video:
            file_type = "video"
            file_id = message.video.file_id
        elif message.audio:
            file_type = "audio"
            file_id = message.audio.file_id
        elif message.voice:
            file_type = "voice"
            file_id = message.voice.file_id
        else:
            await message.reply_text("❌ Неподдерживаемый тип файла. Пожалуйста, отправьте документ, фото, видео, аудио или голосовое сообщение.")
            return False, "UNSUPPORTED"
        
        logger.info(f"Попытка загрузки файла '{file_name}' типа {file_type} в канал {STORAGE_CHANNEL_ID}")
        
        # Отправляем файл в канал хранилища
        storage_message = None
        try:
            if file_type == "document":
                storage_message = await client.send_document(
                    chat_id=STORAGE_CHANNEL_ID,
                    document=file_id,
                    caption=f"FILE:{file_name}:CURRENT"
                )
            elif file_type == "photo":
                storage_message = await client.send_photo(
                    chat_id=STORAGE_CHANNEL_ID,
                    photo=file_id,
                    caption=f"FILE:{file_name}:CURRENT"
                )
            elif file_type == "video":
                storage_message = await client.send_video(
                    chat_id=STORAGE_CHANNEL_ID,
                    video=file_id,
                    caption=f"FILE:{file_name}:CURRENT"
                )
            elif file_type == "audio":
                storage_message = await client.send_audio(
                    chat_id=STORAGE_CHANNEL_ID,
                    audio=file_id,
                    caption=f"FILE:{file_name}:CURRENT"
                )
            elif file_type == "voice":
                storage_message = await client.send_voice(
                    chat_id=STORAGE_CHANNEL_ID,
                    voice=file_id,
                    caption=f"FILE:{file_name}:CURRENT"
                )
            
            logger.info(f"Файл отправлен в канал, получен message_id: {storage_message.id if storage_message else 'None'}")
            
        except Exception as send_error:
            logger.error(f"Ошибка при отправке файла в канал: {send_error}")
            await message.reply_text(f"❌ Ошибка при отправке файла в канал хранилища: {send_error}")
            return False, "SEND_ERROR"
        
        if not storage_message:
            await message.reply_text("❌ Не удалось отправить файл в канал хранилища.")
            return False, "NO_MESSAGE"
        
        # Сохраняем информацию о файле в базе данных
        current_time = datetime.datetime.now()
        try:
            cursor.execute('''
            INSERT INTO storage_files (file_name, file_id, message_id, file_type, upload_date, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (file_name, file_id, storage_message.id, file_type, current_time, message.from_user.id))
            db_connection.commit()
            logger.info(f"Файл '{file_name}' сохранен в БД с message_id: {storage_message.id}")
        except Exception as db_error:
            logger.error(f"Ошибка при сохранении файла в БД: {db_error}")
            await message.reply_text(f"❌ Файл отправлен в канал, но ошибка сохранения в БД: {db_error}")
            return False, "DB_ERROR"
        
        await message.reply_text(f"✅ Файл '{file_name}' успешно загружен в хранилище.\n📁 Message ID: {storage_message.id}")
        return True, storage_message.id
        
    except Exception as e:
        logger.error(f"Критическая ошибка при загрузке файла в хранилище: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
        await message.reply_text(f"❌ Произошла критическая ошибка: {e}")
        return False, "CRITICAL_ERROR"
        
@business.on_message(filters.command("debug_storage") & filters.chat(SUPPORT_GROUP_ID))
async def debug_storage_access(client, message):
    try:
        await message.reply_text("🔍 Начинаю диагностику канала хранилища...")
        
        # Проверяем переменную окружения
        await message.reply_text(f"📋 STORAGE_CHANNEL_ID из config: {STORAGE_CHANNEL_ID}")
        
        # Проверяем доступ к каналу
        try:
            storage_chat = await client.get_chat(STORAGE_CHANNEL_ID)
            await message.reply_text(
                f"✅ Канал найден:\n"
                f"**Название**: {storage_chat.title}\n"
                f"**ID**: {storage_chat.id}\n"
                f"**Тип**: {storage_chat.type}\n"
                f"**Username**: @{storage_chat.username if storage_chat.username else 'не установлен'}"
            )
        except Exception as e:
            await message.reply_text(f"❌ Ошибка доступа к каналу: {e}")
            return
        
        # Проверяем права бота в канале
        try:
            me = await client.get_me()
            my_member = await client.get_chat_member(STORAGE_CHANNEL_ID, me.id)
            await message.reply_text(
                f"🤖 Статус бота в канале: {my_member.status}\n"
                f"**Права**: {my_member.privileges if hasattr(my_member, 'privileges') else 'стандартные'}"
            )
        except Exception as e:
            await message.reply_text(f"❌ Ошибка проверки прав: {e}")
            return
        
        # Пробуем отправить тестовое сообщение
        try:
            test_message = await client.send_message(
                chat_id=STORAGE_CHANNEL_ID,
                text=f"🧪 Тест отправки сообщения: {datetime.datetime.now().strftime('%H:%M:%S')}"
            )
            await message.reply_text(f"✅ Текстовое сообщение отправлено успешно! ID: {test_message.id}")
        except Exception as e:
            await message.reply_text(f"❌ Ошибка отправки текста: {e}")
            return
        
        # Пробуем отправить тестовое фото
        try:
            # Используем стандартное тестовое изображение
            test_photo = await client.send_photo(
                chat_id=STORAGE_CHANNEL_ID,
                photo="https://via.placeholder.com/150/0000FF/FFFFFF?text=TEST",
                caption=f"🧪 Тест отправки фото: {datetime.datetime.now().strftime('%H:%M:%S')}"
            )
            await message.reply_text(f"✅ Фото отправлено успешно! ID: {test_photo.id}")
        except Exception as e:
            await message.reply_text(f"❌ Ошибка отправки фото: {e}")
            logger.error(f"Детальная ошибка отправки фото: {e}")
            return
        
        await message.reply_text("🎉 Диагностика завершена!")
        
    except Exception as e:
        logger.error(f"Ошибка в диагностике: {e}")
        await message.reply_text(f"❌ Критическая ошибка диагностики: {e}")
        
        

# Функция замены существующего файла
async def replace_file_in_storage(client, message, file_name):
    try:
        # Проверяем, есть ли файл с таким названием
        cursor = db_connection.cursor()
        cursor.execute('SELECT * FROM storage_files WHERE file_name = ?', (file_name,))
        existing_file = cursor.fetchone()
        
        if not existing_file:
            await message.reply_text(f"❌ Файл '{file_name}' не найден в хранилище. Используйте команду /upload для создания нового файла.")
            return False, "NOT_FOUND"
        
        file_id = existing_file[2]
        message_id = existing_file[3]
        file_type = existing_file[4]
        
        # Определяем тип нового файла
        new_file_type = None
        new_file_id = None
        
        if message.document:
            new_file_type = "document"
            new_file_id = message.document.file_id
        elif message.photo:
            new_file_type = "photo"
            new_file_id = message.photo.file_id
        elif message.video:
            new_file_type = "video"
            new_file_id = message.video.file_id
        elif message.audio:
            new_file_type = "audio"
            new_file_id = message.audio.file_id
        elif message.voice:
            new_file_type = "voice"
            new_file_id = message.voice.file_id
        else:
            await message.reply_text("❌ Неподдерживаемый тип файла. Пожалуйста, отправьте документ, фото, видео, аудио или голосовое сообщение.")
            return False, "UNSUPPORTED"
        
        # Сохраняем текущую версию в историю
        current_time = datetime.datetime.now()
        cursor.execute('''
        INSERT INTO file_versions (file_name, file_id, message_id, version_date, created_by)
        VALUES (?, ?, ?, ?, ?)
        ''', (file_name, file_id, message_id, current_time, message.from_user.id))
        
        # Отправляем новый файл в канал хранилища
        storage_message = None
        if new_file_type == "document":
            storage_message = await client.send_document(
                chat_id=STORAGE_CHANNEL_ID,
                document=new_file_id,
                caption=f"FILE:{file_name}:CURRENT"
            )
        elif new_file_type == "photo":
            storage_message = await client.send_photo(
                chat_id=STORAGE_CHANNEL_ID,
                photo=new_file_id,
                caption=f"FILE:{file_name}:CURRENT"
            )
        elif new_file_type == "video":
            storage_message = await client.send_video(
                chat_id=STORAGE_CHANNEL_ID,
                video=new_file_id,
                caption=f"FILE:{file_name}:CURRENT"
            )
        elif new_file_type == "audio":
            storage_message = await client.send_audio(
                chat_id=STORAGE_CHANNEL_ID,
                audio=new_file_id,
                caption=f"FILE:{file_name}:CURRENT"
            )
        elif new_file_type == "voice":
            storage_message = await client.send_voice(
                chat_id=STORAGE_CHANNEL_ID,
                voice=new_file_id,
                caption=f"FILE:{file_name}:CURRENT"
            )
        
        if not storage_message:
            await message.reply_text("❌ Ошибка при отправке файла в хранилище.")
            return False, "ERROR"
        
        # Обновляем информацию о файле в базе данных
        cursor.execute('''
        UPDATE storage_files 
        SET file_id = ?, message_id = ?, file_type = ?, upload_date = ?, uploaded_by = ?
        WHERE file_name = ?
        ''', (new_file_id, storage_message.id, new_file_type, current_time, message.from_user.id, file_name))
        db_connection.commit()
        
        await message.reply_text(f"✅ Файл '{file_name}' успешно обновлен. Старая версия сохранена в истории.")
        return True, storage_message.id
        
    except Exception as e:
        logger.error(f"Ошибка при замене файла в хранилище: {e}")
        await message.reply_text(f"❌ Произошла ошибка при замене файла: {e}")
        return False, "ERROR"

# Функция получения списка файлов
async def get_files_list(client, message):
    try:
        cursor = db_connection.cursor()
        cursor.execute('''
        SELECT file_name, file_id, file_type, upload_date 
        FROM storage_files
        ORDER BY file_name
        ''')
        
        files = cursor.fetchall()
        
        if not files:
            await message.reply_text("📂 Хранилище файлов пусто.")
            return
        
        # Формируем список файлов
        files_list = "📋 **Список файлов в хранилище:**\n\n"
        
        for i, file in enumerate(files, 1):
            file_name, file_id, file_type, upload_date = file
            
            # Преобразуем дату, если это строка
            if isinstance(upload_date, str):
                try:
                    upload_date = datetime.datetime.strptime(upload_date, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    upload_date = datetime.datetime.now()
            
            # Форматируем дату
            date_str = upload_date.strftime('%d.%m.%Y %H:%M')
            
            # Определяем иконку типа файла
            type_icon = "📄"
            if file_type == "photo":
                type_icon = "🖼️"
            elif file_type == "video":
                type_icon = "🎬"
            elif file_type == "audio":
                type_icon = "🎵"
            elif file_type == "voice":
                type_icon = "🎤"
            
            files_list += f"{i}. {type_icon} **{file_name}**\n"
            files_list += f"   *Дата загрузки:* {date_str}\n"
            files_list += f"   *ID файла:* `{file_id[:20]}...`\n\n"
        
        # Добавляем инструкции
        files_list += "Чтобы отправить файл клиенту, используйте команду:\n"
        files_list += "/send [thread_id] [file_name]  или  /#[client_id] [file_name]"
        
        await message.reply_text(files_list, parse_mode=pyrogram.enums.ParseMode.MARKDOWN)
    
    except Exception as e:
        logger.error(f"Ошибка при получении списка файлов: {e}")
        await message.reply_text(f"❌ Произошла ошибка при получении списка файлов: {e}")

# Функция отправки файла клиенту
async def send_file_to_client(client, thread_id_or_client_id, file_name, message, is_thread_id=True):
    try:
        cursor = db_connection.cursor()
        
        # Получаем информацию о файле
        cursor.execute('SELECT file_id, message_id, file_type FROM storage_files WHERE file_name = ?', (file_name,))
        file_info = cursor.fetchone()
        
        if not file_info:
            await message.reply_text(f"❌ Файл '{file_name}' не найден в хранилище.")
            return False
        
        file_id, message_id, file_type = file_info
        
        # Получаем ID клиента
        client_id = None
        
        if is_thread_id:
            # Получаем client_id по thread_id
            cursor.execute('SELECT user_id FROM clients WHERE thread_id = ?', (thread_id_or_client_id,))
            client_data = cursor.fetchone()
            
            if not client_data:
                await message.reply_text(f"❌ Не найден клиент с thread_id {thread_id_or_client_id}")
                return False
            
            client_id = client_data[0]
        else:
            # Используем client_id напрямую
            cursor.execute('SELECT user_id FROM clients WHERE custom_id = ?', (thread_id_or_client_id,))
            client_data = cursor.fetchone()
            
            if not client_data:
                await message.reply_text(f"❌ Не найден клиент с ID {thread_id_or_client_id}")
                return False
            
            client_id = client_data[0]
        
        # Пробуем получить сообщение из канала хранилища
        try:
            storage_message = await client.get_messages(STORAGE_CHANNEL_ID, message_id)
            
            if not storage_message:
                await message.reply_text(f"❌ Ошибка при получении файла из хранилища.")
                return False
            
            # Копируем сообщение клиенту
            await client.copy_message(
                chat_id=client_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=message_id
            )
            
            # Получаем имя клиента для лога
            cursor.execute('SELECT first_name, last_name FROM clients WHERE user_id = ?', (client_id,))
            client_name_data = cursor.fetchone()
            
            client_name = "неизвестный клиент"
            if client_name_data:
                first_name, last_name = client_name_data
                client_name = f"{first_name or ''}"
                if last_name:
                    client_name += f" {last_name}"
            
            await message.reply_text(f"✅ Файл '{file_name}' успешно отправлен клиенту {client_name} (ID: {client_id}).")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при отправке файла клиенту: {e}")
            await message.reply_text(f"❌ Ошибка при отправке файла: {e}")
            return False
    
    except Exception as e:
        logger.error(f"Ошибка при отправке файла клиенту: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")
        return False

# Общий обработчик для документов, видео, аудио и голосовых сообщений
@business.on_message((filters.document | filters.video | filters.audio | filters.voice) & filters.chat(SUPPORT_GROUP_ID))
async def handle_media_with_caption_command(client, message):
    try:
        caption = message.caption or ""
        
        # Обрабатываем команды загрузки и замены файлов
        if caption.startswith("/upload "):
            file_name = caption.replace("/upload ", "").strip()
            await upload_file_to_storage(client, message, file_name)
        elif caption.startswith("/replace "):
            file_name = caption.replace("/replace ", "").strip()
            await replace_file_in_storage(client, message, file_name)
        # Если это не команда для работы с файлами, продолжаем обычную обработку
        # Например, перенаправляем в другие обработчики для ответа клиентам
    except Exception as e:
        logger.error(f"Ошибка при обработке медиа с командой: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")

# Обработчик события добавления бота в новую группу
@business.on_chat_member_updated()
async def handle_chat_member_update(client, update):
    try:
        # Получаем информацию о нашем боте
        me = await client.get_me()
        
        # Проверяем, что обновление касается нашего бота
        if update.new_chat_member and update.new_chat_member.user.id == me.id:
            # Бот был добавлен в чат
            chat = update.chat
            
            # Проверяем, что это обычная группа, а не группа поддержки
            if chat.id == SUPPORT_GROUP_ID:
                return
                
            logger.info(f"Бизнес-аккаунт добавлен в группу: {chat.title} (ID: {chat.id})")
            
            # Приобретаем блокировку для этой группы
            lock = await acquire_group_lock(chat.id)
            try:
                # Проверяем, есть ли уже тред для этой группы
                cursor = db_connection.cursor()
                cursor.execute('SELECT thread_id FROM group_threads WHERE group_id = ?', (chat.id,))
                existing_thread = cursor.fetchone()
                
                if existing_thread:
                    thread_id = existing_thread[0]
                    logger.info(f"Найден существующий тред {thread_id} для группы {chat.title}")
                    
                    # Отправляем сообщение об обновлении в тред
                    await client.send_message(
                        chat_id=SUPPORT_GROUP_ID,
                        text=f"ℹ️ Бизнес-аккаунт повторно добавлен в группу **{chat.title}**",
                        reply_to_message_id=thread_id
                    )
                else:
                    # Создаем новый тред для этой группы
                    thread_title = f"Группа: {chat.title}"
                    thread_id = await create_support_thread(client, thread_title)
                    
                    if thread_id:
                        # Сохраняем информацию о треде
                        try:
                            cursor.execute('''
                            INSERT INTO group_threads (group_id, group_title, thread_id, created_at)
                            VALUES (?, ?, ?, ?)
                            ''', (chat.id, chat.title, thread_id, datetime.datetime.now()))
                            db_connection.commit()
                            
                            # Логируем успешное создание треда
                            logger.info(f"Успешно сохранена запись в БД для треда {thread_id} группы {chat.title}")
                            
                            # Отправляем начальную информацию о группе
                            try:
                                # Базовая информация о группе без лишних запросов к API
                                info_message = f"📋 **Новая группа: {chat.title}**\n\n"
                                info_message += f"🆔 **ID группы**: `{chat.id}`\n"
                                
                                await client.send_message(
                                    chat_id=SUPPORT_GROUP_ID,
                                    text=info_message,
                                    reply_to_message_id=thread_id
                                )
                                
                                # Создаем задачу для асинхронного получения дополнительной информации
                                asyncio.create_task(send_additional_group_info(client, chat.id, thread_id))
                                
                            except Exception as e:
                                logger.error(f"Ошибка при отправке информации о группе: {e}")
                            
                            logger.info(f"Создан тред {thread_id} для группы {chat.title}")
                        except sqlite3.IntegrityError as e:
                            # Обрабатываем возможные конфликты с базой данных
                            logger.warning(f"Ошибка целостности БД при сохранении треда для группы {chat.id}: {e}")
                            
                            # Проверяем, не успел ли кто-то другой создать тред для этой группы
                            cursor.execute('SELECT thread_id FROM group_threads WHERE group_id = ?', (chat.id,))
                            concurrent_thread = cursor.fetchone()
                            if concurrent_thread:
                                logger.info(f"Обнаружен параллельно созданный тред {concurrent_thread[0]} для группы {chat.title}")
                                # Пропускаем дальнейшие действия
                            else:
                                # Если записи все еще нет, пробуем обновить существующую или создать новую
                                cursor.execute('''
                                INSERT OR REPLACE INTO group_threads (group_id, group_title, thread_id, created_at)
                                VALUES (?, ?, ?, ?)
                                ''', (chat.id, chat.title, thread_id, datetime.datetime.now()))
                                db_connection.commit()
                                logger.info(f"Создана/обновлена запись для треда {thread_id} группы {chat.title}")
                        except Exception as e:
                            logger.error(f"Непредвиденная ошибка при сохранении треда для группы {chat.id}: {e}")
            finally:
                # Освобождаем блокировку в любом случае
                lock.release()
        
        # Отслеживаем изменения участников в группах
        # ... (остальной код без изменений)
    
    except Exception as e:
        logger.error(f"Ошибка при обработке обновления участников чата: {e}")
        # Убедимся, что блокировка освобождена, если произошла ошибка
        if 'lock' in locals() and lock.locked():
            lock.release()
            
# Вспомогательная функция для асинхронного получения дополнительной информации о группе
async def send_additional_group_info(client, chat_id, thread_id):
    try:
        # Делаем паузу, чтобы избежать FLOOD_WAIT
        await asyncio.sleep(2)
        
        # Используем кэширование для предотвращения FLOOD_WAIT
        if not hasattr(send_additional_group_info, "chat_info_cache"):
            send_additional_group_info.chat_info_cache = {}
        
        if chat_id in send_additional_group_info.chat_info_cache:
            chat_info = send_additional_group_info.chat_info_cache[chat_id]
        else:
            # Получаем информацию о группе
            chat_info = await client.get_chat(chat_id)
            send_additional_group_info.chat_info_cache[chat_id] = chat_info
        
        # Делаем еще одну паузу перед запросом количества участников
        await asyncio.sleep(3)
        
        member_count = await client.get_chat_members_count(chat_id)
        
        # Формируем дополнительную информацию
        additional_info = f"👥 **Обновленная информация о группе**\n\n"
        additional_info += f"**Участников**: {member_count}\n"
        
        if hasattr(chat_info, 'description') and chat_info.description:
            additional_info += f"**Описание**: {chat_info.description}\n"
            
        await client.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=additional_info,
            reply_to_message_id=thread_id
        )
        
        # Отправляем список участников с задержкой
        await asyncio.sleep(5)
        await send_member_list(client, chat_id, thread_id)
        
    except Exception as e:
        logger.error(f"Ошибка при получении дополнительной информации о группе: {e}")
        
# Обработчик сообщений из групп
@business.on_message(filters.group & ~filters.chat(SUPPORT_GROUP_ID))
async def handle_group_messages(client, message):
    try:
        # Получаем информацию о группе
        chat = message.chat
        
        # Проверяем, есть ли тред для этой группы
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id FROM group_threads WHERE group_id = ?', (chat.id,))
        thread_data = cursor.fetchone()
        
        if not thread_data:
            # Нет треда для этой группы, возможно бот был добавлен ранее
            # Создаем тред
            thread_title_base = f"Группа: {chat.title}"
            thread_id = await create_support_thread(client, thread_title_base)
            
            if thread_id:
                # Сохраняем информацию о треде
                cursor.execute('''
                INSERT INTO group_threads (group_id, group_title, thread_id, created_at)
                VALUES (?, ?, ?, ?)
                ''', (chat.id, chat.title, thread_id, datetime.datetime.now()))
                db_connection.commit()
                
                # Отправляем информацию о группе
                await client.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=f"📋 **Новая группа: {chat.title}**\nБот был добавлен ранее, создан тред для сообщений.",
                    reply_to_message_id=thread_id
                )
                
                # Отправляем список участников
                await send_member_list(client, chat.id, thread_id)
                
                logger.info(f"Создан тред {thread_id} для существующей группы {chat.title}")
            else:
                logger.error(f"Не удалось создать тред для группы {chat.title}")
                return
        else:
            thread_id = thread_data[0]
        
        # Получаем информацию о боте
        me = await client.get_me()
        
        # Проверяем, есть ли упоминание бота
        is_mentioned = False
        
        # Проверяем текстовые упоминания через @username
        if message.text and f"@{me.username}" in message.text:
            is_mentioned = True
        
        # Проверяем упоминания через entities
        if message.entities:
            for entity in message.entities:
                if entity.type == "mention" and message.text[entity.offset:entity.offset+entity.length] == f"@{me.username}":
                    is_mentioned = True
                    break
                elif entity.type == "text_mention" and entity.user and entity.user.id == me.id:
                    is_mentioned = True
                    break
        
        # Получаем имя отправителя
        user = message.from_user
        user_name = f"{user.first_name or ''}"
        if user.last_name:
            user_name += f" {user.last_name}"
        if user.username:
            user_name += f" (@{user.username})"
        
        # Получаем ответственного менеджера для треда
        duty_manager = get_duty_manager(db_connection, thread_id)
            
        # ВАЖНО: Удаляем префикс при упоминании бота
        if is_mentioned:
            # Простое форматирование сообщения без префиксов о срочности
            forwarded_text = f"**От {user_name}**:\n\n"
            
            if message.text:
                forwarded_text += message.text
            
            # Отправляем текст в тред
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=forwarded_text,
                reply_to_message_id=thread_id
            )
            
            # Отправляем медиа, если есть (фото, видео и т.д.)
            if message.media:
                await client.copy_message(
                    chat_id=SUPPORT_GROUP_ID,
                    from_chat_id=message.chat.id,
                    message_id=message.id,
                    reply_to_message_id=thread_id
                )
            
            # ВАЖНО: Активация системы оповещений
            # Проверяем, существует ли запись thread_status для этого треда
            cursor.execute('SELECT thread_id FROM thread_status WHERE thread_id = ?', (thread_id,))
            status_exists = cursor.fetchone()
            
            current_time = datetime.datetime.now()
            
            if status_exists:
                # Обновляем время последнего сообщения
                cursor.execute('''
                UPDATE thread_status 
                SET last_client_message = ?, is_notified = 0
                WHERE thread_id = ?
                ''', (current_time, thread_id))
            else:
                # Создаем новую запись в thread_status
                cursor.execute('''
                INSERT INTO thread_status 
                (thread_id, last_client_message, is_notified, notification_disabled)
                VALUES (?, ?, 0, 0)
                ''', (thread_id, current_time))
            
            db_connection.commit()
            
            logger.info(f"Активированы оповещения для треда {thread_id} из группы {chat.title}")
            
            # Если нет ответственного менеджера, пробуем найти последнего отвечавшего
            if not duty_manager:
                cursor.execute('''
                SELECT m.username 
                FROM managers m
                JOIN (
                    SELECT manager_id 
                    FROM first_replies 
                    WHERE thread_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                ) fr ON m.manager_id = fr.manager_id
                ''', (thread_id,))
                manager_data = cursor.fetchone()
                
                if manager_data and manager_data[0]:
                    # Назначаем последнего отвечавшего менеджера ответственным
                    assign_duty_manager(db_connection, thread_id, manager_data[0], me.id)
                    logger.info(f"Автоматически назначен ответственный менеджер {manager_data[0]} для треда {thread_id}")
        else:
            # Просто пересылаем сообщение без пометок и без активации уведомлений
            # Отформатированное сообщение
            forwarded_text = f"**От {user_name}**:\n\n"
            
            if message.text:
                forwarded_text += message.text
            
            # Отправляем текст в тред
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=forwarded_text,
                reply_to_message_id=thread_id
            )
            
            # Отправляем медиа, если есть (фото, видео и т.д.)
            if message.media:
                await client.copy_message(
                    chat_id=SUPPORT_GROUP_ID,
                    from_chat_id=message.chat.id,
                    message_id=message.id,
                    reply_to_message_id=thread_id
                )
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения из группы: {e}")

# Единый обработчик для всех медиа-файлов в группе поддержки
@business.on_message(filters.chat(SUPPORT_GROUP_ID) & (filters.photo | filters.document | filters.video | filters.audio | filters.voice))
async def handle_support_group_media(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            # Игнорируем неавторизованных пользователей
            return
        
        # Проверяем, является ли это частью медиа-группы
        is_media_group = hasattr(message, 'media_group_id') and message.media_group_id
        
        # Определяем тип медиа для логирования
        media_type = "неизвестный"
        if hasattr(message, 'photo') and message.photo:
            media_type = "фото"
        elif hasattr(message, 'document') and message.document:
            media_type = "документ"
        elif hasattr(message, 'video') and message.video:
            media_type = "видео"
        elif hasattr(message, 'audio') and message.audio:
            media_type = "аудио"
        elif hasattr(message, 'voice') and message.voice:
            media_type = "голосовое"
        
        # Получаем текст подписи
        caption_text = message.caption or ""
        
        # Проверяем наличие командной подписи
        has_thread_command = False
        thread_id = None
        command_match = re.match(r"^/(\d+)", caption_text)
        
        if command_match:
            has_thread_command = True
            thread_id = int(command_match.group(1))
            
        # Получаем текст подписи без команды
        caption = None
        if has_thread_command and " " in caption_text:
            caption = caption_text.split(" ", 1)[1]
        elif not has_thread_command and caption_text:
            caption = caption_text
        
        # Логируем для отладки
        logger.info(f"Получено медиа сообщение типа {media_type}, media_group_id: {message.media_group_id if is_media_group else 'нет'}, "
                   f"has_thread_command: {has_thread_command}, thread_id: {thread_id}, caption: {caption}")
        
        # Если это не часть медиа-группы, обрабатываем как обычный файл
        if not is_media_group:
            # Обычный файл (не группа)
            if has_thread_command:
                # Проверяем, существует ли клиент с таким thread_id
                client_data = get_client_by_thread(db_connection, thread_id)
                if not client_data:
                    await message.reply_text(
                        f"Не удалось найти клиента, связанного с темой {thread_id}."
                    )
                    return
                    
                client_id = client_data[0]  # user_id
                
                # Отправляем медиафайл клиенту
                file_id = None
                if hasattr(message, 'photo') and message.photo:
                    file_id = message.photo.file_id
                    media_type = "photo"
                elif hasattr(message, 'document') and message.document:
                    file_id = message.document.file_id
                    media_type = "document"
                elif hasattr(message, 'video') and message.video:
                    file_id = message.video.file_id
                    media_type = "video"
                elif hasattr(message, 'audio') and message.audio:
                    file_id = message.audio.file_id
                    media_type = "audio"
                elif hasattr(message, 'voice') and message.voice:
                    file_id = message.voice.file_id
                    media_type = "voice"
                
                # Отправляем медиафайл клиенту
                success = await send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type)
                
                if success:
                    update_manager_reply_time(db_connection, thread_id)
                    await mark_thread_urgent(client, thread_id, is_urgent=False)
                    
                    if is_first_reply(db_connection, thread_id, manager_id):
                        save_first_reply(db_connection, thread_id, client_id, manager_id)
            
            return
        
        # Дальше обрабатываем только медиа-группы
        media_group_id = message.media_group_id
        
        # Случай 1: Файл с командой (это может быть любой файл группы)
        if has_thread_command:
            # Проверяем, существует ли клиент с таким thread_id
            client_data = get_client_by_thread(db_connection, thread_id)
            if not client_data:
                await message.reply_text(
                    f"Не удалось найти клиента, связанного с темой {thread_id}."
                )
                return
                
            client_id = client_data[0]  # user_id
            
            # Проверяем, есть ли уже эта группа в инициализированных
            is_new_group = False
            if media_group_id not in manager_media_groups:
                # Создаём новую запись для этой группы
                manager_media_groups[media_group_id] = {
                    "thread_id": thread_id,
                    "client_id": client_id,
                    "manager_id": manager_id,
                    "caption": caption,
                    "files": [],
                    "timestamp": datetime.datetime.now(),
                    "processing_task": None,
                    "initialized_by_message_id": message.id
                }
                is_new_group = True
                logger.info(f"Создана новая запись для медиа-группы {media_group_id} с thread_id={thread_id}, client_id={client_id}")
                
                # Проверяем, есть ли файлы в ожидании для этой группы
                if media_group_id in pending_media_groups:
                    # Добавляем все файлы из ожидания в основную группу
                    pending_files = pending_media_groups[media_group_id]["files"]
                    manager_media_groups[media_group_id]["files"].extend(pending_files)
                    
                    logger.info(f"Добавлено {len(pending_files)} файлов из ожидания в группу {media_group_id}")
                    
                    # Удаляем из ожидания
                    del pending_media_groups[media_group_id]
                
                # Отправляем подтверждение о начале сбора группы
                await message.reply_text(
                    f"✅ Первый файл медиа-группы принят. Ожидаю остальные файлы группы..."
                )
            
            # Добавляем текущий файл в группу, если его там еще нет
            file_already_in_group = False
            for existing_msg in manager_media_groups[media_group_id]["files"]:
                if existing_msg.id == message.id:
                    file_already_in_group = True
                    break
                    
            if not file_already_in_group:
                manager_media_groups[media_group_id]["files"].append(message)
                logger.info(f"Добавлен файл в медиа-группу {media_group_id}, всего файлов: {len(manager_media_groups[media_group_id]['files'])}")
            
            # Обновляем таймер обработки группы
            if manager_media_groups[media_group_id]["processing_task"]:
                manager_media_groups[media_group_id]["processing_task"].cancel()
                
            # Запускаем таймер для отправки файлов после задержки
            task = asyncio.create_task(process_manager_media_group_after_delay(client, media_group_id, 3))
            manager_media_groups[media_group_id]["processing_task"] = task
            
            return
        
        # Случай 2: Файл без команды, но группа уже инициализирована
        if media_group_id in manager_media_groups:
            # Добавляем файл в группу, если его там еще нет
            file_already_in_group = False
            for existing_msg in manager_media_groups[media_group_id]["files"]:
                if existing_msg.id == message.id:
                    file_already_in_group = True
                    break
                    
            if not file_already_in_group:
                manager_media_groups[media_group_id]["files"].append(message)
                logger.info(f"Добавлен файл без команды в существующую медиа-группу {media_group_id}, всего файлов: {len(manager_media_groups[media_group_id]['files'])}")
            
            # Обновляем таймер обработки группы
            if manager_media_groups[media_group_id]["processing_task"]:
                manager_media_groups[media_group_id]["processing_task"].cancel()
                
            # Запускаем таймер для отправки файлов после задержки
            task = asyncio.create_task(process_manager_media_group_after_delay(client, media_group_id, 3))
            manager_media_groups[media_group_id]["processing_task"] = task
            
            return
        
        # Случай 3: Файл без команды, и группа еще не инициализирована
        # Добавляем в ожидающие файлы
        if media_group_id not in pending_media_groups:
            pending_media_groups[media_group_id] = {
                "files": [],
                "manager_id": manager_id,
                "timestamp": datetime.datetime.now()
            }
            
        # Добавляем файл в список ожидания
        file_already_pending = False
        for existing_msg in pending_media_groups[media_group_id]["files"]:
            if existing_msg.id == message.id:
                file_already_pending = True
                break
                
        if not file_already_pending:
            pending_media_groups[media_group_id]["files"].append(message)
            logger.info(f"Добавлен файл в ожидающую медиа-группу {media_group_id}, всего в ожидании: {len(pending_media_groups[media_group_id]['files'])}")
        
        # Запускаем таймер для очистки старых ожидающих групп
        asyncio.create_task(cleanup_pending_group_after_delay(media_group_id, 10))
        
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")

# Обработчик команд /#custom_id с медиафайлами
# Обработчик команд /#custom_id с медиафайлами
@business.on_message(filters.regex(r"^/#[A-Za-zА-Яа-я0-9]+") & filters.chat(SUPPORT_GROUP_ID) & (filters.photo | filters.document | filters.video | filters.audio | filters.voice))
async def handle_media_with_custom_id(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Извлекаем custom_id из первого слова текста, удаляя префикс "/#"
        caption_text = message.caption or ""
        if not caption_text:
            await message.reply_text(
                "Пожалуйста, добавьте подпись к медиафайлу в формате: /#{custom_id} [текст подписи]"
            )
            return
            
        first_word = caption_text.split()[0]
        custom_id = first_word[2:]  # Отрезаем "/#" и получаем custom_id
        
        logger.info(f"Получен медиафайл с командой /#{custom_id} от менеджера {manager_id}")
        
        # Ищем клиента по custom_id
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id, user_id FROM clients WHERE custom_id = ?', (custom_id,))
        result = cursor.fetchone()
        
        if not result:
            # Пытаемся найти по частичному совпадению
            cursor.execute('SELECT thread_id, user_id, custom_id FROM clients WHERE custom_id LIKE ?', (f'%{custom_id}%',))
            results = cursor.fetchall()
            
            if results:
                # Если нашли несколько совпадений, предлагаем выбрать
                if len(results) > 1:
                    reply_text = "Найдено несколько клиентов с похожим ID:\n\n"
                    for thread_id, user_id, found_id in results:
                        cursor.execute('SELECT first_name, last_name FROM clients WHERE user_id = ?', (user_id,))
                        name_data = cursor.fetchone()
                        client_name = f"{name_data[0] or ''} {name_data[1] or ''}".strip()
                        reply_text += f"- {found_id}: {client_name} (тред #{thread_id})\n"
                    
                    reply_text += "\nПожалуйста, используйте полный ID клиента в формате: /#{results[0][2]} текст ответа"
                    await message.reply_text(reply_text)
                    return
                
                # Если нашли одно приблизительное совпадение, используем его
                thread_id, client_id, found_id = results[0]
                custom_id = found_id  # Используем полный ID
            else:
                await message.reply_text(
                    f"Клиент с ID '{custom_id}' не найден.\n\nПроверьте ID и повторите попытку или используйте команду /threads для просмотра всех активных тредов."
                )
                return
        else:
            thread_id, client_id = result
            
        # Проверяем, является ли это частью медиа-группы
        if hasattr(message, 'media_group_id') and message.media_group_id:
            media_group_id = message.media_group_id
            
            # Сохраняем информацию о группе для последующих файлов без подписи
            manager_media_groups[media_group_id] = {
                "thread_id": thread_id,
                "client_id": client_id,
                "manager_id": manager_id,
                "caption": None if not " " in caption_text else caption_text.split(" ", 1)[1],
                "files": [message],  # Сразу добавляем текущий файл
                "timestamp": datetime.datetime.now(),
                "processed": False,
                "initialized_by_message_id": message.id
            }
            
            # Логируем для отладки
            logger.info(f"Создана новая запись для медиа-группы {media_group_id} с custom_id={custom_id}, client_id={client_id}")
            
            # Не отправляем файл сейчас, а отложим до получения всех файлов группы
            await message.reply_text(
                f"✅ Первый файл медиа-группы принят. Ожидаю остальные файлы группы..."
            )
            
            # Запускаем таймер для отправки файлов после задержки
            asyncio.create_task(process_manager_media_group_after_delay(client, media_group_id, 3))
            return
            
        # Обрабатываем одиночный медиафайл
        
        # Парсим текст подписи для получения сообщения
        caption = None
        if " " in caption_text:
            caption = caption_text.split(" ", 1)[1]
        
        # Определяем тип медиафайла и получаем его file_id
        file_id = None
        media_type = None
        
        if hasattr(message, 'photo') and message.photo:
            file_id = message.photo.file_id
            media_type = "photo"
        elif hasattr(message, 'document') and message.document:
            file_id = message.document.file_id
            media_type = "document"
        elif hasattr(message, 'video') and message.video:
            file_id = message.video.file_id
            media_type = "video"
        elif hasattr(message, 'audio') and message.audio:
            file_id = message.audio.file_id
            media_type = "audio"
        elif hasattr(message, 'voice') and message.voice:
            file_id = message.voice.file_id
            media_type = "voice"
        
        # Отправляем медиафайл клиенту
        success = await send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type)
        
        if success:
            # Обновляем время последнего ответа менеджера
            update_manager_reply_time(db_connection, thread_id)
            
            # Назначаем менеджера ответственным
            _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
            if username:
                assign_duty_manager(db_connection, thread_id, username, manager_id)
            
            # Сбрасываем индикатор срочности
            try:
                await mark_thread_urgent(client, thread_id, is_urgent=False)
            except Exception as e:
                logger.error(f"Ошибка при изменении индикатора треда: {e}")
            
            # Не отправляем сообщение о подтверждении
            logger.info(f"Медиафайл от менеджера {manager_id} отправлен клиенту {client_id} по ID {custom_id}")
            
            # Сохраняем информацию о первом ответе менеджера
            if is_first_reply(db_connection, thread_id, manager_id):
                save_first_reply(db_connection, thread_id, client_id, manager_id)
        else:
            await message.reply_text("❌ Не удалось отправить медиафайл клиенту.")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла с custom_id: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Обновляем обработчик ответов менеджеров, чтобы они могли отвечать в группы
@business.on_message(filters.regex(r"^/reply_\d+") & filters.chat(SUPPORT_GROUP_ID))
async def handle_reply_to_group(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Извлекаем ID группы из команды /reply_123
        command_text = message.text.strip()
        first_word = command_text.split()[0]
        
        try:
            group_id = int(first_word[7:])  # Отрезаем "/reply_" и преобразуем в число
        except ValueError:
            await message.reply_text("Неверный формат команды. Используйте: /reply_GROUP_ID текст ответа")
            return
        
        # Проверяем, есть ли группа в базе данных
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id, group_title FROM group_threads WHERE group_id = ?', (group_id,))
        group_data = cursor.fetchone()
        
        if not group_data:
            await message.reply_text(f"Группа с ID {group_id} не найдена в базе данных.")
            return
        
        thread_id, group_title = group_data
        
        # Парсим текст сообщения для получения ответа
        if " " in command_text:
            reply_text = command_text.split(" ", 1)[1]
        else:
            await message.reply_text(f"Пожалуйста, укажите текст ответа после команды /reply_{group_id}")
            return
        
        # Формируем подпись менеджера с добавочным номером
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
        signature = f"\n\n{emoji} {name}, {position}, доб. {extension}"
        
        # Полное сообщение с подписью
        full_message = f"{reply_text}{signature}"
        
        # Отправляем сообщение в группу
        try:
            sent_message = await client.send_message(
                chat_id=group_id,
                text=full_message
            )
            
            # Обновляем время последнего ответа менеджера для этого треда
            update_manager_reply_time(db_connection, thread_id)
            
            # Отправляем подтверждение
            await message.reply_text(f"✅ Ответ отправлен в группу {group_title}.")
            
            logger.info(f"Ответ менеджера {manager_id} отправлен в группу {group_id}")
            
            # Добавляем информацию об отправленном ответе в тред
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=f"📤 **Ответ отправлен в группу**\n\n{full_message}",
                reply_to_message_id=thread_id
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения в группу {group_id}: {e}")
            await message.reply_text(f"❌ Ошибка при отправке сообщения в группу: {e}")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды ответа в группу: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Команда для вывода списка групп
@business.on_message(filters.command("groups") & filters.chat(SUPPORT_GROUP_ID))
async def handle_list_groups(client, message):
    try:
        cursor = db_connection.cursor()
        cursor.execute('''
        SELECT g.group_id, g.group_title, g.thread_id, g.created_at,
               dm.manager_username
        FROM group_threads g
        LEFT JOIN duty_managers dm ON g.thread_id = dm.thread_id
        ORDER BY g.created_at DESC
        ''')
        
        groups = cursor.fetchall()
        
        if not groups:
            await message.reply_text("Нет активных групп в базе данных.")
            return
        
        response = "📋 **Список групп**:\n\n"
        
        for group in groups:
            group_id, group_title, thread_id, created_at, manager_username = group
            
            response += f"🔹 **{group_title}**\n"
            response += f"   🆔 ID группы: `{group_id}`\n"
            response += f"   🧵 ID треда: {thread_id}\n"
            
            if manager_username:
                response += f"   👨‍💼 Ответственный: @{manager_username}\n"
            else:
                response += f"   👨‍💼 Ответственный: Не назначен\n"
            
            response += f"   📅 Добавлен: {created_at}\n"
            response += f"   📝 Ответить: `/reply_{group_id} текст`\n\n"
        
        await message.reply_text(response)
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка групп: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
 
# Команда для назначения ответсвенного       
@business.on_message(filters.command("onduty") & filters.chat(SUPPORT_GROUP_ID))
async def handle_assign_duty(client, message):
    try:
        assigner_id = message.from_user.id
        
        # Проверяем, авторизован ли назначающий менеджер
        manager = get_manager(db_connection, assigner_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Парсим команду: /onduty @username [thread_id]
        command_text = message.text.strip()
        parts = command_text.split()
        
        # Проверяем формат команды
        if len(parts) != 3:
            await message.reply_text(
                "Неверный формат команды. Используйте: /onduty @username {thread_id}"
            )
            return
        
        # Получаем username (удаляем @ если есть)
        manager_username = parts[1]
        if manager_username.startswith('@'):
            manager_username = manager_username[1:]
        
        # Получаем thread_id
        try:
            thread_id = int(parts[2])
        except ValueError:
            await message.reply_text("ID треда должен быть числом.")
            return
        
        # Проверяем, существует ли тред
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text(
                f"Не удалось найти клиента, связанного с темой {thread_id}."
            )
            return
        
        # Назначаем ответственного менеджера
        assign_duty_manager(db_connection, thread_id, manager_username, assigner_id)
        
        await message.reply_text(
            f"✅ Менеджер @{manager_username} назначен ответственным за тред #{thread_id}."
        )
        logger.info(f"Менеджер @{manager_username} назначен ответственным за тред {thread_id} пользователем {assigner_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при назначении ответственного менеджера: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Обработчик команды /start
@business.on_message(filters.command("start") & filters.private)
async def handle_start(client, message):
    logger.info(f"Получена команда /start от пользователя {message.from_user.id}")
    await message.reply_text(
        "Фото успешно добавлено! Ваша авторизация завершена.\n"
        "Теперь вы можете отвечать клиентам, используя команду /{thread_id} в теме клиента."
    )
    logger.info("Ответ на команду /start отправлен")

# Обработчик команды авторизации менеджера
@business.on_message(filters.command("auth") & filters.chat(SUPPORT_GROUP_ID))
async def handle_auth(client, message):
    try:
        manager_id = message.from_user.id
        manager_username = message.from_user.username  # Получаем username менеджера
        
        logger.info(f"Получена команда /auth от пользователя {manager_id} (username: {manager_username}) в группе поддержки")
        
        # Парсим команду: /auth [emoji], [name], [position], [extension]
        command_text = message.text.strip()
        
        # Удаляем команду /auth
        if " " in command_text:
            auth_data = command_text.split(" ", 1)[1]
        else:
            await message.reply_text(
                "Неверный формат команды. Используйте: /auth [эмодзи], [Имя], [Должность], [4 цифры]\n"
                "Например: /auth 🔧, Иван Петров, Технический специалист, 1234"
            )
            return
        
        # Разделяем по запятым и удаляем лишние пробелы
        parts = [part.strip() for part in auth_data.split(",")]
        
        # Проверяем, что есть как минимум 4 части
        if len(parts) < 4:
            await message.reply_text(
                "Неверный формат команды. Требуется: [эмодзи], [Имя], [Должность], [4 цифры]\n"
                "Например: /auth 🔧, Иван Петров, Технический специалист, 1234"
            )
            return
        
        # Получаем данные
        emoji = parts[0]
        name = parts[1]
        
        # Все части между именем и добавочным номером считаем должностью
        position = ", ".join(parts[2:-1])
        
        # Последняя часть - добавочный номер
        extension = parts[-1].strip()
        
        # Проверяем, что добавочный номер состоит из 4 цифр
        if not re.match(r'^\d{4}$', extension):
            await message.reply_text(
                "Неверный формат добавочного номера. Необходимо указать 4 цифры."
            )
            return
        
        # Сохраняем данные о менеджере с username (без фото пока)
        save_manager(db_connection, manager_id, emoji, name, position, extension, username=manager_username)
        
        # Запрашиваем фото менеджера
        await message.reply_text(
            f"Спасибо! Теперь, пожалуйста, отправьте фотографию для вашего профиля.\n"
            f"Фото будет показано клиентам при ответе на их обращения."
        )
        
        # Устанавливаем состояние ожидания фото
        manager_auth_state[manager_id] = "waiting_photo"
        
        logger.info(f"Менеджер {manager_id} ({name}) успешно зарегистрирован, ожидается фото")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /auth: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

# Обработчик для приема фотографий менеджеров
# Обработчик фото от менеджера должен иметь более высокий приоритет
@business.on_message(filters.photo & filters.chat(SUPPORT_GROUP_ID), group=-1)  # Высокий приоритет
async def handle_manager_photo(client, message):
    try:
        manager_id = message.from_user.id
        manager_username = message.from_user.username
        
        logger.info(f"Обработка фото от пользователя {manager_id} (username: {manager_username})")
        
        # Проверяем, ожидается ли фото от этого менеджера
        if manager_id in manager_auth_state and manager_auth_state[manager_id] == "waiting_photo":
            logger.info(f"Обработка фото для авторизации менеджера {manager_id}")
            
            # Получаем информацию о менеджере
            manager = get_manager(db_connection, manager_id)
            
            if manager:
                # Получаем file_id фотографии
                photo_file_id = message.photo.file_id
                
                # Обновляем запись менеджера с фото
                update_manager_photo(db_connection, manager_id, photo_file_id)
                
                cursor = db_connection.cursor()
                cursor.execute('UPDATE managers SET username = ? WHERE manager_id = ?', 
                             (manager_username, manager_id))
                db_connection.commit()
                
                # Удаляем состояние ожидания
                del manager_auth_state[manager_id]
                
                # Отправляем подтверждение
                await message.reply_text(
                    f"Фото успешно добавлено! Ваша авторизация завершена.\n"
                    f"Теперь вы можете отвечать клиентам, используя команду /(номер треда) в теме клиента."
                )
                
                logger.info(f"Фото менеджера {manager_id} успешно сохранено")
                return True
            else:
                await message.reply_text(
                    "Не удалось найти вашу регистрацию. Пожалуйста, используйте сначала команду /auth."
                )
                return True
        else:
            # Если сообщение с фото не для авторизации, продолжаем обработку другими обработчиками
            logger.info(f"Фото не для авторизации менеджера, продолжаем обработку {manager_id}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при обработке фото менеджера: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        return True

# Команда для получения информации
@business.on_message(filters.command("myinfo") & filters.chat(SUPPORT_GROUP_ID))
async def handle_myinfo(client, message):
    try:
        manager_id = message.from_user.id
        manager_username = message.from_user.username
        
        logger.info(f"Получена команда /myinfo от пользователя {manager_id} (username: {manager_username})")
        
        # Получаем информацию о менеджере из базы
        manager = get_manager(db_connection, manager_id)
        
        if not manager:
            await message.reply_text("Вы не зарегистрированы в системе. Используйте команду /auth для регистрации.")
            return
        
        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, db_username = manager
        
        # Отправляем информацию
        info_text = f"Ваша информация в системе:\n\n"
        info_text += f"ID: {manager_id}\n"
        info_text += f"Emoji: {emoji}\n"
        info_text += f"Имя: {name}\n"
        info_text += f"Должность: {position}\n"
        info_text += f"Добавочный: {extension}\n"
        info_text += f"Текущий username: {manager_username}\n"
        info_text += f"Username в базе: {db_username}\n"
        info_text += f"Фото: {'Загружено' if photo_file_id else 'Не загружено'}\n"
        
        await message.reply_text(info_text)
        
        # Если username в базе отличается от текущего, предлагаем обновить
        if db_username != manager_username:
            # Обновляем username в базе
            cursor = db_connection.cursor()
            cursor.execute('UPDATE managers SET username = ? WHERE manager_id = ?', (manager_username, manager_id))
            db_connection.commit()
            
            await message.reply_text(f"Ваш username в базе данных обновлен с {db_username} на {manager_username}.")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /myinfo: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

# Обработчик команды ответа менеджера клиенту - команд вида /{num}, где num - номер треда
@business.on_message(filters.regex(r"^/\d+") & filters.chat(SUPPORT_GROUP_ID))
async def handle_thread_number_command(client, message):
   try:
       manager_id = message.from_user.id
       
       # Извлекаем текст команды (из text или caption)
       command_text = ""
       if hasattr(message, 'text') and message.text:
           command_text = message.text.strip()
       elif hasattr(message, 'caption') and message.caption:
           command_text = message.caption.strip()
       else:
           await message.reply_text("Необходимо добавить текст или подпись к сообщению.")
           return
       
       # Извлекаем номер треда из первого слова текста, удаляя префикс "/"
       first_word = command_text.split()[0]
       thread_id = int(first_word[1:])  # Отрезаем "/" и преобразуем в число
       
       logger.info(f"Получена команда /{thread_id} от менеджера {manager_id}")
       
       # Проверяем, авторизован ли менеджер
       manager = get_manager(db_connection, manager_id)
       if not manager:
           await message.reply_text(
               "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
           )
           return
       
       # Безопасная распаковка данных менеджера
       _, emoji, name, position, extension, photo_file_id, auth_date, username = unpack_manager_data(manager)
       
       # Проверяем наличие media_group_id (это часть медиа-группы)
       if hasattr(message, 'media_group_id') and message.media_group_id:
           media_group_id = message.media_group_id
           
           # Проверяем, это тред клиента или тред группы
           cursor = db_connection.cursor()
           cursor.execute('SELECT user_id FROM clients WHERE thread_id = ?', (thread_id,))
           client_data = cursor.fetchone()
   
           if client_data:
               client_id = client_data[0]
               
               # Сохраняем информацию о медиа-группе
               # Парсим текст подписи для получения сообщения
               caption = None
               if " " in command_text:
                   caption = command_text.split(" ", 1)[1]
               
               # Инициализируем группу, если это первое сообщение из группы
               if media_group_id not in manager_media_groups:
                   manager_media_groups[media_group_id] = {
                       "thread_id": thread_id,
                       "client_id": client_id,
                       "manager_id": manager_id,
                       "caption": caption,
                       "files": [message],  # Добавляем текущий файл
                       "timestamp": datetime.datetime.now(),
                       "processed": False,
                       "initialized_by_message_id": message.id
                   }
                   
                   # Отправляем подтверждение о начале сбора группы
                   await message.reply_text(f"✅ Первый файл медиа-группы принят. Ожидаю остальные файлы группы...")
               else:
                   # Добавляем файл в существующую группу
                   manager_media_groups[media_group_id]["files"].append(message)
                   manager_media_groups[media_group_id]["timestamp"] = datetime.datetime.now()
               
               # Запускаем обработку после задержки
               asyncio.create_task(process_manager_media_group_after_delay(client, media_group_id, 3))
               return
           
           else:
               # Проверяем в таблице групп
               cursor.execute('SELECT group_id FROM group_threads WHERE thread_id = ?', (thread_id,))
               group_data = cursor.fetchone()
       
               if not group_data:
                   await message.reply_text(f"Не удалось найти клиента или группу для треда {thread_id}.")
                   return
               
               # Для групп обрабатываем как обычное сообщение
               group_id = group_data[0]
               
               # Парсим текст сообщения для получения сообщения
               if " " in command_text:
                   reply_text = command_text.split(" ", 1)[1]
               else:
                   await message.reply_text(
                       f"Пожалуйста, укажите текст сообщения после команды /{thread_id}."
                   )
                   return
               
               # Формируем подпись менеджера
               signature = f"\n\n{emoji} {name}, {position}, доб. {extension}"
               full_message = f"{reply_text}{signature}"
               
               try:
                   await client.send_message(
                       chat_id=group_id,
                       text=full_message
                   )
                   await message.reply_text(f"✅ Сообщение отправлено в группу.")
                   logger.info(f"Сообщение от менеджера {manager_id} отправлено в группу {group_id}")
                   
                   # Назначаем менеджера ответственным за группу
                   if username:
                       assign_duty_manager(db_connection, thread_id, username, manager_id)
                       logger.info(f"Менеджер @{username} назначен ответственным за группу {group_id}")
                   
                   # Обновляем время последнего ответа
                   update_manager_reply_time(db_connection, thread_id)
               except Exception as e:
                   logger.error(f"Ошибка при отправке сообщения в группу: {e}")
                   await message.reply_text(f"❌ Ошибка при отправке сообщения в группу: {e}")
               
               return
       
       # Для фото, документов и других медиафайлов (одиночных) обрабатываем отдельно
       if (hasattr(message, 'photo') and message.photo) or \
          (hasattr(message, 'document') and message.document) or \
          (hasattr(message, 'video') and message.video) or \
          (hasattr(message, 'audio') and message.audio) or \
          (hasattr(message, 'voice') and message.voice):
           
           # Извлекаем подпись
           caption = None
           if " " in command_text:
               caption = command_text.split(" ", 1)[1]
           
           # Определяем тип медиа
           file_id = None
           media_type = None
           
           if hasattr(message, 'photo') and message.photo:
               file_id = message.photo.file_id
               media_type = "photo"
           elif hasattr(message, 'document') and message.document:
               file_id = message.document.file_id
               media_type = "document"
           elif hasattr(message, 'video') and message.video:
               file_id = message.video.file_id
               media_type = "video"
           elif hasattr(message, 'audio') and message.audio:
               file_id = message.audio.file_id
               media_type = "audio"
           elif hasattr(message, 'voice') and message.voice:
               file_id = message.voice.file_id
               media_type = "voice"
           
           # Проверяем, существует ли клиент с таким thread_id
           cursor = db_connection.cursor()
           cursor.execute('SELECT user_id FROM clients WHERE thread_id = ?', (thread_id,))
           client_data = cursor.fetchone()
           
           if client_data:
               client_id = client_data[0]
               
               # Отправляем медиафайл клиенту
               success = await send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type)
               
               if success:
                   update_manager_reply_time(db_connection, thread_id)
                   await mark_thread_urgent(client, thread_id, is_urgent=False)
                   
                   # Проверяем, первый ли это ответ менеджера
                   is_first = is_first_reply(db_connection, thread_id, manager_id)
                   
                   # Сохраняем информацию о первом ответе менеджера, если это первый ответ
                   if is_first:
                       save_first_reply(db_connection, thread_id, client_id, manager_id)
                       
                       # Автоматически назначаем менеджера ответственным
                       if username:
                           assign_duty_manager(db_connection, thread_id, username, manager_id)
                           # Отправляем уведомление о назначении ответственным при первом ответе
                           await message.reply_text(f"✅ Медиафайл отправлен. Вы назначены ответственным за клиента (тред #{thread_id}).")
                           
                           # Отправляем карточку менеджера при первом ответе
                           await send_manager_card_to_client(client, manager_id, client_id)
                       else:
                           await message.reply_text("✅ Медиафайл отправлен клиенту. Рекомендуется добавить username в настройках телеграма для корректной работы системы назначения ответственных.")
                   else:
                       # Для последующих ответов только подтверждение отправки
                       await message.reply_text("✅ Медиафайл отправлен клиенту.")
                   
                   logger.info(f"Медиафайл от менеджера {manager_id} отправлен клиенту {client_id}, статус треда обновлен")
               else:
                   await message.reply_text("❌ Не удалось отправить медиафайл клиенту.")
           else:
               await message.reply_text(f"Не удалось найти клиента для треда {thread_id}.")
               
           return
       
       # Обработка текстовых сообщений (без медиа)
       if hasattr(message, 'text') and message.text:
           # Парсим текст сообщения для получения ответа
           if " " in message.text:
               reply_text = message.text.split(" ", 1)[1]
           else:
               await message.reply_text("Пожалуйста, добавьте текст сообщения после команды.")
               return
           
           # Проверяем, существует ли клиент с таким thread_id
           cursor = db_connection.cursor()
           cursor.execute('SELECT user_id FROM clients WHERE thread_id = ?', (thread_id,))
           client_data = cursor.fetchone()
           
           if not client_data:
               # Проверяем, может это тред группы
               cursor.execute('SELECT group_id FROM group_threads WHERE thread_id = ?', (thread_id,))
               group_data = cursor.fetchone()
               
               if group_data:
                   # Отправляем сообщение в группу
                   group_id = group_data[0]
                   
                   # Формируем подпись менеджера
                   signature = f"\n\n{emoji} {name}, {position}, доб. {extension}"
                   full_message = f"{reply_text}{signature}"
                   
                   try:
                       await client.send_message(
                           chat_id=group_id,
                           text=full_message
                       )
                       await message.reply_text(f"✅ Сообщение отправлено в группу.")
                       logger.info(f"Сообщение от менеджера {manager_id} отправлено в группу {group_id}")
                       
                       # Назначаем менеджера ответственным за группу
                       if username:
                           assign_duty_manager(db_connection, thread_id, username, manager_id)
                           logger.info(f"Менеджер @{username} назначен ответственным за группу {group_id}")
                       
                       # Обновляем время последнего ответа
                       update_manager_reply_time(db_connection, thread_id)
                   except Exception as e:
                       logger.error(f"Ошибка при отправке сообщения в группу: {e}")
                       await message.reply_text(f"❌ Ошибка при отправке сообщения в группу: {e}")
                   return
               else:
                   await message.reply_text(f"Не удалось найти клиента или группу, связанную с темой {thread_id}.")
                   return
           
           client_id = client_data[0]
           
           # Отправляем ответ клиенту
           success = await send_manager_reply_to_client(client, manager_id, client_id, reply_text)
           
           if success:
               # Обновляем время последнего ответа менеджера
               update_manager_reply_time(db_connection, thread_id)
               
               # Убираем пометку срочности
               await mark_thread_urgent(client, thread_id, is_urgent=False)
               
               # Проверяем, первый ли это ответ менеджера
               is_first = is_first_reply(db_connection, thread_id, manager_id)
               
               # Обработка в зависимости от того, первый ли это ответ
               if is_first:
                   # Сохраняем информацию о первом ответе
                   save_first_reply(db_connection, thread_id, client_id, manager_id)
                   
                   # Автоматически назначаем менеджера ответственным
                   if username:
                       assign_duty_manager(db_connection, thread_id, username, manager_id)
                       await message.reply_text(f"✅ Ответ отправлен. Вы назначены ответственным за клиента (тред #{thread_id}).")
                       
                       # Отправляем карточку менеджера при первом ответе
                       await send_manager_card_to_client(client, manager_id, client_id)
                   else:
                       await message.reply_text("✅ Ответ отправлен клиенту. Рекомендуется добавить username в настройках телеграма для корректной работы системы назначения ответственных.")
               else:
                   # Для последующих ответов только подтверждение отправки
                   await message.reply_text("✅ Ответ отправлен клиенту.")
               
               logger.info(f"Ответ менеджера {manager_id} отправлен клиенту {client_id}, статус треда обновлен")
           else:
               await message.reply_text("❌ Не удалось отправить ответ клиенту.")
       else:
           await message.reply_text("Не удалось определить текст сообщения.")
           
   except ValueError:
       # Если номер треда не удалось преобразовать в число
       logger.error(f"Некорректный формат команды: {message.text}")
       await message.reply_text("Некорректный формат команды. Используйте: /{thread_id} {текст ответа}")
   except Exception as e:
       logger.error(f"Ошибка при обработке команды ответа: {e}")
       await message.reply_text(f"Произошла ошибка: {e}")
   
# Обработчик для команд с custom_id клиента с префиксом /#
@business.on_message(filters.regex(r"^/#[A-Za-zА-Яа-я0-9]+") & filters.chat(SUPPORT_GROUP_ID))
async def handle_custom_id_command(client, message):
    try:
        manager_id = message.from_user.id
        
        # Извлекаем текст команды (из text или caption)
        command_text = ""
        if hasattr(message, 'text') and message.text:
            command_text = message.text.strip()
        elif hasattr(message, 'caption') and message.caption:
            command_text = message.caption.strip()
        else:
            await message.reply_text("Необходимо добавить текст или подпись к сообщению.")
            return
        
        # Извлекаем custom_id из первого слова текста, удаляя префикс "/#"
        first_word = command_text.split()[0]
        custom_id = first_word[2:]  # Отрезаем "/#" и получаем custom_id
        
        logger.info(f"Получена команда с custom_id клиента: /#{custom_id} от менеджера {manager_id}")
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Ищем клиента по custom_id
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id, user_id FROM clients WHERE custom_id = ?', (custom_id,))
        result = cursor.fetchone()
        
        if not result:
            # Пытаемся найти по частичному совпадению
            cursor.execute('SELECT thread_id, user_id, custom_id FROM clients WHERE custom_id LIKE ?', (f'%{custom_id}%',))
            results = cursor.fetchall()
            
            if results:
                # Если нашли несколько совпадений, предлагаем выбрать
                if len(results) > 1:
                    reply_text = "Найдено несколько клиентов с похожим ID:\n\n"
                    for thread_id, user_id, found_id in results:
                        cursor.execute('SELECT first_name, last_name FROM clients WHERE user_id = ?', (user_id,))
                        name_data = cursor.fetchone()
                        client_name = f"{name_data[0] or ''} {name_data[1] or ''}".strip()
                        reply_text += f"- {found_id}: {client_name} (тред #{thread_id})\n"
                    
                    reply_text += "\nПожалуйста, используйте полный ID клиента в формате: /#{results[0][2]} текст ответа"
                    await message.reply_text(reply_text)
                    return
                
                # Если нашли одно приблизительное совпадение
                thread_id, client_id, found_id = results[0]
                
                # Подтверждаем, что найден клиент
                cursor.execute('SELECT first_name, last_name FROM clients WHERE user_id = ?', (client_id,))
                name_data = cursor.fetchone()
                client_name = f"{name_data[0] or ''} {name_data[1] or ''}".strip()
                
                # Спрашиваем, хочет ли менеджер использовать найденный ID
                confirmation_text = f"Найден клиент:\n{found_id}: {client_name} (тред #{thread_id})\n\nИспользовать этот ID для ответа?"
                await message.reply_text(
                    confirmation_text,
                    reply_markup=pyrogram.types.InlineKeyboardMarkup([
                        [
                            pyrogram.types.InlineKeyboardButton("Да, ответить", callback_data=f"reply_{found_id}_{message.id}")
                        ]
                    ])
                )
                return
            
            # Если ничего не нашли даже по частичному совпадению
            await message.reply_text(
                f"Клиент с ID '{custom_id}' не найден.\n\nПроверьте ID и повторите попытку или используйте команду /threads для просмотра всех активных тредов."
            )
            return
        
        thread_id, client_id = result
        
        # Проверяем, есть ли у сообщения media_group_id (это группа файлов)
        if hasattr(message, 'media_group_id') and message.media_group_id:
            # Это медиа-группа, собираем все файлы
            if not hasattr(handle_custom_id_command, "media_groups"):
                handle_custom_id_command.media_groups = {}
            
            media_group_id = message.media_group_id
            
            # Инициализируем группу, если это первое сообщение из группы
            if media_group_id not in handle_custom_id_command.media_groups:
                handle_custom_id_command.media_groups[media_group_id] = {
                    "messages": [],
                    "client_id": client_id,
                    "thread_id": thread_id,
                    "manager_id": manager_id,
                    "text": None,
                    "expiry": datetime.datetime.now() + datetime.timedelta(seconds=60)
                }
            
            # Добавляем сообщение в группу
            handle_custom_id_command.media_groups[media_group_id]["messages"].append(message)
            
            # Берем текст из первого сообщения в группе, которое содержит текст
            if " " in command_text:
                reply_text = command_text.split(" ", 1)[1]
                if not handle_custom_id_command.media_groups[media_group_id]["text"]:
                    handle_custom_id_command.media_groups[media_group_id]["text"] = reply_text
            
            # Планируем обработку медиа-группы через 1 секунду
            # Это позволит собрать все файлы из группы перед отправкой
            if not hasattr(handle_custom_id_command, "scheduled_tasks"):
                handle_custom_id_command.scheduled_tasks = {}
            
            # Отменяем предыдущую задачу для этой группы, если она есть
            if media_group_id in handle_custom_id_command.scheduled_tasks:
                handle_custom_id_command.scheduled_tasks[media_group_id].cancel()
            
            # Планируем новую задачу
            async def process_media_group(media_group_id):
                try:
                    await asyncio.sleep(1)  # Ждем 1 секунду, чтобы собрать все файлы группы
                    
                    if media_group_id in handle_custom_id_command.media_groups:
                        group_data = handle_custom_id_command.media_groups[media_group_id]
                        
                        # Отправляем медиа-группу клиенту
                        success = await send_manager_media_group_to_client(
                            client, 
                            group_data["manager_id"], 
                            group_data["client_id"], 
                            group_data["messages"], 
                            group_data["text"]
                        )
                        
                        if success:
                            # Обновляем время последнего ответа менеджера
                            update_manager_reply_time(db_connection, group_data["thread_id"])
                            
                            # Пытаемся убрать индикатор срочности из заголовка треда
                            await mark_thread_urgent(client, group_data["thread_id"], is_urgent=False)
                            
                            # Сохраняем информацию о первом ответе менеджера
                            if is_first_reply(db_connection, group_data["thread_id"], group_data["manager_id"]):
                                save_first_reply(db_connection, group_data["thread_id"], group_data["client_id"], group_data["manager_id"])
                                
                            # Очищаем данные о группе
                            del handle_custom_id_command.media_groups[media_group_id]
                            if media_group_id in handle_custom_id_command.scheduled_tasks:
                                del handle_custom_id_command.scheduled_tasks[media_group_id]
                except Exception as e:
                    logger.error(f"Ошибка при обработке медиа-группы: {e}")
            
            # Создаем и сохраняем задачу
            task = asyncio.create_task(process_media_group(media_group_id))
            handle_custom_id_command.scheduled_tasks[media_group_id] = task
            
            # Запускаем задачу очистки только один раз
            if not hasattr(handle_custom_id_command, "cleanup_task_started"):
                handle_custom_id_command.cleanup_task_started = True
                asyncio.create_task(cleanup_media_groups())
            
            return
        
        # Для фото, документов и других медиафайлов
        if hasattr(message, 'photo') or hasattr(message, 'document') or hasattr(message, 'video') or hasattr(message, 'audio') or hasattr(message, 'voice'):
            file_id = None
            media_type = None
            
            if hasattr(message, 'photo') and message.photo:
                file_id = message.photo.file_id
                media_type = "photo"
            elif hasattr(message, 'document') and message.document:
                file_id = message.document.file_id
                media_type = "document"
            elif hasattr(message, 'video') and message.video:
                file_id = message.video.file_id
                media_type = "video"
            elif hasattr(message, 'audio') and message.audio:
                file_id = message.audio.file_id
                media_type = "audio"
            elif hasattr(message, 'voice') and message.voice:
                file_id = message.voice.file_id
                media_type = "voice"
            
            # Получаем текст сообщения из caption
            reply_text = None
            if " " in command_text:
                reply_text = command_text.split(" ", 1)[1]
            
            # Отправляем медиафайл клиенту
            success = await send_manager_media_to_client(client, manager_id, client_id, file_id, reply_text, media_type)
            
            if success:
                # Обновляем время последнего ответа менеджера
                update_manager_reply_time(db_connection, thread_id)
                
                # Назначаем менеджера ответственным
                _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
                if username:
                    assign_duty_manager(db_connection, thread_id, username, manager_id)
                
                # Сбрасываем индикатор срочности
                try:
                    await mark_thread_urgent(client, thread_id, is_urgent=False)
                except Exception as e:
                    logger.error(f"Ошибка при изменении индикатора треда: {e}")
                
                # Не отправляем сообщение о подтверждении
                logger.info(f"Медиафайл от менеджера {manager_id} отправлен клиенту {client_id} по ID {custom_id}")
                
                # Сохраняем информацию о первом ответе менеджера
                if is_first_reply(db_connection, thread_id, manager_id):
                    save_first_reply(db_connection, thread_id, client_id, manager_id)
            else:
                await message.reply_text("❌ Не удалось отправить медиафайл клиенту.")
            
            return
        
        # Обработка текстовых сообщений (оставляем без изменений)
        # Парсим текст сообщения для получения сообщения
        if " " in command_text:
            reply_text = command_text.split(" ", 1)[1]
        else:
            await message.reply_text(
                f"Пожалуйста, укажите текст ответа после команды /#{custom_id}."
            )
            return
        
        # Отправляем ответ клиенту
        success = await send_manager_reply_to_client(client, manager_id, client_id, reply_text)
        
        if success:
            # Обновляем время последнего ответа менеджера
            update_manager_reply_time(db_connection, thread_id)
            
            # Назначаем менеджера ответственным
            _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
            if username:
                assign_duty_manager(db_connection, thread_id, username, manager_id)
            
            # Сбрасываем индикатор срочности
            try:
                await mark_thread_urgent(client, thread_id, is_urgent=False)
            except Exception as e:
                logger.error(f"Ошибка при изменении индикатора треда: {e}")
            
            # Не отправляем сообщение о подтверждении
            logger.info(f"Ответ менеджера {manager_id} отправлен клиенту {client_id} по ID {custom_id}")
            
            # Сохраняем информацию о первом ответе менеджера
            if is_first_reply(db_connection, thread_id, manager_id):
                save_first_reply(db_connection, thread_id, client_id, manager_id)
        else:
            await message.reply_text("❌ Не удалось отправить ответ клиенту.")
    except Exception as e:
        logger.error(f"Ошибка при обработке custom_id: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Обновленная команда для установки/изменения custom_id клиента
@business.on_message(filters.command("set_id") & filters.chat(SUPPORT_GROUP_ID))
async def handle_set_id_command(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Парсим команду: /set_id {thread_id} [custom_id]
        command_text = message.text.strip()
        parts = command_text.split()
        
        if len(parts) < 2:
            await message.reply_text(
                "Неверный формат команды. Используйте: /set_id {ID_треда} [желаемый_ID]"
            )
            return
        
        # Получаем thread_id
        try:
            thread_id = int(parts[1])
        except ValueError:
            await message.reply_text("ID треда должен быть числом.")
            return
        
        # Получаем клиента по thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text(
                f"Не удалось найти клиента, связанного с темой {thread_id}."
            )
            return
        
        # Получаем ID клиента
        client_id = client_data[0]  # Первый элемент - user_id
        
        # Если указан желаемый ID, используем его, иначе генерируем автоматически
        if len(parts) > 2:
            custom_id = parts[2].upper()  # Преобразуем в верхний регистр
            
            # Проверяем, соответствует ли формат
            if not re.match(r'^[A-ZА-Я0-9]{2,10}$', custom_id):
                await message.reply_text(
                    "Неверный формат ID. ID должен содержать от 2 до 10 символов (буквы и цифры)."
                )
                return
            
            # Проверяем, не занят ли уже такой ID
            cursor = db_connection.cursor()
            cursor.execute('SELECT user_id FROM clients WHERE custom_id = ? AND user_id != ?', (custom_id, client_id))
            existing_client = cursor.fetchone()
            
            if existing_client:
                await message.reply_text(
                    f"ID {custom_id} уже занят другим клиентом. Пожалуйста, выберите другой ID."
                )
                return
            
            # Сохраняем указанный ID
            cursor.execute('UPDATE clients SET custom_id = ? WHERE user_id = ?', (custom_id, client_id))
            db_connection.commit()
        else:
            # Генерируем ID автоматически
            custom_id = generate_client_id(db_connection, client_id, manager_id)
        
        # Обновляем заголовок треда с новым custom_id
        # Получаем имя клиента
        client_name = f"{client_data[1] or ''}"  # first_name
        if client_data[2]:  # last_name
            client_name += f" {client_data[2]}"
        if client_data[3]:  # username
            client_name += f" (@{client_data[3]})"
        
        new_thread_title = f"{custom_id} | {client_name} | тред {thread_id}"
        
        await edit_thread_title(client, thread_id, new_thread_title)
        
        await message.reply_text(
            f"✅ Клиенту назначен ID: **{custom_id}**\n\n"
            f"Теперь вы можете отвечать клиенту командой:\n`/#{custom_id} текст ответа`\n\n"
            f"Вы назначены ответственным за этого клиента."
        )
        
        logger.info(f"Клиенту {client_id} (тред {thread_id}) назначен ID {custom_id}, ответственный менеджер: {manager_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при назначении ID клиенту: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Обновленная команда для получения информации о треде
@business.on_message(filters.command("wtt") & filters.chat(SUPPORT_GROUP_ID))
async def handle_what_thread_this_command(client, message):
    try:
        # Извлекаем thread_id из сообщения - обходим проблему с message_thread_id
        thread_id = None
        
        # Проверяем, что это сообщение в теме форума
        if hasattr(message, 'message_thread_id') and message.message_thread_id:
            thread_id = message.message_thread_id
        else:
            # Если не нашли message_thread_id, пробуем извлечь из reply_to_message_id
            thread_id = message.id
            
            # Пробуем получить информацию о теме
            try:
                # Получаем информацию о сообщении
                peer = await client.resolve_peer(SUPPORT_GROUP_ID)
                for attempt in range(2):  # Делаем несколько попыток с разными подходами
                    try:
                        if attempt == 0:
                            # Попытка 1: Смотрим на родительское сообщение
                            if message.reply_to_message:
                                thread_id = message.reply_to_message.id  # Используем ID сообщения, на которое отвечаем
                        else:
                            # Попытка 2: Используем текущий топик
                            thread_id = message.id  # Используем ID текущего сообщения как возможный ID треда
                            
                        # Проверяем тред
                        cursor = db_connection.cursor()
                        
                        # Проверяем в таблице клиентов
                        cursor.execute('SELECT user_id FROM clients WHERE thread_id = ?', (thread_id,))
                        if cursor.fetchone():
                            break  # Нашли, выходим из цикла
                            
                        # Проверяем в таблице групп
                        cursor.execute('SELECT group_id FROM group_threads WHERE thread_id = ?', (thread_id,))
                        if cursor.fetchone():
                            break  # Нашли, выходим из цикла
                    except Exception as e:
                        logger.error(f"Ошибка при попытке {attempt} определения thread_id: {e}")
                        if attempt == 0:
                            continue  # Переходим к следующей попытке
                        else:
                            raise  # Последняя попытка, поднимаем ошибку
            except Exception as e:
                logger.error(f"Не удалось определить thread_id: {e}")
                await message.reply_text("Не удалось определить ID треда. Используйте эту команду в треде клиента или в ответ на сообщение в треде.")
                return
        
        # Теперь у нас должен быть thread_id
        if not thread_id:
            await message.reply_text(
                "Не удалось определить ID треда. Убедитесь, что вы используете команду в треде клиента."
            )
            return
        
        logger.info(f"Определен thread_id: {thread_id} для команды /wtt")
        
        # Получаем информацию о клиенте
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            # Проверяем, может это групповой тред
            cursor = db_connection.cursor()
            cursor.execute('SELECT group_id, group_title FROM group_threads WHERE thread_id = ?', (thread_id,))
            group_data = cursor.fetchone()
            
            if group_data:
                group_id, group_title = group_data
                
                # Получаем ответственного менеджера
                cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
                manager_data = cursor.fetchone()
                duty_manager = f"@{manager_data[0]}" if manager_data and manager_data[0] else "Не назначен"
                
                await message.reply_text(
                    f"📊 **Информация о треде #{thread_id}**\n\n"
                    f"**Тип**: Группа\n"
                    f"**Название**: {group_title}\n"
                    f"**ID группы**: `{group_id}`\n"
                    f"**Ответственный**: {duty_manager}\n\n"
                    f"Для ответа используйте команду: `/{thread_id} текст ответа`"
                )
                return
            
            await message.reply_text(
                f"Не удалось найти информацию о треде #{thread_id}."
            )
            return
        
        # Распаковываем данные клиента
        # Проверяем, есть ли custom_id в данных клиента
        has_custom_id = False
        if len(client_data) >= 9:  # Если у нас есть колонка custom_id
            user_id, first_name, last_name, username, first_contact, last_contact, message_count, client_thread_id, custom_id = client_data
            has_custom_id = True
        else:
            user_id, first_name, last_name, username, first_contact, last_contact, message_count, client_thread_id = client_data
            custom_id = None
        
        # Формируем имя клиента
        client_name = f"{first_name or ''}"
        if last_name:
            client_name += f" {last_name}"
        
        # Получаем ответственного менеджера
        cursor = db_connection.cursor()
        cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
        manager_data = cursor.fetchone()
        duty_manager = f"@{manager_data[0]}" if manager_data and manager_data[0] else "Не назначен"
        
        # Получаем статистику по сообщениям
        cursor.execute('SELECT COUNT(*) FROM messages WHERE user_id = ? AND is_from_user = 1', (user_id,))
        client_messages = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM messages WHERE user_id = ? AND is_from_user = 0', (user_id,))
        manager_messages = cursor.fetchone()[0]
        
        # Формируем информацию о треде
        thread_info = f"📊 **Информация о треде #{thread_id}**\n\n"
        thread_info += f"**Клиент**: {client_name}"
        if username:
            thread_info += f" (@{username})"
        thread_info += f"\n**ID пользователя**: `{user_id}`"
        
        if has_custom_id and custom_id:
            thread_info += f"\n**ID клиента**: `{custom_id}`"
        
        thread_info += f"\n**Первое обращение**: {first_contact.strftime('%d.%m.%Y %H:%M') if isinstance(first_contact, datetime.datetime) else 'Неизвестно'}"
        thread_info += f"\n**Последнее обращение**: {last_contact.strftime('%d.%m.%Y %H:%M') if isinstance(last_contact, datetime.datetime) else 'Неизвестно'}"
        thread_info += f"\n**Сообщений от клиента**: {client_messages}"
        thread_info += f"\n**Ответов от поддержки**: {manager_messages}"
        thread_info += f"\n**Ответственный менеджер**: {duty_manager}"
        
        thread_info += f"\n\n**Для ответа используйте**:"
        if has_custom_id and custom_id:
            thread_info += f"\n`/#{custom_id} текст ответа` (по ID клиента)"
        else:
            thread_info += f"\n`/set_id {thread_id} [ID]` чтобы назначить ID клиенту"
        thread_info += f"\n`/{thread_id} текст ответа` (по номеру треда)"
        
        await message.reply_text(thread_info)
        
    except Exception as e:
        logger.error(f"Ошибка при получении информации о треде: {e}")
        import traceback
        logger.error(f"Трассировка: {traceback.format_exc()}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Обновленный обработчик для кнопок подтверждения
@business.on_callback_query(filters.regex(r"^reply_"))
async def handle_reply_confirmation(client, callback_query):
    try:
        data_parts = callback_query.data.split('_')
        if len(data_parts) < 3:
            await callback_query.answer("Некорректные данные", show_alert=True)
            return
        
        custom_id = data_parts[1]
        original_message_id = int(data_parts[2])
        
        manager_id = callback_query.from_user.id
        
        # Получаем оригинальное сообщение
        try:
            original_message = await client.get_messages(
                chat_id=callback_query.message.chat.id,
                message_ids=original_message_id
            )
            
            if not original_message or not original_message.text:
                await callback_query.answer("Не удалось найти исходное сообщение", show_alert=True)
                return
            
            command_text = original_message.text.strip()
            if " " in command_text:
                reply_text = command_text.split(" ", 1)[1]
            else:
                await callback_query.answer("В исходном сообщении нет текста для ответа", show_alert=True)
                return
            
            # Найдем клиента по custom_id
            cursor = db_connection.cursor()
            cursor.execute('SELECT thread_id, user_id FROM clients WHERE custom_id = ?', (custom_id,))
            result = cursor.fetchone()
            
            if not result:
                await callback_query.answer("Клиент не найден", show_alert=True)
                return
            
            thread_id, client_id = result
            
            # Отправляем ответ клиенту
            success = await send_manager_reply_to_client(client, manager_id, client_id, reply_text)
            
            if success:
                update_manager_reply_time(db_connection, thread_id)
                await mark_thread_urgent(client, thread_id, is_urgent=False)
                
                # Обновляем сообщение с подтверждением и показываем новый формат команды
                await callback_query.message.edit_text(
                    f"✅ Ответ успешно отправлен клиенту с ID {custom_id}.\n\n"
                    f"Для дальнейших ответов используйте формат:\n`/#{custom_id} текст ответа`"
                )
                
                # Сохраняем информацию о первом ответе
                manager = get_manager(db_connection, manager_id)
                if manager and manager[7]:  # username в индексе 7
                    assign_duty_manager(db_connection, thread_id, manager[7], manager_id)
                
                if is_first_reply(db_connection, thread_id, manager_id):
                    save_first_reply(db_connection, thread_id, client_id, manager_id)
            else:
                await callback_query.answer("Не удалось отправить ответ клиенту", show_alert=True)
        except Exception as e:
            logger.error(f"Ошибка при обработке подтверждения ответа: {e}")
            await callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)
    except Exception as e:
        logger.error(f"Общая ошибка в обработчике колбека: {e}")
        await callback_query.answer("Произошла ошибка", show_alert=True) 
        
# Обработчик команды для отправки карточки менеджера клиенту
@business.on_message(filters.command("card") & filters.chat(SUPPORT_GROUP_ID))
async def handle_send_card(client, message):
    try:
        manager_id = message.from_user.id
        
        # Парсим команду: /card {thread_id}
        command_text = message.text.strip()
        
        # Проверяем формат команды
        parts = command_text.split()
        if len(parts) != 2:
            await message.reply_text(
                "Неверный формат команды. Используйте: /card {thread_id}"
            )
            return
        
        # Получаем thread_id
        try:
            thread_id = int(parts[1])
        except ValueError:
            await message.reply_text("ID треда должен быть числом.")
            return
        
        # Проверяем, что менеджер авторизован
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Получаем клиента по thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text(
                f"Не удалось найти клиента, связанного с темой {thread_id}."
            )
            return
        
        # Получаем ID клиента
        client_id = client_data[0]  # Первый элемент - user_id
        
        # Отправляем карточку менеджера
        logger.info(f"Отправка карточки менеджера {manager_id} клиенту {client_id} по запросу")
        card_sent = await send_manager_card_to_client(client, manager_id, client_id)
        
        if card_sent:
            await message.reply_text(f"✅ Карточка успешно отправлена клиенту.")
            # Сохраняем первый ответ (если еще не сохранен)
            if is_first_reply(db_connection, thread_id, manager_id):
                save_first_reply(db_connection, thread_id, client_id, manager_id)
                
            # НОВЫЙ КОД: Автоматически назначаем ответственного менеджера
            _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
            if username:
                assign_duty_manager(db_connection, thread_id, username, manager_id)
                await message.reply_text(f"✅ Вы назначены ответственным за клиента (тред #{thread_id}).")
                logger.info(f"Менеджер @{username} автоматически назначен ответственным за клиента {client_id} (тред {thread_id})")
        else:
            await message.reply_text("❌ Не удалось отправить карточку клиенту.")
            
    except Exception as e:
        logger.error(f"Ошибка при отправке карточки: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Функция для изменения заголовка треда
async def edit_thread_title(client, thread_id, new_title):
    try:
        logger.info(f"Попытка изменить заголовок треда {thread_id} на '{new_title}'")
        
        # Получаем peer объект группы
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        
        # Вызываем API метод для изменения заголовка темы
        result = await client.invoke(
            pyrogram.raw.functions.channels.EditForumTopic(
                channel=peer,
                topic_id=thread_id,
                title=new_title
            )
        )
        
        logger.info(f"Заголовок треда {thread_id} успешно изменен на '{new_title}'")
        return True
    except Exception as e:
        logger.error(f"Ошибка при изменении заголовка треда: {e}")
        return False
@business.on_message(filters.command("list_topics") & filters.private)
async def handle_list_topics(client, message):
    logger.info(f"Получена команда /list_topics от пользователя {message.from_user.id}")
    
    try:
        # Проверяем группу
        is_forum = await check_forum_capabilities(client)
        
        if not is_forum:
            await message.reply_text("❌ Группа не настроена как форум. Пожалуйста, настройте группу как форум.")
            return
        
        # Попытка получить список тем через низкоуровневый API
        await message.reply_text("Пытаюсь получить список тем в группе...")
        
        try:
            # Получаем темы с использованием правильного метода API
            peer = await client.resolve_peer(SUPPORT_GROUP_ID)
            
            result = await client.invoke(
                pyrogram.raw.functions.channels.GetForumTopics(
                    channel=peer,
                    offset_date=0,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                )
            )
            
            if result and hasattr(result, "topics") and result.topics:
                topics_info = "Список тем в группе:\n\n"
                for i, topic in enumerate(result.topics, 1):
                    topics_info += f"{i}. ID: {topic.id}, Название: {topic.title}\n"
                
                await message.reply_text(topics_info)
                logger.info(f"Найдено {len(result.topics)} тем")
            else:
                await message.reply_text("В группе нет тем или не удалось получить список тем.")
                logger.info("Темы не найдены или недоступны")
        except Exception as e:
            logger.error(f"Ошибка при получении списка тем: {e}")
            await message.reply_text(f"Ошибка при получении списка тем: {e}")
    except Exception as e:
        logger.error(f"Общая ошибка: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Обработчик команды для получения списка всех активных тредов
@business.on_message(filters.command("threads") & filters.chat(SUPPORT_GROUP_ID))
async def handle_list_threads(client, message):
    try:
        logger.info(f"Получена команда /threads от пользователя {message.from_user.id}")
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, message.from_user.id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Получаем список всех активных тредов
        threads = get_all_active_threads(db_connection)
        
        if not threads:
            await message.reply_text("На данный момент нет активных тредов.")
            return
        
        # Формируем сообщение со списком тредов
        response = "📋 **Список активных тредов**:\n\n"
        
        for thread in threads:
            thread_id, user_id, first_name, last_name, username, assigned_manager, last_message_time = thread
            
            # Формируем имя клиента
            client_name = f"{first_name or ''}"
            if last_name:
                client_name += f" {last_name}"
            if username:
                client_name += f" (@{username})"
            
            # Форматируем дату последнего сообщения
            if last_message_time:
                try:
                    # Преобразуем строку времени в datetime объект
                    last_message_date = datetime.datetime.strptime(last_message_time, '%Y-%m-%d %H:%M:%S.%f')
                    # Форматируем дату
                    formatted_date = last_message_date.strftime('%d.%m.%Y %H:%M')
                except (ValueError, TypeError):
                    formatted_date = "Неизвестно"
            else:
                formatted_date = "Нет сообщений"
            
            # Добавляем информацию о треде в ответ
            response += f"🔹 **Тред #{thread_id}** - {client_name} (ID: {user_id})\n"
            response += f"   📅 Последнее сообщение: {formatted_date}\n"
            response += f"   👨‍💼 Ответственный: {assigned_manager or 'Не назначен'}\n\n"
        
        # Отправляем сообщение, учитывая ограничение Telegram на длину сообщения
        if len(response) <= 4096:
            await message.reply_text(response)
        else:
            # Разбиваем большое сообщение на части
            chunks = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    # Первый фрагмент с заголовком
                    await message.reply_text(chunk)
                else:
                    # Последующие фрагменты
                    await client.send_message(
                        chat_id=message.chat.id,
                        text=f"(Продолжение списка тредов...)\n\n{chunk}"
                    )
        
        logger.info(f"Список тредов отправлен, всего {len(threads)} тредов")
    except Exception as e:
        logger.error(f"Ошибка при получении списка тредов: {e}")
        await message.reply_text(f"Произошла ошибка при получении списка тредов: {e}")

# Обработчик команды /ok для сброса текущего уведомления
@business.on_message(filters.command("ok") & filters.chat(SUPPORT_GROUP_ID))
async def handle_ok_command(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Парсим команду: /ok {thread_id}
        command_text = message.text.strip()
        parts = command_text.split()
        
        if len(parts) != 2:
            await message.reply_text(
                "Неверный формат команды. Используйте: /ok {thread_id}"
            )
            return
        
        try:
            thread_id = int(parts[1])
        except ValueError:
            await message.reply_text("ID треда должен быть числом.")
            return
        
        # Проверяем, существует ли тред (в clients или в group_threads)
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id FROM clients WHERE thread_id = ? UNION SELECT thread_id FROM group_threads WHERE thread_id = ?', 
                      (thread_id, thread_id))
        thread_exists = cursor.fetchone()
        
        if not thread_exists:
            await message.reply_text(
                f"Тред #{thread_id} не найден."
            )
            return
        
        # Сбрасываем текущее уведомление для треда
        reset_thread_notification(db_connection, thread_id)
        
        # Меняем иконку треда на обычную
        try:
            await mark_thread_urgent(client, thread_id, is_urgent=False)
        except Exception as e:
            logger.error(f"Ошибка при изменении иконки треда {thread_id}: {e}")
        
        await message.reply_text(
            f"✅ Уведомление для треда #{thread_id} сброшено, тред отмечен как обработанный."
        )
        logger.info(f"Уведомление для треда {thread_id} сброшено менеджером {manager_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /ok: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

@business.on_message(filters.command("group_info") & filters.chat(SUPPORT_GROUP_ID))
async def handle_group_info(client, message):
    try:
        # Парсим команду: /group_info {thread_id или group_id}
        command_text = message.text.strip()
        parts = command_text.split()
        
        if len(parts) != 2:
            await message.reply_text(
                "Неверный формат команды. Используйте: /group_info {thread_id или group_id}"
            )
            return
        
        try:
            id_param = int(parts[1])
        except ValueError:
            await message.reply_text("ID должен быть числом.")
            return
        
        cursor = db_connection.cursor()
        
        # Проверяем, это thread_id или group_id
        cursor.execute('SELECT group_id, group_title, thread_id, created_at FROM group_threads WHERE thread_id = ? OR group_id = ?', 
                      (id_param, id_param))
        group_data = cursor.fetchone()
        
        if not group_data:
            await message.reply_text(
                f"Группа с ID или Thread ID {id_param} не найдена в базе данных."
            )
            return
        
        group_id, group_title, thread_id, created_at = group_data
        
        # Получаем дополнительную информацию
        cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
        manager_data = cursor.fetchone()
        duty_manager = manager_data[0] if manager_data else "Не назначен"
        
        # Формируем информацию о группе
        info_text = f"📋 **Информация о группе**\n\n"
        info_text += f"📝 **Название**: {group_title}\n"
        info_text += f"🆔 **ID группы**: `{group_id}`\n"
        info_text += f"🧵 **ID треда**: {thread_id}\n"
        info_text += f"👨‍💼 **Ответственный**: @{duty_manager}\n"
        info_text += f"📅 **Добавлена**: {created_at}\n\n"
        
        # Добавляем команды для взаимодействия
        info_text += f"💬 **Ответить в группу**: `/{thread_id} текст сообщения`\n"
        info_text += f"✅ **Отметить как обработанное**: `/ok {thread_id}`\n"
        
        await message.reply_text(info_text)
        
    except Exception as e:
        logger.error(f"Ошибка при получении информации о группе: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
# Функция для отправки предложения о назначении ID новому клиенту
async def send_id_assignment_proposal(client, thread_id, client_name):
    try:
        if thread_id:
            # Отправляем предложение о назначении ID
            proposal_text = f"""
📝 **Новый клиент добавлен**

Для этого клиента можно:
- Назначить уникальный ID (используйте `/set_id {thread_id} [ID]`)
- Оставить для коммуникации номер треда (отправьте команду `/none` в ответ на это сообщение)

👤 Клиент: {client_name}
🧵 Тред: #{thread_id}

⚠️ Важно: Первый ответивший менеджер автоматически назначается ответственным.
          Для ответа клиенту используйте формат: `/{thread_id} текст ответа`
            """
            
            # Отправляем предложение в группу поддержки
            sent_message = await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=proposal_text,
                reply_to_message_id=thread_id,
                reply_markup=pyrogram.types.InlineKeyboardMarkup([
                    [
                        pyrogram.types.InlineKeyboardButton("Авто ID", callback_data=f"autoid_{thread_id}"),
                        pyrogram.types.InlineKeyboardButton("Оставить тред", callback_data=f"usethreadid_{thread_id}")
                    ]
                ])
            )
            logger.info(f"Отправлено предложение о назначении ID для треда {thread_id}")
            return True
    except Exception as e:
        logger.error(f"Ошибка при отправке предложения о назначении ID: {e}")
        return False

# Обработчик нажатий на кнопки автоназначения ID
@business.on_callback_query(filters.regex(r"^autoid_"))
async def handle_autoid_button(client, callback_query):
    try:
        data_parts = callback_query.data.split("_")
        if len(data_parts) != 2:
            await callback_query.answer("Некорректные данные", show_alert=True)
            return
        
        thread_id = int(data_parts[1])
        manager_id = callback_query.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await callback_query.answer(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации.",
                show_alert=True
            )
            return
        
        # Получаем клиента по thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await callback_query.answer(
                f"Не удалось найти клиента, связанного с темой {thread_id}.",
                show_alert=True
            )
            return
        
        # Получаем ID клиента
        client_id = client_data[0]  # Первый элемент - user_id
        
        # Генерируем ID автоматически
        custom_id = generate_client_id(db_connection, client_id, manager_id)
        
        # Обновляем заголовок треда с новым custom_id
        # Получаем имя клиента
        client_name = f"{client_data[1] or ''}"  # first_name
        if client_data[2]:  # last_name
            client_name += f" {client_data[2]}"
        if client_data[3]:  # username
            client_name += f" (@{client_data[3]})"
        
        new_thread_title = f"{custom_id} | {client_name} | тред {thread_id}"
        
        await edit_thread_title(client, thread_id, new_thread_title)
        
        # Назначаем менеджера ответственным
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
        if username:
            assign_duty_manager(db_connection, thread_id, username, manager_id)
        
        # Обновляем сообщение
        await callback_query.message.edit_text(
            f"✅ Клиенту назначен ID: **{custom_id}**\n\n"
            f"Теперь вы можете отвечать клиенту командой:\n`/#{custom_id} текст ответа`\n\n"
            f"Менеджер @{username or 'Unknown'} назначен ответственным за этого клиента.",
            reply_markup=None
        )
        
        await callback_query.answer("ID клиента успешно сгенерирован", show_alert=True)
        logger.info(f"Клиенту {client_id} (тред {thread_id}) автоматически назначен ID {custom_id}, ответственный менеджер: {manager_id}")
        
    except Exception as e:
        logger.error(f"Ошибка при автоназначении ID: {e}")
        await callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)

# Обработчик нажатий на кнопку "Оставить тред"
@business.on_callback_query(filters.regex(r"^usethreadid_"))
async def handle_usethreadid_button(client, callback_query):
    try:
        data_parts = callback_query.data.split("_")
        if len(data_parts) != 2:
            await callback_query.answer("Некорректные данные", show_alert=True)
            return
        
        thread_id = int(data_parts[1])
        manager_id = callback_query.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await callback_query.answer(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации.",
                show_alert=True
            )
            return
        
        # Получаем клиента по thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await callback_query.answer(
                f"Не удалось найти клиента, связанного с темой {thread_id}.",
                show_alert=True
            )
            return
        
        client_id = client_data[0]  # user_id
        
        # Назначаем менеджера ответственным
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
        if username:
            assign_duty_manager(db_connection, thread_id, username, manager_id)
        
        # Обновляем сообщение
        await callback_query.message.edit_text(
            f"✅ Выбрано использование номера треда для коммуникации.\n\n"
            f"Вы можете отвечать клиенту командой:\n`/{thread_id} текст ответа`\n\n"
            f"Менеджер @{username or 'Unknown'} назначен ответственным за этого клиента.",
            reply_markup=None
        )
        
        await callback_query.answer("Будет использоваться номер треда", show_alert=True)
        logger.info(f"Менеджер {manager_id} выбрал использование thread_id для клиента {client_id} (тред {thread_id})")
    
    except Exception as e:
        logger.error(f"Ошибка при выборе использования thread_id: {e}")
        await callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)

# Обработчик команды /none
@business.on_message(filters.command("none") & filters.chat(SUPPORT_GROUP_ID))
async def handle_none_command(client, message):
    try:
        manager_id = message.from_user.id
        
        # Проверяем, авторизован ли менеджер
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Проверяем, является ли это ответом на сообщение
        if not message.reply_to_message:
            await message.reply_text(
                "Пожалуйста, используйте эту команду в ответ на предложение о назначении ID."
            )
            return
            
        # Извлекаем thread_id из текста сообщения, на которое отвечают
        reply_text = message.reply_to_message.text or ""
        thread_matches = re.findall(r'Тред: #(\d+)', reply_text)
        
        if not thread_matches:
            await message.reply_text(
                "Не удалось определить ID треда из сообщения. Используйте эту команду только в ответ на предложение о назначении ID."
            )
            return
            
        thread_id = int(thread_matches[0])
        
        # Получаем информацию о клиенте
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text(f"Не удалось найти клиента для треда {thread_id}.")
            return
            
        client_id = client_data[0]  # user_id
        
        # Назначаем менеджера ответственным
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager
        if username:
            assign_duty_manager(db_connection, thread_id, username, manager_id)
        
        # Отправляем подтверждение
        await message.reply_text(
            f"✅ Выбрано использование номера треда для коммуникации.\n\n"
            f"Вы можете отвечать клиенту командой:\n`/{thread_id} текст ответа`\n\n"
            f"Вы назначены ответственным за этого клиента."
        )
        
        logger.info(f"Менеджер {manager_id} выбрал использование thread_id для клиента {client_id} (тред {thread_id})")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /none: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
  
# Новая команда для просмотра ответственных менеджеров
@business.on_message(filters.command("duties") & filters.chat(SUPPORT_GROUP_ID))
async def handle_duties_command(client, message):
    try:
        # Проверяем, авторизован ли менеджер
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Пожалуйста, используйте команду /auth для авторизации."
            )
            return
        
        # Получаем список всех тредов с ответственными менеджерами
        cursor = db_connection.cursor()
        cursor.execute('''
        SELECT 
            d.thread_id, 
            d.manager_username, 
            c.user_id, 
            c.first_name,
            c.last_name,
            c.username,
            ts.last_client_message,
            ts.last_manager_reply,
            g.group_title
        FROM duty_managers d
        LEFT JOIN clients c ON d.thread_id = c.thread_id
        LEFT JOIN thread_status ts ON d.thread_id = ts.thread_id
        LEFT JOIN group_threads g ON d.thread_id = g.thread_id
        ORDER BY 
            CASE 
                WHEN ts.last_manager_reply IS NULL OR ts.last_client_message > ts.last_manager_reply THEN 0
                ELSE 1
            END,
            ts.last_client_message DESC
        ''')
        
        threads = cursor.fetchall()
        
        if not threads:
            await message.reply_text("Нет тредов с назначенными ответственными менеджерами.")
            return
        
        response = "📋 **Список тредов с ответственными менеджерами**:\n\n"
        
        for thread in threads:
            thread_id, manager_username, user_id, first_name, last_name, username, last_client_msg, last_manager_reply, group_title = thread
            
            # Определяем тип треда (клиент или группа)
            if group_title:
                client_name = f"Группа: {group_title}"
                thread_type = "группа"
            else:
                client_name = f"{first_name or ''}"
                if last_name:
                    client_name += f" {last_name}"
                if username:
                    client_name += f" (@{username})"
                thread_type = "клиент"
            
            # Определяем статус (отвечен/не отвечен)
            if last_manager_reply is None or (last_client_msg and last_client_msg > last_manager_reply):
                status = "🔴 Не отвечен"
            else:
                status = "✅ Отвечен"
            
            # Форматируем время последнего сообщения
            time_str = "неизвестно"
            if last_client_msg:
                if isinstance(last_client_msg, str):
                    last_client_msg = datetime.datetime.strptime(last_client_msg, '%Y-%m-%d %H:%M:%S.%f')
                time_str = last_client_msg.strftime('%d.%m.%Y %H:%M')
            
            # Добавляем строку в ответ
            response += f"**{thread_id}** - {client_name} ({thread_type})\n"
            response += f"Ответственный: @{manager_username}\n"
            response += f"Статус: {status}\n"
            response += f"Последнее сообщение: {time_str}\n\n"
        
        # Отправляем ответ
        if len(response) <= 4096:
            await message.reply_text(response)
        else:
            # Если сообщение слишком длинное, разбиваем на части
            chunks = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await message.reply_text(chunk)
                else:
                    await client.send_message(
                        chat_id=message.chat.id,
                        text=f"(Продолжение списка тредов {i+1}/{len(chunks)}):\n\n{chunk}"
                    )
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка ответственных: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
        
# Обработчик команды /upload
@business.on_message(filters.command("upload") & filters.chat(SUPPORT_GROUP_ID))
async def handle_upload_command(client, message):
    try:
        # Проверяем, что есть файл для загрузки
        if not (message.document or message.photo or message.video or message.audio or message.voice):
            await message.reply_text("❌ Нет файла для загрузки. Пожалуйста, прикрепите файл к команде.")
            return
        
        # Получаем название файла из команды
        command_parts = message.text.split(maxsplit=1)
        
        if len(command_parts) < 2:
            await message.reply_text("❌ Пожалуйста, укажите название файла: /upload [название файла]")
            return
        
        file_name = command_parts[1].strip()
        
        # Загружаем файл в хранилище
        success, result = await upload_file_to_storage(client, message, file_name)
        
        # Обработка результата происходит внутри функции upload_file_to_storage
        
    except Exception as e:
        logger.error(f"Ошибка при обработке команды upload: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")

# Обработчик команды /replace
@business.on_message(filters.command("replace") & filters.chat(SUPPORT_GROUP_ID))
async def handle_replace_command(client, message):
    try:
        # Проверяем, что есть файл для замены
        if not (message.document or message.photo or message.video or message.audio or message.voice):
            await message.reply_text("❌ Нет файла для замены. Пожалуйста, прикрепите файл к команде.")
            return
        
        # Получаем название файла из команды
        command_parts = message.text.split(maxsplit=1)
        
        if len(command_parts) < 2:
            await message.reply_text("❌ Пожалуйста, укажите название файла для замены: /replace [название файла]")
            return
        
        file_name = command_parts[1].strip()
        
        # Заменяем файл в хранилище
        success, result = await replace_file_in_storage(client, message, file_name)
        
        # Обработка результата происходит внутри функции replace_file_in_storage
        
    except Exception as e:
        logger.error(f"Ошибка при обработке команды replace: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")

# Обработчик команды /files - вывод списка файлов
@business.on_message(filters.command("files") & filters.chat(SUPPORT_GROUP_ID))
async def handle_files_command(client, message):
    await get_files_list(client, message)

# Обработчик команды /send - отправка файла клиенту по thread_id
@business.on_message(filters.command("send") & filters.chat(SUPPORT_GROUP_ID))
async def handle_send_command(client, message):
    try:
        # Получаем аргументы команды
        command_parts = message.text.split()
        
        if len(command_parts) < 3:
            await message.reply_text("❌ Пожалуйста, укажите thread_id и название файла: /send [thread_id] [название файла]")
            return
        
        thread_id = int(command_parts[1])
        file_name = " ".join(command_parts[2:])
        
        # Отправляем файл клиенту
        await send_file_to_client(client, thread_id, file_name, message, is_thread_id=True)
        
    except ValueError:
        await message.reply_text("❌ Некорректный формат thread_id. Используйте число.")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды send: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")

# Обработчик команды /#client_id [file_name] - отправка файла по ID клиента
@business.on_message(filters.regex(r"^/#[A-Za-z0-9]+") & filters.chat(SUPPORT_GROUP_ID))
async def handle_send_by_client_id(client, message):
    try:
        # Извлекаем client_id из команды
        command_text = message.text.strip()
        parts = command_text.split(maxsplit=1)
        
        if len(parts) < 2:
            await message.reply_text("❌ Пожалуйста, укажите название файла: /#[client_id] [название файла]")
            return
        
        client_id = parts[0][2:]  # Убираем "/#"
        file_name = parts[1]
        
        # Отправляем файл клиенту
        await send_file_to_client(client, client_id, file_name, message, is_thread_id=False)
        
    except Exception as e:
        logger.error(f"Ошибка при обработке команды отправки по client_id: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")
                 
# функция планировщика проверок
async def schedule_checks():
    # Начальная задержка для полной инициализации клиента
    await asyncio.sleep(30)
    
    while True:
        try:
            logger.info("Запуск проверки неотвеченных сообщений...")
            await check_unanswered_messages(business)
            
            # Проверяем каждые N минут согласно настройке
            await asyncio.sleep(CHECK_INTERVAL * 60)
        except Exception as e:
            logger.error(f"Ошибка в планировщике проверок: {e}")
            # При ошибке делаем более короткую паузу
            await asyncio.sleep(60)

# Обработчик для тестового создания темы через низкоуровневый API
@business.on_message(filters.command("create_test_topic") & filters.private)
async def handle_create_test_topic(client, message):
    logger.info(f"Получена команда /create_test_topic от пользователя {message.from_user.id}")
    
    try:
        # Попытка создать тему через низкоуровневый API
        await message.reply_text("Пытаюсь создать тестовую тему в группе...")
        
        topic_title = f"Тестовая тема {random.randint(1000, 9999)}"
        
        try:
            # Используем правильный метод вызова API
            peer = await client.resolve_peer(SUPPORT_GROUP_ID)
            random_id = random.randint(1, 999999)
            
            result = await client.invoke(
                pyrogram.raw.functions.channels.CreateForumTopic(
                    channel=peer,
                    title=topic_title,
                    random_id=random_id,
                    icon_color=random.randint(0, 7)
                )
            )
            
            if result and hasattr(result, "updates"):
                thread_id = None
                # Ищем в updates
                for update in result.updates:
                    if hasattr(update, 'message') and hasattr(update.message, 'action'):
                        # Это сервисное сообщение о создании темы
                        if update.message.id:
                            thread_id = update.message.id
                            break
                
                if thread_id:
                    await message.reply_text(f"✅ Тема успешно создана!\nID: {thread_id}\nНазвание: {topic_title}")
                    logger.info(f"Создана тестовая тема с ID: {thread_id}")
                    
                    # Отправляем тестовое сообщение в тему
                    try:
                        await client.send_message(
                            chat_id=SUPPORT_GROUP_ID,
                            text=f"Это тестовое сообщение в теме '{topic_title}'",
                            reply_to_message_id=thread_id
                        )
                        await message.reply_text("✅ Тестовое сообщение отправлено в тему.")
                    except Exception as e:
                        logger.error(f"Ошибка при отправке сообщения в тему: {e}")
                        await message.reply_text(f"❌ Ошибка при отправке сообщения в тему: {e}")
                else:
                    await message.reply_text("❌ Не удалось извлечь ID темы из ответа API.")
                    logger.info(f"Ответ API: {result}")
            else:
                await message.reply_text("❌ Не удалось создать тему или получить информацию о созданной теме.")
                logger.info(f"Ответ API: {result}")
        except Exception as e:
            logger.error(f"Ошибка при создании темы: {e}")
            await message.reply_text(f"❌ Ошибка при создании темы: {e}")
    except Exception as e:
        logger.error(f"Общая ошибка: {e}")
        await message.reply_text(f"❌ Произошла ошибка: {e}")

# Обработчик команды помощи - обновленный список команд
@business.on_message(filters.command("help") & filters.chat(SUPPORT_GROUP_ID))
async def handle_help_command(client, message):
    try:
        logger.info(f"Получена команда /help от пользователя {message.from_user.id}")
        
        help_text = """
📋 **Доступные команды**:

⚙️ **Команды для работы с клиентами**:
- `/{ID_треда} [текст]` - Ответить клиенту по ID треда (например, /12345 Здравствуйте!)
- `/#{ID_клиента} [текст]` - Ответить клиенту по его уникальному ID (например, /#АМ2504001 Здравствуйте!)
- `/set_id {ID_треда} [желаемый_ID]` - Назначить или изменить ID клиента
- `/wtt` - Получить информацию о текущем треде (использовать в ответ на сообщение в треде)
- `/card {ID_треда}` - Отправить свою карточку клиенту
- `/ok {ID_треда}` - Сбросить уведомления для треда

⚙️ **Команды для управления**:
- `/auth [эмодзи], [Имя], [Должность], [4 цифры]` - Авторизоваться как менеджер
- `/onduty @username {ID_треда}` - Назначить ответственного менеджера
- `/duties` - Показать список тредов с ответственными менеджерами
- `/threads` - Показать список всех активных тредов
- `/find [запрос]` - Поиск клиентов по имени или ID
- `/group_info [ID_треда]` - Показать информацию о группе
- `/myinfo` - Просмотреть свою информацию в системе

📁 **Команды для работы с файлами**:
- `/upload [название_файла]` - Загрузить файл в хранилище (с вложением)
- `/replace [название_файла]` - Заменить существующий файл (с вложением)
- `/files` - Показать список файлов в хранилище
- `/send [ID_треда] [название_файла]` - Отправить файл клиенту по ID треда
- `/#[ID_клиента] [название_файла]` - Отправить файл клиенту по его ID

ℹ️ **О назначении ответственных менеджеров**:
- Первый ответивший менеджер автоматически назначается ответственным за клиента
- Ответственный менеджер получает уведомления о новых сообщениях от клиента
- Вы всегда можете изменить ответственного с помощью команды /onduty

ℹ️ **Подсказки**:
- Используйте `/#{ID_клиента}` для ответа клиенту по его уникальному ID
- При ответе клиенту через /{ID_треда} или /#{ID_клиента} вы автоматически назначаетесь ответственным
- Для отправки медиафайлов (фото, документов и т.д.) просто добавьте подпись с ID клиента: /{ID_треда} [текст]
- У каждого клиента может быть уникальный ID, который вы можете назначить командой /set_id
- Если клиент упомянет бизнес-аккаунт в группе, сообщение будет отмечено как требующее ответа
"""
        
        # Отправляем список команд
        await message.reply_text(help_text)
        logger.info("Отправлен список команд")
    except Exception as e:
        logger.error(f"Ошибка при отправке справки: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

# Обработчик нажатий на кнопки команд
@business.on_callback_query(filters.regex(r"^cmd_"))
async def handle_command_buttons(client, callback_query):
    try:
        command = callback_query.data.replace("cmd_", "")
        user_id = callback_query.from_user.id
        message = callback_query.message
        
        logger.info(f"Нажата кнопка команды {command} пользователем {user_id}")
        
        # В зависимости от выбранной команды, отправляем шаблонный текст
        if command == "otvet":
            template = "/otvet "
            await callback_query.answer("Введите текст ответа после команды /otvet")
        elif command == "auth":
            template = "/auth 👨‍💼, Иван Иванов, Менеджер поддержки, 1234"
            await callback_query.answer("Отредактируйте шаблон авторизации")
        elif command == "rename":
            template = "/rename_thread [ID_треда] [Новый заголовок]"
            await callback_query.answer("Укажите ID треда и новый заголовок")
        elif command == "list_topics":
            # Сразу выполняем команду list_topics
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text="/list_topics",
                reply_to_message_id=message.id if message else None
            )
            await callback_query.answer("Запрос списка тредов отправлен")
            return
        else:
            await callback_query.answer("Неизвестная команда")
            return
        
        # Отправляем шаблон команды как новое сообщение
        await client.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=template,
            reply_to_message_id=message.id if message else None
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке нажатия кнопки: {e}")
        await callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)

# Обработчик всех сообщений в личных чатах (кроме команд)
# Обновленный обработчик сообщений от клиентов
@business.on_message(filters.private & ~filters.command(["start", "check_forum", "list_topics", "create_test_topic", "help"]))
async def handle_private_messages(client, message):
    try:
        # Получаем информацию о сообщении
        user = message.from_user
        
        # Определяем тип сообщения и текст для записи в базу данных
        media_type = None
        if message.text:
            message_text = message.text
        elif message.photo:
            message_text = ""
            media_type = "ФОТО"
        elif message.video:
            message_text = ""
            media_type = "ВИДЕО"
        elif message.document:
            message_text = message.document.file_name if message.document.file_name else ""
            media_type = "ДОКУМЕНТ"
        elif message.audio:
            message_text = message.audio.title if message.audio.title else ""
            media_type = "АУДИО"
        elif message.voice:
            message_text = ""
            media_type = "ГОЛОСОВОЕ"
        elif message.sticker:
            message_text = ""
            media_type = "СТИКЕР"
        elif message.animation:
            message_text = ""
            media_type = "АНИМАЦИЯ"
        else:
            message_text = "[НЕИЗВЕСТНЫЙ ТИП СООБЩЕНИЯ]"
        
        logger.info(f"Получено сообщение от клиента: {user.id}, {user.first_name}")
        
        # Проверяем, новый ли это клиент
        cursor = db_connection.cursor()
        cursor.execute('SELECT message_count FROM clients WHERE user_id = ?', (user.id,))
        result = cursor.fetchone()
        is_new_client = not result or result[0] == 0
        
        # Сохраняем клиента в базу данных
        thread_id = save_client(db_connection, user)
        
        # Сохраняем сообщение в базу данных
        save_message(db_connection, user.id, message_text, is_from_user=True, media_type=media_type)
        
        # Проверяем, есть ли уже тред для клиента
        if thread_id:
            # Обновляем время последнего сообщения клиента
            update_client_message_time(db_connection, thread_id)
            # Если тред существует, пробуем переслать сообщение в него
            logger.info(f"Найден существующий тред {thread_id} для клиента {user.id}")
            
            # Пробуем переслать в существующий тред
            result = await forward_message_to_support(client, message, thread_id)
            
            # Если тред был удален, создаем новый
            if result == "TOPIC_DELETED":
                logger.info(f"Создаем новый тред для клиента {user.id} после удаления предыдущего")
                # Создаем новый тред
                new_thread_id = await create_thread_for_client(client, user)
                
                if new_thread_id:
                    # Обновляем thread_id в базе данных
                    update_client_thread(db_connection, user.id, new_thread_id)
                    
                    # Пересылаем сообщение в созданный тред
                    await forward_message_to_support(client, message, new_thread_id)
                    
                    # Формируем имя клиента для заголовка треда 
                    client_name = f"{user.first_name}"
                    if user.last_name:
                        client_name += f" {user.last_name}"
                    if user.username:
                        client_name += f" (@{user.username})"
                    
                    # Обновляем заголовок треда с номером треда
                    new_thread_title = f"{new_thread_id}: {client_name}"
                    await edit_thread_title(client, new_thread_id, new_thread_title)
                    
                    # Отправляем предложение о назначении ID
                    await send_id_assignment_proposal(client, new_thread_id, client_name)
                    
                    logger.info(f"Создан новый тред {new_thread_id} для клиента {user.id}")
                else:
                    # Если не удалось создать тред, сообщение уже переслано в группу
                    logger.warning("Не удалось создать новый тред")
        else:
            # Если треда нет, всегда пытаемся создать новый тред
            logger.info(f"Пытаемся создать новый тред для клиента {user.id}")
            
            # Пытаемся создать новый тред для клиента
            new_thread_id = await create_thread_for_client(client, user)
            
            if new_thread_id:
                # Обновляем thread_id в базе данных
                update_client_thread(db_connection, user.id, new_thread_id)
                
                # Пересылаем сообщение в созданный тред
                await forward_message_to_support(client, message, new_thread_id)
                
                # Формируем имя клиента для заголовка треда 
                client_name = f"{user.first_name}"
                if user.last_name:
                    client_name += f" {user.last_name}"
                if user.username:
                    client_name += f" (@{user.username})"
                
                # Отправляем предложение о назначении ID
                await send_id_assignment_proposal(client, new_thread_id, client_name)
                
                # Отправляем ответ клиенту при первом обращении
                if is_new_client:
                    # Отправляем более информативное сообщение
                    greeting_message = (
                        "Здравствуйте! Спасибо за ваше обращение.\n\n"
                        "Ваше сообщение получено и передано команде поддержки. "
                        "Скоро с вами свяжется персональный менеджер, который будет "
                        "вести ваш запрос от начала до полного решения вопроса.\n\n"
                        "Рабочее время поддержки: пн-пт с 10:00 до 19:00 МСК."
                    )
                    await message.reply_text(greeting_message)
                
                logger.info(f"Создан новый тред {new_thread_id} для клиента {user.id}")
            else:
                # Если не удалось создать тред, пересылаем сообщение в основную группу
                logger.warning("Не удалось создать тред, пересылаем сообщение без треда")
                await forward_message_to_support(client, message)
                
                # Отправляем ответ клиенту при первом обращении
                if is_new_client:
                    greeting_message = (
                        "Здравствуйте! Спасибо за ваше обращение.\n\n"
                        "Ваше сообщение получено и передано команде поддержки. "
                        "Скоро с вами свяжется персональный менеджер, который будет "
                        "вести ваш запрос от начала до полного решения вопроса.\n\n"
                        "Рабочее время поддержки: пн-пт с 10:00 до 19:00 МСК."
                    )
                    await message.reply_text(greeting_message)
                
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")
        # Отправляем ответ клиенту в случае ошибки только если действительно критическая ошибка
        if str(e).startswith("FLOOD_WAIT_") or "Too Many Requests" in str(e):
            logger.warning(f"Обнаружено ограничение Telegram API: {e}")
        else:
            await message.reply_text(
                "Извините, произошла ошибка при обработке вашего сообщения. Пожалуйста, попробуйте позже."
            )

# Функция для настройки команд бота
async def setup_bot_commands(client):
    try:
        logger.info("Настройка команд бота...")
        
        # Команды для менеджеров в группе
        group_commands = [
            pyrogram.types.BotCommand(
                command="otvet",
                description="Ответить клиенту"
            ),
            pyrogram.types.BotCommand(
                command="auth",
                description="Авторизоваться как менеджер"
            ),
            pyrogram.types.BotCommand(
                command="card",
                description="Отправить карточку клиенту"
            ),
            pyrogram.types.BotCommand(
                command="rename_thread",
                description="Переименовать тред"
            ),
            pyrogram.types.BotCommand(
                command="threads",
                description="Список активных тредов"
            ),
            pyrogram.types.BotCommand(
                command="myinfo",
                description="Просмотреть свою информацию"
            ),
            pyrogram.types.BotCommand(
                command="help",
                description="Список команд"
            ),
            pyrogram.types.BotCommand(
                command="ok",
                description="Отключить уведомления для треда"
            )
        ]
        
        # Команды для клиентов в личных чатах
        private_commands = [
            pyrogram.types.BotCommand(
                command="start",
                description="Начать общение с поддержкой"
            )
        ]
        
        # Устанавливаем команды для группы
        await client.set_bot_commands(
            commands=group_commands,
            scope=pyrogram.types.BotCommandScopeChat(SUPPORT_GROUP_ID)
        )
        
        # Устанавливаем команды для личных чатов
        await client.set_bot_commands(
            commands=private_commands,
            scope=pyrogram.types.BotCommandScopeDefault()
        )
        
        logger.info("Команды бота успешно настроены")
    except Exception as e:
        logger.error(f"Ошибка при настройке команд бота: {e}")

# После запуска бота добавьте проверку:
@business.on_message(filters.command("check_db") & filters.chat(SUPPORT_GROUP_ID))
async def handle_check_db(client, message):
    try:
        cursor = db_connection.cursor()
        
        # Проверяем существование таблицы thread_status
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='thread_status'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            await message.reply_text("⚠️ Таблица thread_status не существует!")
            return
            
        # Проверяем структуру таблицы
        cursor.execute("PRAGMA table_info(thread_status)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        expected_columns = ['thread_id', 'last_client_message', 'last_manager_reply', 
                          'is_notified', 'last_notification', 'notification_disabled']
        
        missing_columns = [col for col in expected_columns if col not in column_names]
        
        # Проверяем содержимое таблицы
        cursor.execute("SELECT COUNT(*) FROM thread_status")
        row_count = cursor.fetchone()[0]
        
        # Получаем актуальные записи в таблице
        cursor.execute("SELECT * FROM thread_status LIMIT 5")
        recent_entries = cursor.fetchall()
        
        # Формируем отчет
        report = "📊 **Статус базы данных**:\n\n"
        report += f"✅ Таблица thread_status существует: {table_exists}\n"
        
        if missing_columns:
            report += f"⚠️ Отсутствуют колонки: {', '.join(missing_columns)}\n"
        else:
            report += "✅ Структура таблицы корректна\n"
            
        report += f"📈 Всего записей в таблице: {row_count}\n\n"
        
        if recent_entries:
            report += "**Последние записи**:\n"
            for entry in recent_entries:
                report += f"- Тред {entry[0]}: последнее сообщение клиента {entry[1]}, последний ответ {entry[2]}\n"
        else:
            report += "⚠️ Таблица пуста\n"
            
        await message.reply_text(report)
        
    except Exception as e:
        logger.error(f"Ошибка при проверке БД: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

# Обработчик команды для настройки подсказок команд
# Запускаем клиент
if __name__ == "__main__":
    try:
        logger.info("Запуск бизнес-аккаунта Telegram...")
        logger.info(f"База данных клиентов настроена. Группа поддержки: {SUPPORT_GROUP_ID}")
        
        # Запускаем периодическую проверку неотвеченных сообщений
        business.loop.create_task(schedule_checks())
        
        # Запускаем периодическую очистку медиа-групп
        business.loop.create_task(cleanup_manager_media_groups())
        business.loop.create_task(cleanup_media_groups())
        
        # Запускаем мониторинг зависших медиа-групп от менеджеров
        business.loop.create_task(cleanup_processing_groups())
        
        business.run()
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {e}")