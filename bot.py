from pyrogram import Client, filters, ContinuePropagation
from pyrogram.handlers import RawUpdateHandler
from pyrogram.types import MessageEntity
from pyrogram.enums import MessageEntityType
import pyrogram
import sqlite3
import datetime
import logging
import random
import re
import os
import asyncio
import functools
import signal
import io
from PIL import Image

# Флаг для graceful shutdown
shutdown_requested = False


def escape_markdown(text):
    """Экранирует специальные символы markdown чтобы не ломать форматирование"""
    if not text:
        return ""
    # Экранируем символы markdown: * _ ` [ ]
    escape_chars = ['*', '_', '`', '[', ']']
    for char in escape_chars:
        text = text.replace(char, '\\' + char)
    return text


def escape_html(text):
    """Экранирует специальные символы HTML"""
    if not text:
        return ""
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

# Импорт конфигурации
from config import (
    API_ID, API_HASH, PHONE_NUMBER, SESSION_NAME,
    SUPPORT_GROUP_ID, DATABASE_NAME, FILES_CHANNEL_ID,
    EMOJI_PACK_NAME,
    URGENT_WAIT_TIME, FIRST_NOTIFICATION_DELAY,
    NOTIFICATION_INTERVAL, CHECK_INTERVAL,
    PARSE_MODE, WORKERS,
    MONITORING_CHAT_ID, HEARTBEAT_INTERVAL, MONITORING_ENABLED
)

# Импорт функций БД
import database as db

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Бизнес-аккаунт с дополнительными настройками
business = Client(
    SESSION_NAME,
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE_NUMBER,
    parse_mode=PARSE_MODE,
    workers=WORKERS
)

# Глобальное соединение с базой данных (из модуля database)
db_connection = db.setup_database()

# Глобальное хранилище для процесса авторизации менеджеров
manager_auth_state = {}

# Алиасы для обратной совместимости (используют функции из database.py)
save_client = db.save_client
update_client_thread = db.update_client_thread
get_client_by_thread = db.get_client_by_thread
save_message = db.save_message
save_manager = db.save_manager
get_manager = db.get_manager
update_manager_photo = db.update_manager_photo
get_manager_photo_channel_id = db.get_manager_photo_channel_id
get_manager_custom_emoji_id = db.get_manager_custom_emoji_id
update_manager_custom_emoji = db.update_manager_custom_emoji
save_first_reply = db.save_first_reply
is_first_reply = db.is_first_reply
get_managers_replied_to_client = db.get_managers_replied_to_client
get_all_active_threads = db.get_all_active_threads
assign_duty_manager = db.assign_duty_manager
get_duty_manager = db.get_duty_manager
update_client_message_time = db.update_client_message_time
update_manager_reply_time = db.update_manager_reply_time
set_custom_id = db.set_custom_id
get_custom_id = db.get_custom_id
get_thread_id_by_custom_id = db.get_thread_id_by_custom_id
get_custom_id_by_thread = db.get_custom_id_by_thread
save_message_mapping = db.save_message_mapping
get_group_message_id = db.get_group_message_id
get_client_message_id = db.get_client_message_id
update_message_text = db.update_message_text


# === ЦЕНТРАЛЬНЫЙ РОУТЕР КОМАНД ДЛЯ ГРУППЫ ПОДДЕРЖКИ ===
# Из-за бага в pyrofork, фильтры filters.command() и filters.regex() не работают
# ни для outgoing, ни для incoming сообщений в группах. Поэтому используем роутер для всех.

def _cmd_match(text, cmd):
    """Проверяет, начинается ли текст с команды /cmd"""
    if not text.startswith(f'/{cmd}'):
        return False
    # После команды должен быть конец строки или пробел
    rest = text[len(cmd) + 1:]
    return len(rest) == 0 or rest[0].isspace()

@business.on_message(filters.chat(SUPPORT_GROUP_ID), group=-10)
async def command_router(client, message):
    """Центральный роутер команд для группы поддержки (outgoing и incoming)"""
    # DEBUG: логируем ВСЕ сообщения
    logger.info(f"[ROUTER-DEBUG] Получено сообщение: text={message.text[:30] if message.text else None}, caption={message.caption[:30] if message.caption else None}, photo={bool(message.photo)}")

    if not message.text:
        raise ContinuePropagation

    text = message.text.strip()

    # Не логируем каждое сообщение, только команды
    if not text.startswith('/'):
        raise ContinuePropagation

    logger.info(f"[ROUTER] Команда: {text[:50]}, outgoing={message.outgoing}")

    # Роутинг команд (для outgoing и incoming)
    # Команды с реализацией в роутере
    if _cmd_match(text, 'team'):
        await _handle_team_command(client, message)
    elif _cmd_match(text, 'help'):
        await _handle_help_command(client, message)
    elif _cmd_match(text, 'ok'):
        await _handle_ok_command(client, message)
    # Команды с существующими обработчиками - вызываем напрямую
    elif _cmd_match(text, 'groups'):
        await handle_list_groups(client, message)
    elif _cmd_match(text, 'onduty'):
        await handle_assign_duty(client, message)
    elif _cmd_match(text, 'auth'):
        await handle_auth(client, message)
    elif _cmd_match(text, 'myinfo'):
        await handle_myinfo(client, message)
    elif _cmd_match(text, 'card'):
        await handle_send_card(client, message)
    elif _cmd_match(text, 'id'):
        await handle_set_custom_id(client, message)
    elif _cmd_match(text, 'company'):
        await handle_set_company(client, message)
    elif _cmd_match(text, 'upload'):
        await handle_upload_file(client, message)
    elif _cmd_match(text, 'file'):
        await handle_send_file(client, message)
    elif _cmd_match(text, 'files'):
        await handle_list_files(client, message)
    elif _cmd_match(text, 'temp'):
        await handle_send_template(client, message)
    elif _cmd_match(text, 'threads'):
        await handle_list_threads(client, message)
    elif _cmd_match(text, 'del') and message.reply_to_message:
        await handle_delete_message(client, message)
    elif _cmd_match(text, 'duties'):
        await handle_duties_command(client, message)
    elif _cmd_match(text, 'tier'):
        await handle_tier_command(client, message)
    elif _cmd_match(text, 'vacation'):
        await handle_vacation_command(client, message)
    elif _cmd_match(text, 'return'):
        await handle_return_command(client, message)
    elif _cmd_match(text, 'fire'):
        await handle_fire_command(client, message)
    elif _cmd_match(text, 'readme'):
        await handle_readme_command(client, message)
    elif _cmd_match(text, 'newclient'):
        await handle_newclient_command(client, message)
    else:
        # Пропускаем дальше для других обработчиков (например, ответы по thread_id)
        raise ContinuePropagation


# Реализации команд для роутера
async def _handle_team_command(client, message):
    """Показать карточки всех менеджеров"""
    try:
        if not message.from_user:
            return

        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы в системе. Используйте /auth для авторизации.")
            return

        from database import get_all_managers, get_manager_threads, is_on_vacation
        managers = get_all_managers(db_connection)

        if not managers:
            await message.reply_text("Нет зарегистрированных менеджеров.")
            return

        await message.reply_text(
            f"👥 **Команда менеджеров** ({len(managers)})\n\n"
            "Для увольнения: reply на карточку + `/fire @новый_менеджер`"
        )

        for mgr in managers:
            mgr_id, username, name = mgr
            threads = get_manager_threads(db_connection, username) if username else []
            on_vacation = is_on_vacation(db_connection, username) if username else False

            status = "🏖 В отпуске" if on_vacation else "✅ Активен"
            username_str = f"@{username}" if username else "без username"

            card_text = (
                f"**{name or 'Без имени'}**\n"
                f"ID: `{mgr_id}`\n"
                f"Username: {username_str}\n"
                f"Тредов: {len(threads)}\n"
                f"Статус: {status}"
            )

            await client.send_message(
                chat_id=message.chat.id,
                text=card_text,
                message_thread_id=message.message_thread_id
            )

        logger.info(f"Команда /team выполнена пользователем {manager_id}")

    except Exception as e:
        logger.error(f"Ошибка при показе команды /team: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


async def _handle_help_command(client, message):
    """Показать справку по командам"""
    try:
        logger.info(f"Получена команда /help от пользователя {message.from_user.id if message.from_user else 'N/A'}")

        help_text = """
📋 **Доступные команды**:

⚙️ **Ответы клиентам**:
- `/[thread_id] [текст]` - Ответить по номеру треда
- `/[ИмяКлиента] [текст]` - Ответить по ID клиента (русские буквы)
- `/id [thread_id] [Имя]` - Задать ID клиенту
- `/company [thread_id] [Компания]` - Задать название компании
- `/card [thread_id]` - Отправить визитку клиенту

⚙️ **Управление**:
- `/auth [эмодзи], [Имя], [Должность]` - Авторизоваться
- `/newclient @username` - Создать тред для нового клиента (только в General)
- `/onduty @username [ID_треда]` - Назначить ответственного
- `/ok [ID_треда]` - Сбросить уведомления для треда
- `/duties` - Список ответственных менеджеров
- `/threads` - Список активных тредов
- `/tier [ID_треда] [1|2|3]` - Установить тир клиента

👥 **Менеджеры**:
- `/vacation @менеджер @заместитель [tier-1 tier-2]` - Отпуск/больничный
- `/return @менеджер` - Вернуть из отпуска
- `/team` - Карточки всех менеджеров
- `/fire @новый` (reply на карточку) - Уволить менеджера

📊 **Информация**:
- `/myinfo` - Ваша информация в системе
- `/help` - Краткая справка

ℹ️ **Подсказки**:
- ID клиента: русские буквы и цифры (например: Иванов, Клиент123)
- При ответе через /{номер} или /ИмяКлиента менеджер становится ответственным
"""

        await message.reply_text(help_text)
        logger.info("Отправлен список команд")

    except Exception as e:
        logger.error(f"Ошибка при отправке справки: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


async def _handle_ok_command(client, message):
    """Сбросить уведомление для треда (роутер для outgoing)"""
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

        # Проверяем, существует ли тред
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id FROM clients WHERE thread_id = ? UNION SELECT thread_id FROM group_threads WHERE thread_id = ?',
                      (thread_id, thread_id))
        thread_exists = cursor.fetchone()

        if not thread_exists:
            await message.reply_text(f"Тред #{thread_id} не найден.")
            return

        # Сбрасываем уведомление
        reset_thread_notification(db_connection, thread_id)

        # Меняем иконку треда
        try:
            await mark_thread_urgent(client, thread_id, is_urgent=False)
        except Exception as e:
            logger.error(f"Ошибка при изменении иконки треда {thread_id}: {e}")

        await message.reply_text(f"✅ Уведомление для треда #{thread_id} сброшено.")
        logger.info(f"Уведомление для треда {thread_id} сброшено менеджером {manager_id} (outgoing)")

    except Exception as e:
        logger.error(f"Ошибка при обработке команды /ok (outgoing): {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# === ФУНКЦИИ ДЛЯ КАСТОМ ЭМОДЗИ ===

def format_signature_with_custom_emoji(message_text, name, position, custom_emoji_id=None, fallback_emoji="👤"):
    """
    Форматирует сообщение с подписью менеджера.
    Если есть custom_emoji_id - возвращает (text, entities) для отправки через entities.
    Если нет - возвращает (text, None) для отправки через markdown.
    """
    signature_text = f"{name}, {position}"

    if custom_emoji_id:
        # Формируем текст: сообщение + подпись с placeholder для эмодзи
        emoji_placeholder = "👤"  # Placeholder который заменится на кастом эмодзи
        full_text = f"{message_text}\n—\n{emoji_placeholder}  {signature_text}"

        # Позиция эмодзи в тексте (после \n—\n)
        emoji_offset = len(message_text) + 3  # +3 для "\n—\n"

        # Создаём entity для кастом эмодзи
        entity = MessageEntity(
            type=MessageEntityType.CUSTOM_EMOJI,
            offset=emoji_offset,
            length=2,  # длина placeholder эмодзи в UTF-16
            custom_emoji_id=custom_emoji_id
        )

        return full_text, [entity]
    else:
        # Без кастом эмодзи - обычный markdown
        full_text = f"{message_text}\n—\n`{fallback_emoji} {signature_text}`"
        return full_text, None


async def add_manager_emoji(client, photo_file_id, manager_name):
    """
    Создаёт кастом эмодзи из фото менеджера и добавляет в пак.
    Возвращает custom_emoji_id или None при ошибке.
    """
    try:
        from pyrogram.raw.functions.stickers import AddStickerToSet
        from pyrogram.raw.functions.channels import GetMessages
        from pyrogram.raw.types import InputStickerSetItem, InputDocument, InputStickerSetShortName, InputMessageID

        # Скачиваем фото
        photo_path = await client.download_media(photo_file_id, file_name="/tmp/manager_photo.jpg")

        # Открываем и обрабатываем изображение
        img = Image.open(photo_path)

        # Делаем квадратным (обрезаем по центру)
        min_side = min(img.size)
        left = (img.width - min_side) // 2
        top = (img.height - min_side) // 2
        img = img.crop((left, top, left + min_side, top + min_side))

        # Изменяем размер до 100x100
        img = img.resize((100, 100), Image.Resampling.LANCZOS)

        # Создаём круглую маску
        mask = Image.new('L', (100, 100), 0)
        from PIL import ImageDraw
        draw = ImageDraw.Draw(mask)
        draw.ellipse([0, 0, 100, 100], fill=255)

        # Применяем маску (делаем круглым)
        output = Image.new('RGBA', (100, 100), (0, 0, 0, 0))
        img = img.convert('RGBA')
        output.paste(img, (0, 0), mask)

        # Сохраняем
        emoji_path = "/tmp/manager_emoji.png"
        output.save(emoji_path, 'PNG')

        # Загружаем в канал как документ
        msg = await client.send_document(
            chat_id=FILES_CHANNEL_ID,
            document=emoji_path,
            caption=f"Emoji: {manager_name}"
        )

        # Получаем raw документ
        channel_peer = await client.resolve_peer(FILES_CHANNEL_ID)
        result = await client.invoke(
            GetMessages(
                channel=channel_peer,
                id=[InputMessageID(id=msg.id)]
            )
        )

        raw_doc = result.messages[0].media.document

        # Создаём InputDocument
        input_doc = InputDocument(
            id=raw_doc.id,
            access_hash=raw_doc.access_hash,
            file_reference=raw_doc.file_reference
        )

        # Создаём стикер
        sticker = InputStickerSetItem(
            document=input_doc,
            emoji="👤"
        )

        # Добавляем в существующий пак
        result = await client.invoke(
            AddStickerToSet(
                stickerset=InputStickerSetShortName(short_name=EMOJI_PACK_NAME),
                sticker=sticker
            )
        )

        # Получаем ID нового эмодзи (последний в списке)
        new_emoji_id = result.documents[-1].id

        logger.info(f"Создан кастом эмодзи для {manager_name}: {new_emoji_id}")

        # Удаляем временные файлы
        os.remove(photo_path)
        os.remove(emoji_path)

        return new_emoji_id

    except Exception as e:
        logger.error(f"Ошибка создания кастом эмодзи: {e}")
        return None


# === ДЕКОРАТОР ДЛЯ ОБРАБОТКИ FLOOD WAIT ===
def handle_flood_wait(max_retries=3, initial_delay=1):
    """Декоратор для автоматической обработки FloodWait от Telegram API"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay

            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except pyrogram.errors.FloodWait as e:
                    wait_time = e.value
                    logger.warning(f"FloodWait: ожидание {wait_time} секунд (попытка {retries+1}/{max_retries+1})")

                    if retries == max_retries:
                        raise

                    await asyncio.sleep(wait_time)
                    retries += 1
                    delay *= 2

        return wrapper
    return decorator

# === ХРАНИЛИЩА ДЛЯ MEDIA GROUPS ===
# Хранилище медиа-групп от клиентов
client_media_groups = {}
# Хранилище медиа-групп от менеджеров
manager_media_groups = {}

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
                pyrogram.raw.functions.messages.EditForumTopic(
                    peer=peer,
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


# Вспомогательная функция для получения информации о пересылке (новый API forward_origin)
def get_forward_info(message):
    """
    Возвращает (is_forwarded, is_from_chat, forward_from_name)
    Использует новый API forward_origin вместо deprecated forward_from
    """
    is_forwarded = False
    is_from_chat = False
    forward_from_name = ""

    if hasattr(message, 'forward_origin') and message.forward_origin:
        is_forwarded = True
        origin = message.forward_origin

        # Проверяем тип origin
        origin_type = getattr(origin, 'type', None)

        if origin_type:
            # Используем enum type
            type_name = str(origin_type).lower()

            if 'user' in type_name and 'hidden' not in type_name:
                # MessageOriginType.USER - обычный пользователь
                if hasattr(origin, 'sender_user') and origin.sender_user:
                    user = origin.sender_user
                    forward_from_name = escape_markdown(user.first_name or '')
                    if user.last_name:
                        forward_from_name += f" {escape_markdown(user.last_name)}"
                    if user.username:
                        forward_from_name += f" @{user.username}"

            elif 'hidden' in type_name:
                # MessageOriginType.HIDDEN_USER - скрытый пользователь
                if hasattr(origin, 'sender_user_name') and origin.sender_user_name:
                    forward_from_name = escape_markdown(origin.sender_user_name)

            elif 'chat' in type_name or 'channel' in type_name:
                # MessageOriginType.CHAT или CHANNEL
                is_from_chat = True
                if hasattr(origin, 'chat') and origin.chat:
                    chat = origin.chat
                    forward_from_name = escape_markdown(chat.title or "канала/группы")
                    if chat.username:
                        forward_from_name += f" @{chat.username}"
        else:
            # Fallback: проверяем атрибуты напрямую
            if hasattr(origin, 'sender_user') and origin.sender_user:
                user = origin.sender_user
                forward_from_name = escape_markdown(user.first_name or '')
                if user.last_name:
                    forward_from_name += f" {escape_markdown(user.last_name)}"
                if user.username:
                    forward_from_name += f" @{user.username}"
            elif hasattr(origin, 'sender_user_name') and origin.sender_user_name:
                forward_from_name = escape_markdown(origin.sender_user_name)
            elif hasattr(origin, 'chat') and origin.chat:
                is_from_chat = True
                chat = origin.chat
                forward_from_name = escape_markdown(chat.title or "канала/группы")
                if chat.username:
                    forward_from_name += f" @{chat.username}"

    return is_forwarded, is_from_chat, forward_from_name


# Обновленный обработчик пересылки сообщения клиента в группу поддержки
async def forward_message_to_support(client, message, thread_id=None):
    try:
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
                
                # Определяем имя отправителя (формат: Имя → где → ссылка на профиль)
                user_name = escape_markdown(message.from_user.first_name or "")
                if message.from_user.last_name:
                    user_name += f" {escape_markdown(message.from_user.last_name)}"
                # Добавляем стрелку-ссылку на профиль
                user_id = message.from_user.id
                user_name += f" [↗](tg://user?id={user_id})"

                # Проверяем, является ли сообщение ответом на другое
                reply_info = ""
                quote_info = ""
                reply_to_group_msg_id = None  # ID сообщения в группе для reply
                use_html = False  # Флаг для использования HTML (для blockquote)

                # Проверяем наличие цитаты (выделенный текст при ответе)
                if hasattr(message, 'quote') and message.quote and hasattr(message.quote, 'text') and message.quote.text:
                    quote_text = message.quote.text
                    if len(quote_text) > 200:
                        quote_text = quote_text[:197] + "..."
                    quote_text = escape_html(quote_text)
                    quote_info = f"<blockquote>{quote_text}</blockquote>\n"
                    use_html = True

                if hasattr(message, 'reply_to_message') and message.reply_to_message:
                    reply_msg = message.reply_to_message

                    # Ищем соответствующее сообщение в группе для reply
                    try:
                        mapping = get_group_message_id(db_connection, reply_msg.id, message.from_user.id)
                        if mapping:
                            reply_to_group_msg_id = mapping[0]
                            logger.info(f"Найден маппинг: client_msg {reply_msg.id} -> group_msg {reply_to_group_msg_id}")
                    except Exception as e:
                        logger.debug(f"Не удалось найти маппинг сообщения: {e}")

                    # Получаем текст сообщения, на которое ответили (только если нет цитаты)
                    reply_text = ""
                    if not quote_info:  # Если уже есть цитата, не дублируем
                        if hasattr(reply_msg, 'text') and reply_msg.text:
                            reply_text = reply_msg.text
                        elif hasattr(reply_msg, 'caption') and reply_msg.caption:
                            reply_text = reply_msg.caption
                        else:
                            # Определяем тип медиа
                            if hasattr(reply_msg, 'photo') and reply_msg.photo:
                                reply_text = "[фото]"
                            elif hasattr(reply_msg, 'video') and reply_msg.video:
                                reply_text = "[видео]"
                            elif hasattr(reply_msg, 'document') and reply_msg.document:
                                reply_text = "[файл]"
                            elif hasattr(reply_msg, 'voice') and reply_msg.voice:
                                reply_text = "[голосовое]"
                            elif hasattr(reply_msg, 'sticker') and reply_msg.sticker:
                                reply_text = "[стикер]"
                            else:
                                reply_text = "[сообщение]"

                        # Обрезаем длинный текст
                        if len(reply_text) > 100:
                            reply_text = reply_text[:97] + "..."
                        reply_text = escape_markdown(reply_text)
                        # Показываем текстовую информацию только если нет маппинга для reply
                        if not reply_to_group_msg_id:
                            reply_info = f"↩️ В ответ на: _{reply_text}_\n\n"

                # Комбинируем цитату и reply_info
                if quote_info:
                    reply_info = quote_info

                # Проверяем пересланные сообщения (используем новый API forward_origin)
                is_forwarded, is_forwarded_from_chat, forward_from_name = get_forward_info(message)

                # Проверяем наличие медиа и считаем количество
                media_type = None
                media_count = 1
                media_label = ""

                if hasattr(message, 'photo') and message.photo:
                    media_type = "photo"
                    media_label = "изображение"
                elif hasattr(message, 'video') and message.video:
                    media_type = "video"
                    media_label = "видео"
                elif hasattr(message, 'voice') and message.voice:
                    media_type = "voice"
                    media_label = "голосовое"
                elif hasattr(message, 'audio') and message.audio:
                    media_type = "audio"
                    media_label = "аудио"
                elif hasattr(message, 'document') and message.document:
                    media_type = "document"
                    media_label = "файл"
                elif hasattr(message, 'sticker') and message.sticker:
                    media_type = "sticker"
                    media_label = "стикер"
                elif hasattr(message, 'animation') and message.animation:
                    media_type = "animation"
                    media_label = "анимация"

                # Получаем основной текст сообщения и экранируем markdown
                message_content = ""
                if hasattr(message, 'text') and message.text:
                    message_content = escape_markdown(message.text)
                elif hasattr(message, 'caption') and message.caption:
                    message_content = escape_markdown(message.caption)

                # Формируем заголовок сообщения
                if is_forwarded:
                    if is_forwarded_from_chat:
                        # Формат для каналов
                        message_header = f"**{user_name}** переслал из **{forward_from_name}**:"
                    else:
                        # Формат для людей
                        message_header = f"**{user_name}** переслал от **{forward_from_name}**:"
                else:
                    # Обычный формат
                    message_header = f"**{user_name}**"

                # Формируем строку с медиа (+ 1 видео, + 2 файла и т.д.)
                media_info = ""
                if media_type:
                    media_info = f"\n+ {media_count} {media_label}"

                # Собираем сообщение
                if use_html:
                    # HTML формат для цитат
                    user_name_html = escape_html(message.from_user.first_name or "")
                    if message.from_user.last_name:
                        user_name_html += f" {escape_html(message.from_user.last_name)}"
                    # Добавляем стрелку-ссылку на профиль (HTML формат)
                    user_name_html += f' <a href="tg://user?id={user_id}">↗</a>'

                    message_content_html = ""
                    if hasattr(message, 'text') and message.text:
                        message_content_html = escape_html(message.text)
                    elif hasattr(message, 'caption') and message.caption:
                        message_content_html = escape_html(message.caption)

                    message_header_html = f"<b>{user_name_html}</b>"
                    media_info_html = f"\n+ {media_count} {media_label}" if media_type else ""

                    if message_content_html:
                        full_message = f"{message_header_html}\n{reply_info}{message_content_html}{media_info_html}{manager_mention}"
                    else:
                        full_message = f"{message_header_html}\n{reply_info}{media_info_html}{manager_mention}"
                    parse_mode = pyrogram.enums.ParseMode.HTML
                else:
                    # Markdown формат (стандартный)
                    if reply_info:
                        if message_content:
                            full_message = f"{message_header}\n{reply_info}{message_content}{media_info}{manager_mention}"
                        else:
                            full_message = f"{message_header}\n{reply_info}{media_info}{manager_mention}"
                    else:
                        if message_content:
                            full_message = f"{message_header}\n\n{message_content}{media_info}{manager_mention}"
                        else:
                            full_message = f"{message_header}{media_info}{manager_mention}"
                    parse_mode = pyrogram.enums.ParseMode.MARKDOWN

                # Определяем на какое сообщение отвечать:
                # - если есть маппинг (клиент ответил на конкретное сообщение) - reply на него
                # - иначе просто в топик
                target_reply_id = reply_to_group_msg_id if reply_to_group_msg_id else thread_id

                # Отправляем сообщение в тред
                sent_message = await client.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=full_message,
                    reply_to_message_id=target_reply_id,
                    parse_mode=parse_mode
                )

                # Сохраняем маппинг сообщения клиента -> сообщения в группе
                try:
                    save_message_mapping(
                        db_connection,
                        client_message_id=message.id,
                        group_message_id=sent_message.id,
                        thread_id=thread_id,
                        user_id=message.from_user.id,
                        message_text=message.text or message.caption  # Сохраняем текст для отслеживания редактирования
                    )
                    logger.debug(f"Сохранён маппинг: client_msg {message.id} -> group_msg {sent_message.id}")
                except Exception as map_err:
                    logger.warning(f"Не удалось сохранить маппинг сообщения: {map_err}")

                # Если это медиа, пробуем скопировать его отдельно без подписи
                if media_type:
                    try:
                        copied_message = await client.copy_message(
                            chat_id=SUPPORT_GROUP_ID,
                            from_chat_id=message.chat.id,
                            message_id=message.id,
                            reply_to_message_id=target_reply_id,
                            caption=""  # Пустая подпись, т.к. текст уже отправлен
                        )
                        # Сохраняем маппинг и для медиа-сообщения
                        try:
                            save_message_mapping(
                                db_connection,
                                client_message_id=message.id,
                                group_message_id=copied_message.id,
                                thread_id=thread_id,
                                user_id=message.from_user.id
                            )
                        except:
                            pass
                        logger.info(f"Медиа успешно скопировано в тред {thread_id}")
                    except Exception as media_error:
                        logger.error(f"Ошибка при копировании медиа: {media_error}")

                logger.info(f"Сообщение клиента отправлено в тред {thread_id}" +
                           (f" (reply на {reply_to_group_msg_id})" if reply_to_group_msg_id else ""))
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

# Отправка сообщения от менеджера клиенту
async def send_manager_reply_to_client(client, manager_id, client_id, message_text, group_message_id=None, thread_id=None, reply_to_client_msg_id=None):
    try:
        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False

        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager

        # Получаем custom_emoji_id
        custom_emoji_id = get_manager_custom_emoji_id(db_connection, manager_id)

        # Форматируем сообщение с подписью
        full_message, entities = format_signature_with_custom_emoji(
            message_text, name, position, custom_emoji_id, fallback_emoji=emoji or "👤"
        )

        # Отправляем сообщение клиенту (с reply если указан)
        if entities:
            # С кастом эмодзи - без parse_mode, с entities
            sent_message = await client.send_message(
                chat_id=client_id,
                text=full_message,
                reply_to_message_id=reply_to_client_msg_id,
                entities=entities
            )
        else:
            # Без кастом эмодзи - обычный markdown
            sent_message = await client.send_message(
                chat_id=client_id,
                text=full_message,
                reply_to_message_id=reply_to_client_msg_id,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
        reply_info = f" (reply на {reply_to_client_msg_id})" if reply_to_client_msg_id else ""
        logger.info(f"Ответ менеджера отправлен клиенту {client_id}{reply_info}")

        # Сохраняем маппинг сообщения (группа -> клиент)
        if group_message_id and thread_id:
            try:
                save_message_mapping(
                    db_connection,
                    client_message_id=sent_message.id,
                    group_message_id=group_message_id,
                    thread_id=thread_id,
                    user_id=client_id,
                    message_text=message_text  # Сохраняем текст для возможного редактирования
                )
                logger.debug(f"Сохранён маппинг: group_msg {group_message_id} -> client_msg {sent_message.id}")
            except Exception as map_err:
                logger.warning(f"Не удалось сохранить маппинг сообщения: {map_err}")

        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, full_message, is_from_user=False)

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

        # Распаковываем данные менеджера
        # Было 7 значений, теперь 8 (добавлен username)
        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager

        # Получаем ID сообщения с фото в канале
        photo_channel_message_id = get_manager_photo_channel_id(db_connection, manager_id)

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
        card_text += "С большим интересом займемся вашим проектом"

        logger.info(f"Подготовлен текст карточки для клиента {client_id}")

        # Пробуем получить фото из канала-архива (надёжнее после перезапуска)
        actual_photo_id = None
        if photo_channel_message_id:
            try:
                channel_msg = await client.get_messages(FILES_CHANNEL_ID, photo_channel_message_id)
                if channel_msg and channel_msg.photo:
                    actual_photo_id = channel_msg.photo.file_id
                    logger.info(f"Получен file_id фото из канала для менеджера {manager_id}")
            except Exception as e:
                logger.warning(f"Не удалось получить фото из канала: {e}")

        # Если из канала не получили, используем сохранённый file_id
        if not actual_photo_id and photo_file_id:
            actual_photo_id = photo_file_id
            logger.info(f"Используем сохранённый file_id для менеджера {manager_id}")

        # Если есть фото менеджера
        if actual_photo_id:
            try:
                # Отправляем фото с подписью
                logger.info(f"Отправка карточки с фото менеджера {manager_id}")
                sent_message = await client.send_photo(
                    chat_id=client_id,
                    photo=actual_photo_id,
                    caption=card_text,
                    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                )
                logger.info(f"Карточка менеджера с фото отправлена клиенту {client_id}, message_id: {sent_message.id}")
                return True
            except Exception as e:
                logger.error(f"Ошибка при отправке фото менеджера: {e}, пробуем отправить только текст")
                # Если с фото проблема, отправляем только текст
                sent_message = await client.send_message(
                    chat_id=client_id,
                    text=card_text,
                    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                )
                logger.info(f"Текстовая карточка (после ошибки с фото) отправлена клиенту {client_id}")
                return True
        else:
            # Если фото нет, отправляем только текст
            logger.info(f"Отправка текстовой карточки менеджера {manager_id} (без фото)")
            sent_message = await client.send_message(
                chat_id=client_id,
                text=card_text,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
            logger.info(f"Текстовая карточка менеджера отправлена клиенту {client_id}, message_id: {sent_message.id}")
            return True
    except Exception as e:
        logger.error(f"Критическая ошибка при отправке карточки менеджера: {e}")
        return False

# Функция для отправки медиафайла от менеджера клиенту
async def send_manager_media_to_client(client, manager_id, client_id, file_id, caption=None, media_type="photo"):
    try:
        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False
        
        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager

        # Формируем подпись менеджера (моноширинным)
        signature = f"\n—\n`{emoji} {name}, {position}`"

        # Полная подпись с текстом сообщения
        full_caption = f"{caption or ''}{signature}"

        # Отправляем медиафайл в зависимости от типа
        if media_type == "photo":
            await client.send_photo(
                chat_id=client_id,
                photo=file_id,
                caption=full_caption,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
        elif media_type == "document":
            await client.send_document(
                chat_id=client_id,
                document=file_id,
                caption=full_caption,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
        elif media_type == "video":
            await client.send_video(
                chat_id=client_id,
                video=file_id,
                caption=full_caption,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
        elif media_type == "audio":
            await client.send_audio(
                chat_id=client_id,
                audio=file_id,
                caption=full_caption,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
        elif media_type == "voice":
            await client.send_voice(
                chat_id=client_id,
                voice=file_id,
                caption=full_caption,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )
        
        logger.info(f"Медиафайл типа {media_type} от менеджера отправлен клиенту {client_id}")
        
        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, f"{caption or '[Медиафайл]'}{signature}", is_from_user=False, media_type=media_type.upper())

        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке медиафайла от менеджера: {e}")
        return False

# === ФУНКЦИИ ДЛЯ MEDIA GROUPS ===

async def send_manager_media_group_to_client(client, manager_id, client_id, media_group_data):
    """Отправка медиа-группы от менеджера клиенту"""
    try:
        manager = get_manager(db_connection, manager_id)
        if not manager:
            logger.error(f"Менеджер с ID {manager_id} не найден в базе данных")
            return False

        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager
        signature = f"\n—\n`{emoji} {name}, {position}`"

        # Получаем caption и удаляем команду из начала (/105 или /дс00)
        caption = media_group_data.get("caption", "")
        if caption:
            # Удаляем команду из начала подписи (числовую или custom_id)
            parts = caption.split(None, 1)  # разбиваем по первому пробелу
            if parts and parts[0].startswith("/"):
                # Удаляем любую команду (/105 или /дс00)
                caption = parts[1] if len(parts) > 1 else ""
        full_caption = f"{caption}{signature}" if caption else signature.lstrip("\n—\n`").rstrip("`")

        # Собираем медиа без подписи
        media_group = []
        for msg in media_group_data["messages"]:
            if hasattr(msg, 'photo') and msg.photo:
                media_group.append(pyrogram.types.InputMediaPhoto(msg.photo.file_id))
            elif hasattr(msg, 'document') and msg.document:
                media_group.append(pyrogram.types.InputMediaDocument(msg.document.file_id))
            elif hasattr(msg, 'video') and msg.video:
                media_group.append(pyrogram.types.InputMediaVideo(msg.video.file_id))

        if not media_group:
            logger.error(f"Нет медиафайлов для отправки клиенту {client_id}")
            return False

        # Сначала фото, потом подпись
        try:
            await client.send_media_group(chat_id=client_id, media=media_group)
        except TypeError as e:
            if "topics" not in str(e):
                raise
        await client.send_message(chat_id=client_id, text=full_caption, parse_mode=pyrogram.enums.ParseMode.MARKDOWN)
        logger.info(f"Медиа-группа от менеджера отправлена клиенту {client_id}")

        save_message(db_connection, client_id, f"{caption or '[Медиафайлы]'}{signature}", is_from_user=False, media_type="MEDIA_GROUP")
        return True
    except Exception as e:
        logger.error(f"Ошибка при отправке медиа-группы от менеджера: {e}")
        return False


async def handle_client_media_group(client, message, thread_id=None):
    """Обрабатывает медиа-группы от клиента, собирая их и отправляя в группу поддержки"""
    global client_media_groups

    media_group_id = message.media_group_id
    user_id = message.from_user.id
    group_key = f"{media_group_id}_{thread_id}_{user_id}"

    if group_key not in client_media_groups:
        client_media_groups[group_key] = {
            "messages": [],
            "user_id": user_id,
            "thread_id": thread_id,
            "timestamp": datetime.datetime.now(),
            "processed": False
        }
        logger.info(f"Создана новая запись для медиа-группы клиента {group_key}")

    client_media_groups[group_key]["messages"].append(message)
    logger.info(f"Добавлено сообщение в медиа-группу клиента {group_key}, всего: {len(client_media_groups[group_key]['messages'])}")

    if len(client_media_groups[group_key]["messages"]) == 1:
        async def process_client_group():
            await asyncio.sleep(2)  # Ждем 2 секунды для сбора всех файлов

            if group_key in client_media_groups and not client_media_groups[group_key]["processed"]:
                group_data = client_media_groups[group_key]
                group_data["processed"] = True

                try:
                    if thread_id:
                        update_client_message_time(db_connection, thread_id)

                        user_name = escape_markdown(message.from_user.first_name or "")
                        if message.from_user.last_name:
                            user_name += f" {escape_markdown(message.from_user.last_name)}"
                        # Добавляем стрелку-ссылку на профиль
                        user_id = message.from_user.id
                        user_name += f" [↗](tg://user?id={user_id})"

                        # Проверяем, пересланы ли файлы (используем новый API forward_origin)
                        first_msg = group_data["messages"][0]
                        is_forwarded, is_forwarded_from_chat, forward_from_name = get_forward_info(first_msg)

                        # Получаем caption из медиа-группы и экранируем
                        caption_text = next((msg.caption for msg in group_data["messages"] if msg.caption), "")
                        caption_text = escape_markdown(caption_text)

                        # Определяем тип файлов
                        file_count = len(group_data['messages'])
                        file_type = "файлов"
                        if all(hasattr(m, 'photo') and m.photo for m in group_data['messages']):
                            file_type = "фото" if file_count == 1 else "фото"
                        elif all(hasattr(m, 'video') and m.video for m in group_data['messages']):
                            file_type = "видео"
                        elif all(hasattr(m, 'document') and m.document for m in group_data['messages']):
                            file_type = "файлов" if file_count > 1 else "файл"

                        # Формируем текст
                        if is_forwarded:
                            if is_forwarded_from_chat:
                                header = f"**{user_name}** переслал из **{forward_from_name}**:"
                            else:
                                header = f"**{user_name}** переслал от **{forward_from_name}**:"
                        else:
                            header = f"**{user_name}**"

                        # Собираем: заголовок + текст (если есть) + счётчик файлов
                        if caption_text:
                            info_text = f"{header}\n\n{caption_text}\n+ {file_count} {file_type}"
                        else:
                            info_text = f"{header}\n+ {file_count} {file_type}"

                        await client.send_message(
                            chat_id=SUPPORT_GROUP_ID,
                            text=info_text,
                            reply_to_message_id=thread_id,
                            parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                        )

                    # Собираем медиа без подписи
                    media_list = []
                    for msg in group_data["messages"]:
                        if hasattr(msg, 'photo') and msg.photo:
                            media_list.append(pyrogram.types.InputMediaPhoto(media=msg.photo.file_id))
                        elif hasattr(msg, 'document') and msg.document:
                            media_list.append(pyrogram.types.InputMediaDocument(media=msg.document.file_id))
                        elif hasattr(msg, 'video') and msg.video:
                            media_list.append(pyrogram.types.InputMediaVideo(media=msg.video.file_id))
                        elif hasattr(msg, 'audio') and msg.audio:
                            media_list.append(pyrogram.types.InputMediaAudio(media=msg.audio.file_id))

                    if media_list:
                        kwargs = {"chat_id": SUPPORT_GROUP_ID, "media": media_list}
                        if thread_id:
                            kwargs["reply_to_message_id"] = thread_id
                        try:
                            await client.send_media_group(**kwargs)
                        except TypeError as e:
                            # Баг в Pyrofork - Messages.__init__() missing 'topics'
                            # Сообщения всё равно отправляются
                            if "topics" in str(e):
                                logger.debug(f"Игнорируем баг Pyrofork с topics: {e}")
                            else:
                                raise
                        logger.info(f"Отправлена медиа-группа с {len(media_list)} файлами в группу поддержки, тред {thread_id}")

                    save_message(db_connection, user_id,
                               f"Группа медиафайлов ({len(group_data['messages'])} шт.)" + (f": {caption_text}" if caption_text else ""),
                               is_from_user=True, media_type="MEDIA_GROUP")

                except Exception as e:
                    logger.error(f"Ошибка при обработке медиа-группы клиента {group_key}: {e}")
                    # Fallback - отправляем файлы по одному
                    if thread_id:
                        for msg in group_data["messages"]:
                            try:
                                await client.copy_message(
                                    chat_id=SUPPORT_GROUP_ID,
                                    from_chat_id=msg.chat.id,
                                    message_id=msg.id,
                                    reply_to_message_id=thread_id
                                )
                            except Exception as copy_error:
                                logger.error(f"Ошибка при отправке отдельного файла: {copy_error}")

                # Удаляем данные группы через 30 секунд
                await asyncio.sleep(30)
                if group_key in client_media_groups:
                    del client_media_groups[group_key]

        asyncio.create_task(process_client_group())

    return True


async def handle_manager_media_group(client, message, thread_id, client_id):
    """Обрабатывает медиа-группы от менеджера для отправки клиенту"""
    global manager_media_groups

    media_group_id = message.media_group_id
    manager_id = message.from_user.id
    group_key = f"{media_group_id}_{thread_id}_{manager_id}"

    # Проверяем, есть ли ожидающая группа (фото без caption пришли раньше)
    pending_key = f"{media_group_id}_pending"
    pending_messages = []
    if pending_key in manager_media_groups:
        pending_messages = manager_media_groups[pending_key]["messages"]
        del manager_media_groups[pending_key]
        logger.info(f"Найдена ожидающая группа {pending_key} с {len(pending_messages)} фото")

    is_new_group = group_key not in manager_media_groups
    if is_new_group:
        manager_media_groups[group_key] = {
            "messages": pending_messages,  # Добавляем ранее пришедшие фото
            "manager_id": manager_id,
            "thread_id": thread_id,
            "client_id": client_id,
            "caption": message.caption or "",
            "timestamp": datetime.datetime.now(),
            "processed": False,
            "task_started": False
        }
        logger.info(f"Создана новая запись для медиа-группы менеджера {group_key}")

    manager_media_groups[group_key]["messages"].append(message)
    if message.caption and not manager_media_groups[group_key]["caption"]:
        manager_media_groups[group_key]["caption"] = message.caption

    logger.info(f"Добавлено сообщение в медиа-группу менеджера {group_key}, всего: {len(manager_media_groups[group_key]['messages'])}")

    # Запускаем таймер только один раз при создании группы
    if is_new_group and not manager_media_groups[group_key].get("task_started"):
        manager_media_groups[group_key]["task_started"] = True
        async def process_manager_group():
            await asyncio.sleep(2)

            if group_key in manager_media_groups and not manager_media_groups[group_key]["processed"]:
                group_data = manager_media_groups[group_key]
                group_data["processed"] = True

                try:
                    success = await send_manager_media_group_to_client(
                        client, group_data["manager_id"], group_data["client_id"], group_data
                    )

                    if success:
                        update_manager_reply_time(db_connection, group_data["thread_id"])
                        logger.info(f"Медиа-группа от менеджера {group_data['manager_id']} отправлена клиенту {group_data['client_id']}")
                    else:
                        await message.reply_text("❌ Не удалось отправить медиа-группу клиенту.")

                except Exception as e:
                    logger.error(f"Ошибка при обработке медиа-группы менеджера {group_key}: {e}")
                    await message.reply_text(f"❌ Ошибка: {e}")

                await asyncio.sleep(30)
                if group_key in manager_media_groups:
                    del manager_media_groups[group_key]

        asyncio.create_task(process_manager_group())

    return True


async def handle_manager_media_continuation(client, message):
    """
    Обрабатывает дополнительные фото в media_group от менеджера (без caption).
    Ищет существующую группу по media_group_id и добавляет в неё.
    Если группа ещё не создана - создаёт "ожидающую" запись.
    """
    global manager_media_groups

    if not message.media_group_id:
        return False

    media_group_id = message.media_group_id

    # Ищем существующую группу для этого media_group_id
    for group_key, group_data in manager_media_groups.items():
        if group_key.startswith(f"{media_group_id}_") and not group_data["processed"]:
            # Нашли группу - добавляем сообщение
            group_data["messages"].append(message)
            logger.info(f"Добавлено фото в медиа-группу менеджера {group_key}, всего: {len(group_data['messages'])}")
            return True

    # Группа ещё не создана - создаём "ожидающую" запись
    # Ключ временный, будет обновлён когда придёт сообщение с caption
    pending_key = f"{media_group_id}_pending"
    if pending_key not in manager_media_groups:
        manager_media_groups[pending_key] = {
            "messages": [],
            "pending": True,
            "timestamp": datetime.datetime.now(),
            "processed": False
        }
        logger.info(f"Создана ожидающая медиа-группа {pending_key}")

    manager_media_groups[pending_key]["messages"].append(message)
    logger.info(f"Добавлено фото в ожидающую медиа-группу {pending_key}, всего: {len(manager_media_groups[pending_key]['messages'])}")
    return True


# Алиас для reset_thread_notification (используется локально)
def reset_thread_notification(conn, thread_id):
    """Сброс уведомления треда"""
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    cursor.execute('''
    UPDATE thread_status
    SET is_notified = 0,
        last_notification = NULL,
        last_manager_reply = ?
    WHERE thread_id = ?
    ''', (current_time, thread_id))
    conn.commit()

# Функция для получения информации о клиенте по ID треда
def get_client_info_by_thread(conn, thread_id):
    """Получить информацию о клиенте по thread_id"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT first_name, last_name, username
    FROM clients
    WHERE thread_id = ?
    ''', (thread_id,))
    return cursor.fetchone()

# Упрощенная функция для изменения заголовка треда
async def update_thread_title(client, thread_id, title):
    try:
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        await client.invoke(
            pyrogram.raw.functions.messages.EditForumTopic(
                peer=peer,
                topic_id=thread_id,
                title=title
            )
        )
        logger.info(f"Заголовок треда {thread_id} изменен на '{title}'")
        return True
    except Exception as e:
        # Игнорируем ошибку "TOPIC_NOT_MODIFIED" - заголовок уже такой
        if "TOPIC_NOT_MODIFIED" in str(e):
            logger.debug(f"Тред {thread_id}: заголовок уже '{title}'")
            return True
        logger.error(f"Ошибка при изменении заголовка треда {thread_id}: {e}")
        return False

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
        from database import get_client_info_for_thread_title, format_thread_title
        client_info = get_client_info_for_thread_title(db_connection, thread_id)

        if not client_info:
            logger.error(f"Не удалось найти клиента для треда {thread_id}")
            return False

        # Распаковываем информацию о клиенте
        first_name, last_name, company_name, custom_id = client_info

        # Формируем заголовок через централизованную функцию
        base_title = format_thread_title(thread_id, first_name, last_name, company_name, custom_id)
        alert_title = f"🔥{base_title}"

        # Выбираем заголовок в зависимости от нужного состояния
        new_title = alert_title if is_urgent else base_title
            
        # Обновляем заголовок
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        await client.invoke(
            pyrogram.raw.functions.messages.EditForumTopic(
                peer=peer,
                topic_id=thread_id,
                title=new_title
            )
        )

        # Запоминаем новое состояние
        thread_title_states[thread_id] = {"has_alert": is_urgent, "title": new_title}
        
        logger.info(f"Заголовок треда {thread_id} изменен на '{new_title}'")
        return True

    except Exception as e:
        # Игнорируем ошибку "TOPIC_NOT_MODIFIED" - заголовок уже такой
        if "TOPIC_NOT_MODIFIED" in str(e):
            logger.debug(f"Тред {thread_id}: заголовок уже '{new_title}'")
            thread_title_states[thread_id] = {"has_alert": is_urgent, "title": new_title}
            return True
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
            dm.manager_username
        FROM thread_status ts
        JOIN clients c ON ts.thread_id = c.thread_id
        LEFT JOIN duty_managers dm ON ts.thread_id = dm.thread_id
        WHERE 
            (ts.last_manager_reply IS NULL OR ts.last_client_message > ts.last_manager_reply)
            AND ts.notification_disabled = 0
        ''')
        
        unanswered_threads = cursor.fetchall()
        logger.info(f"Найдено {len(unanswered_threads)} тредов с неотвеченными сообщениями")
        
        for thread in unanswered_threads:
            thread_id, last_client_msg, last_manager_reply, is_notified, last_notification, disabled, \
            user_id, first_name, last_name, username, manager_username = thread
            
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
                    
                    notification_text = f"⚠️ **ВНИМАНИЕ!** ⚠️\n\n"
                    notification_text += f"🔴 Неотвеченное сообщение в треде #{thread_id}!\n"
                    notification_text += f"👤 Клиент: {client_name}\n"
                    notification_text += f"⏱ Ожидание: {waiting_time}\n\n"
                    
                    if manager_username:
                        notification_text += f"📌 Ответственный: @{manager_username}\n\n"
                    else:
                        notification_text += f"📌 Ответственный менеджер не назначен\n\n"
                    
                    notification_text += f"🔗 [Перейти к треду](https://t.me/c/{str(SUPPORT_GROUP_ID)[4:]}/{thread_id})\n\n"
                    notification_text += f"Чтобы отключить текущее уведомление для этого треда, используйте команду /ok {thread_id}"
                    
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
                    cursor.execute('''
                    INSERT INTO group_threads (group_id, group_title, thread_id, created_at)
                    VALUES (?, ?, ?, ?)
                    ''', (chat.id, chat.title, thread_id, datetime.datetime.now()))
                    db_connection.commit()
                    
                    # Отправляем начальную информацию о группе
                    try:
                        # Получаем информацию о группе
                        chat_info = await client.get_chat(chat.id)
                        member_count = await client.get_chat_members_count(chat.id)
                        
                        info_message = f"📋 **Новая группа: {chat.title}**\n\n"
                        info_message += f"🆔 **ID группы**: `{chat.id}`\n"
                        info_message += f"👥 **Участников**: {member_count}\n"
                        
                        if chat_info.description:
                            info_message += f"📝 **Описание**: {chat_info.description}\n"
                            
                        await client.send_message(
                            chat_id=SUPPORT_GROUP_ID,
                            text=info_message,
                            reply_to_message_id=thread_id
                        )
                        
                        # Отправляем список участников
                        await send_member_list(client, chat.id, thread_id)
                        
                    except Exception as e:
                        logger.error(f"Ошибка при отправке информации о группе: {e}")
                    
                    logger.info(f"Создан тред {thread_id} для группы {chat.title}")
        
        # Отслеживаем изменения участников в группах
        elif update.new_chat_member or update.old_chat_member:
            # Получаем группу
            chat = update.chat
            
            # Проверяем, есть ли тред для этой группы
            cursor = db_connection.cursor()
            cursor.execute('SELECT thread_id FROM group_threads WHERE group_id = ?', (chat.id,))
            thread_data = cursor.fetchone()
            
            if not thread_data:
                # Нет треда для этой группы
                return
                
            thread_id = thread_data[0]
            
            # Определяем тип события
            if update.new_chat_member and not update.old_chat_member:
                # Добавлен новый участник
                user = update.new_chat_member.user
                event_text = f"➕ **Новый участник**: {user.first_name}"
                if user.last_name:
                    event_text += f" {user.last_name}"
                if user.username:
                    event_text += f" (@{user.username})"
                event_text += f" [ID: {user.id}]"
            
            elif update.old_chat_member and not update.new_chat_member:
                # Удален участник
                user = update.old_chat_member.user
                event_text = f"➖ **Участник удален**: {user.first_name}"
                if user.last_name:
                    event_text += f" {user.last_name}"
                if user.username:
                    event_text += f" (@{user.username})"
                event_text += f" [ID: {user.id}]"
            
            elif update.old_chat_member and update.new_chat_member:
                # Изменение статуса участника
                user = update.new_chat_member.user
                old_status = update.old_chat_member.status
                new_status = update.new_chat_member.status
                
                event_text = f"🔄 **Изменение статуса**: {user.first_name}"
                if user.last_name:
                    event_text += f" {user.last_name}"
                if user.username:
                    event_text += f" (@{user.username})"
                event_text += f" [ID: {user.id}]"
                event_text += f"\nСтатус изменен с '{old_status}' на '{new_status}'"
            
            else:
                # Неизвестное изменение
                return
            
            # Отправляем уведомление в тред
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=event_text,
                reply_to_message_id=thread_id
            )
            
    except Exception as e:
        logger.error(f"Ошибка при обработке обновления участников чата: {e}")
        
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
        
        # Получаем имя отправителя (формат: Имя → где → ссылка на профиль)
        user = message.from_user
        user_name = f"{user.first_name or ''}"
        if user.last_name:
            user_name += f" {user.last_name}"
        user_name += f" [↗](tg://user?id={user.id})"

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

# Обработчик медиафайлов с командой в подписи
@business.on_message(filters.regex(r"^/\d+") & filters.chat(SUPPORT_GROUP_ID) & (filters.photo | filters.document | filters.video | filters.audio | filters.voice))
async def handle_media_with_thread_command(client, message):
    try:
        if not message.from_user:
            return
        manager_id = message.from_user.id

        # DEBUG: проверяем media_group_id
        logger.info(f"[DEBUG] handle_media_with_thread_command: media_group_id={message.media_group_id}, caption={message.caption[:30] if message.caption else None}")

        # Извлекаем номер треда из caption
        caption_text = message.caption or ""
        if not caption_text:
            return

        first_word = caption_text.split()[0]
        try:
            thread_id = int(first_word[1:])
        except ValueError:
            return

        # Получаем клиента
        client_data = get_client_by_thread(db_connection, thread_id)
        actual_thread_id = thread_id

        if not client_data:
            # Fallback: пробуем найти по user_id
            cursor = db_connection.cursor()
            cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (thread_id,))
            client_by_user_id = cursor.fetchone()

            if client_by_user_id:
                actual_thread_id = client_by_user_id[0]
                client_id = thread_id  # thread_id на самом деле был user_id
                logger.info(f"Fallback медиа: найден клиент {client_id} по user_id, thread_id={actual_thread_id}")
            else:
                await message.reply_text(f"Клиент для /{thread_id} не найден.\nИспользуйте номер треда или ID клиента.")
                return
        else:
            client_id = client_data[0]

        # Если это медиа-группа - обрабатываем отдельно
        if message.media_group_id:
            await handle_manager_media_group(client, message, actual_thread_id, client_id)
            return

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        logger.info(f"Получен медиафайл с командой /{thread_id} от менеджера {manager_id}")

        # Парсим текст подписи
        caption = None
        if " " in caption_text:
            caption = caption_text.split(" ", 1)[1]

        # Определяем тип медиафайла и получаем его file_id
        file_id = None
        media_type = None
        
        if message.photo:
            file_id = message.photo.file_id
            media_type = "photo"
        elif message.document:
            file_id = message.document.file_id
            media_type = "document"
        elif message.video:
            file_id = message.video.file_id
            media_type = "video"
        elif message.audio:
            file_id = message.audio.file_id
            media_type = "audio"
        elif message.voice:
            file_id = message.voice.file_id
            media_type = "voice"
        
        # Отправляем медиафайл клиенту
        success = await send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type)
        
        if success:
            # Обновляем время последнего ответа менеджера
            update_manager_reply_time(db_connection, actual_thread_id)

            # Пытаемся убрать индикатор срочности из заголовка треда
            try:
                await mark_thread_urgent(client, actual_thread_id, is_urgent=False)
            except Exception as e:
                logger.error(f"Ошибка при изменении визуального индикатора треда {actual_thread_id}: {e}")

            # Не отправляем подтверждение в группу, только записываем в лог
            logger.info(f"Медиафайл от менеджера {manager_id} отправлен клиенту {client_id}, статус треда обновлен")

            # Сохраняем информацию о первом ответе менеджера, если это первый ответ
            if is_first_reply(db_connection, actual_thread_id, manager_id):
                save_first_reply(db_connection, actual_thread_id, client_id, manager_id)
                logger.info(f"Сохранена информация о первом ответе менеджера {manager_id} для треда {actual_thread_id}")
        else:
            # Только в случае ошибки уведомляем менеджера
            await message.reply_text(
                "❌ Не удалось отправить медиафайл клиенту."
            )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке медиафайла с командой: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Кастомный фильтр для дополнительных фото в media_group (без команды в caption)
async def media_group_no_command_filter(_, __, message):
    if not message.media_group_id:
        return False
    caption = message.caption or ""
    # НЕ ловим сообщения с ЛЮБЫМИ командами (включая /дс00)
    # Этот фильтр только для дополнительных фото БЕЗ caption
    return not caption.startswith("/")


# Обработчик дополнительных фото в media_group (без caption с командой)
@business.on_message(filters.create(media_group_no_command_filter) & filters.chat(SUPPORT_GROUP_ID) & (filters.photo | filters.document | filters.video | filters.audio | filters.voice))
async def handle_media_group_continuation(client, message):
    """Добавляет дополнительные фото в существующую media_group"""
    await handle_manager_media_continuation(client, message)


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
        
        # Формируем подпись менеджера (моноширинным)
        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager
        signature = f"\n\n`{emoji} {name}, {position}`"

        # Полное сообщение с подписью
        full_message = f"{reply_text}{signature}"

        # Отправляем сообщение в группу
        try:
            sent_message = await client.send_message(
                chat_id=group_id,
                text=full_message,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
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
# Декоратор убран — команда обрабатывается через роутер (command_router)
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
        
        # Парсим команду: /auth [emoji], [name], [position]
        command_text = message.text.strip()

        # Удаляем команду /auth
        if " " in command_text:
            auth_data = command_text.split(" ", 1)[1]
        else:
            await message.reply_text(
                "Неверный формат команды. Используйте: /auth [эмодзи], [Имя], [Должность]\n"
                "Например: /auth 🔧, Иван Петров, Технический специалист"
            )
            return

        # Разделяем по запятым и удаляем лишние пробелы
        parts = [part.strip() for part in auth_data.split(",")]

        # Проверяем, что есть как минимум 3 части
        if len(parts) < 3:
            await message.reply_text(
                "Неверный формат команды. Требуется: [эмодзи], [Имя], [Должность]\n"
                "Например: /auth 🔧, Иван Петров, Технический специалист"
            )
            return

        # Получаем данные
        emoji = parts[0]
        name = parts[1]

        # Все части после имени считаем должностью
        position = ", ".join(parts[2:])

        # Сохраняем данные о менеджере с username (без фото пока)
        save_manager(db_connection, manager_id, emoji, name, position, username=manager_username)
        
        # Запрашиваем фото менеджера
        await message.reply_text(
            f"Спасибо! Теперь отправьте фотографию для профиля.\n\n"
            f"💡 Рекомендация: лицо должно занимать ~80% кадра (фото будет обрезано в круг 100x100 для эмодзи)"
        )
        
        # Устанавливаем состояние ожидания фото
        manager_auth_state[manager_id] = "waiting_photo"
        
        logger.info(f"Менеджер {manager_id} ({name}) успешно зарегистрирован, ожидается фото")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды /auth: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

# Обработчик для приема фотографий менеджеров
@business.on_message(filters.photo & filters.chat(SUPPORT_GROUP_ID))
async def handle_manager_photo(client, message):
    try:
        logger.info(f"[DEBUG] handle_manager_photo: media_group_id={message.media_group_id}, caption={message.caption[:20] if message.caption else None}")

        # Пропускаем фото с командой в caption (обрабатывается другими handlers)
        caption = message.caption or ""
        if caption.startswith("/"):
            # Если это custom_id (не число) - вызываем handler напрямую
            if len(caption) > 1 and not caption[1:2].isdigit():
                await handle_media_with_custom_id(client, message)
            # Для числовых команд - просто пропускаем (handle_media_with_thread_command сработает)
            return

        # Если это часть медиа-группы - проверяем, есть ли уже запись
        if message.media_group_id:
            manager_id = message.from_user.id if message.from_user else 0
            # Ищем существующую группу для этого media_group_id
            for key, group_data in manager_media_groups.items():
                if key.startswith(f"{message.media_group_id}_"):
                    # Добавляем сообщение в существующую группу
                    group_data["messages"].append(message)
                    logger.info(f"Добавлено фото в медиа-группу менеджера {key}, всего: {len(group_data['messages'])}")
                    return
            # Если группа не найдена - пропускаем (возможно ещё не создана первым сообщением)
            return

        manager_id = message.from_user.id
        manager_username = message.from_user.username

        logger.info(f"Получено фото от пользователя {manager_id} (username: {manager_username})")
        
        # Проверяем, ожидается ли фото от этого менеджера
        if manager_id in manager_auth_state and manager_auth_state[manager_id] == "waiting_photo":
            # Получаем информацию о менеджере
            manager = get_manager(db_connection, manager_id)

            if manager:
                # Получаем file_id фотографии
                photo_file_id = message.photo.file_id
                photo_channel_message_id = None

                # Сохраняем фото в канал-архив для постоянного хранения
                try:
                    manager_name = manager[2]  # name из managers
                    channel_msg = await client.send_photo(
                        chat_id=FILES_CHANNEL_ID,
                        photo=photo_file_id,
                        caption=f"📸 Фото менеджера: {manager_name} (ID: {manager_id})"
                    )
                    photo_channel_message_id = channel_msg.id
                    logger.info(f"Фото менеджера {manager_id} сохранено в канал, message_id: {photo_channel_message_id}")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении фото в канал: {e}")

                # Создаём кастом эмодзи из фото
                custom_emoji_id = None
                try:
                    await message.reply_text("Создаю кастом эмодзи из вашего фото...")
                    custom_emoji_id = await add_manager_emoji(client, photo_file_id, manager_name)
                    if custom_emoji_id:
                        logger.info(f"Кастом эмодзи для {manager_id} создан: {custom_emoji_id}")
                except Exception as e:
                    logger.error(f"Ошибка создания кастом эмодзи: {e}")

                # Обновляем запись менеджера с фото и убедимся, что username тоже обновлен
                cursor = db_connection.cursor()
                if photo_channel_message_id:
                    cursor.execute('UPDATE managers SET photo_file_id = ?, photo_channel_message_id = ?, username = ?, custom_emoji_id = ? WHERE manager_id = ?',
                                  (photo_file_id, photo_channel_message_id, manager_username, custom_emoji_id, manager_id))
                else:
                    cursor.execute('UPDATE managers SET photo_file_id = ?, username = ?, custom_emoji_id = ? WHERE manager_id = ?',
                                  (photo_file_id, manager_username, custom_emoji_id, manager_id))
                db_connection.commit()

                # Удаляем состояние ожидания
                del manager_auth_state[manager_id]

                # Отправляем подтверждение
                emoji_status = "с кастом эмодзи" if custom_emoji_id else "(без кастом эмодзи)"
                await message.reply_text(
                    f"Фото успешно добавлено {emoji_status}! Ваша авторизация завершена.\n"
                    f"Теперь вы можете отвечать клиентам, используя команду /(номер треда) в теме клиента."
                )

                logger.info(f"Фото менеджера {manager_id} успешно сохранено")
            else:
                await message.reply_text(
                    "Не удалось найти вашу регистрацию. Пожалуйста, используйте сначала команду /auth."
                )
        else:
            # Если фото не ожидается или от другого пользователя, игнорируем
            logger.info(f"Фото проигнорировано, т.к. не ожидалось от менеджера {manager_id}")
    except Exception as e:
        logger.error(f"Ошибка при обработке фото менеджера: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Обработчик для документов/видео/аудио в медиа-группах от менеджера
@business.on_message((filters.document | filters.video | filters.audio) & filters.chat(SUPPORT_GROUP_ID))
async def handle_manager_media_in_group(client, message):
    """Обрабатывает документы/видео/аудио из медиа-групп менеджера"""
    try:
        # Только для медиа-групп без caption (последующие файлы в группе)
        if message.media_group_id and not message.caption:
            manager_id = message.from_user.id if message.from_user else 0
            # Ищем существующую группу для этого media_group_id
            for key, group_data in manager_media_groups.items():
                if key.startswith(f"{message.media_group_id}_"):
                    # Добавляем сообщение в существующую группу
                    group_data["messages"].append(message)
                    logger.info(f"Добавлен файл в медиа-группу менеджера {key}, всего: {len(group_data['messages'])}")
                    return
    except Exception as e:
        logger.error(f"Ошибка при обработке медиа менеджера: {e}")


# Обработчик reply менеджера на сообщение в треде - ОТКЛЮЧЕН
# Теперь reply-логика встроена в команду /{thread_id}
# async def handle_manager_reply_in_thread - см. handle_thread_number_command


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
        _, emoji, name, position, extension, photo_file_id, auth_date, db_username, *_ = manager
        
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

# Обработчик команды ответа по Custom ID - команды вида /Иванов текст (русские буквы + цифры)
@business.on_message(filters.regex(r"^/[А-Яа-я][А-Яа-я0-9]*\s") & filters.chat(SUPPORT_GROUP_ID))
async def handle_custom_id_command(client, message):
    """Обработчик команд с Custom ID клиента (/Иванов текст)"""
    try:
        if not message.from_user:
            await message.reply_text("❌ Невозможно определить отправителя.")
            return

        manager_id = message.from_user.id
        command_text = message.text.strip()

        # Извлекаем custom_id и текст
        parts = command_text.split(maxsplit=1)
        custom_id = parts[0][1:]  # Убираем "/"

        if len(parts) < 2:
            await message.reply_text(f"Укажите текст ответа после /{custom_id}")
            return

        reply_text = parts[1]

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Получаем thread_id и client_id по custom_id
        thread_id, client_id = get_thread_id_by_custom_id(db_connection, custom_id)

        if not thread_id or not client_id:
            await message.reply_text(f"❌ Клиент с ID #{custom_id} не найден")
            return

        # Отправляем ответ клиенту
        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager
        custom_emoji_id = get_manager_custom_emoji_id(db_connection, manager_id)

        full_message, entities = format_signature_with_custom_emoji(
            reply_text, name, position, custom_emoji_id, fallback_emoji=emoji or "👤"
        )

        if entities:
            sent_message = await client.send_message(chat_id=client_id, text=full_message, entities=entities)
        else:
            sent_message = await client.send_message(chat_id=client_id, text=full_message, parse_mode=pyrogram.enums.ParseMode.MARKDOWN)

        # Сохраняем маппинг для reply
        try:
            save_message_mapping(
                db_connection,
                client_message_id=sent_message.id,
                group_message_id=message.id,
                thread_id=thread_id,
                user_id=client_id
            )
        except:
            pass

        # Обновляем статус треда
        update_manager_reply_time(db_connection, thread_id)

        # Назначаем менеджера ответственным если не назначен
        duty_manager = get_duty_manager(db_connection, thread_id)
        if not duty_manager and username:
            assign_duty_manager(db_connection, thread_id, username, manager_id)

        logger.info(f"Ответ по /{custom_id} отправлен клиенту {client_id}")

    except Exception as e:
        logger.error(f"Ошибка при обработке команды /ID: {e}")
        await message.reply_text(f"Ошибка: {e}")


# Кастомный фильтр для медиа с custom_id в caption (русские буквы)
async def custom_id_caption_filter(_, __, message):
    caption = message.caption or ""
    import re
    result = bool(re.match(r"^/[А-Яа-яA-Za-z][А-Яа-яA-Za-z0-9]*", caption))
    if caption.startswith("/") and not caption[1:2].isdigit():
        logger.info(f"[DEBUG] custom_id_caption_filter: caption={caption}, result={result}")
    return result


# Обработчик медиафайлов с Custom ID в подписи (/дс00)
@business.on_message(filters.create(custom_id_caption_filter) & filters.chat(SUPPORT_GROUP_ID) & (filters.photo | filters.document | filters.video | filters.audio | filters.voice))
async def handle_media_with_custom_id(client, message):
    """Обработчик медиа с Custom ID клиента в caption (/Иванов)"""
    try:
        if not message.from_user:
            return
        manager_id = message.from_user.id

        caption_text = message.caption or ""
        parts = caption_text.split(maxsplit=1)
        custom_id = parts[0][1:]  # Убираем "/"

        logger.info(f"[DEBUG] handle_media_with_custom_id: custom_id={custom_id}, media_group_id={message.media_group_id}")

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Получаем thread_id и client_id по custom_id
        thread_id, client_id = get_thread_id_by_custom_id(db_connection, custom_id)

        if not thread_id or not client_id:
            await message.reply_text(f"❌ Клиент с ID #{custom_id} не найден")
            return

        # Если это медиа-группа - обрабатываем отдельно
        if message.media_group_id:
            await handle_manager_media_group(client, message, thread_id, client_id)
            return

        # Парсим текст подписи (после custom_id)
        caption = parts[1] if len(parts) > 1 else None

        # Определяем тип медиафайла
        file_id = None
        media_type = None

        if message.photo:
            file_id = message.photo.file_id
            media_type = "photo"
        elif message.document:
            file_id = message.document.file_id
            media_type = "document"
        elif message.video:
            file_id = message.video.file_id
            media_type = "video"
        elif message.audio:
            file_id = message.audio.file_id
            media_type = "audio"
        elif message.voice:
            file_id = message.voice.file_id
            media_type = "voice"

        # Отправляем медиафайл клиенту
        success = await send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type)

        if success:
            update_manager_reply_time(db_connection, thread_id)
            try:
                await mark_thread_urgent(client, thread_id, is_urgent=False)
            except Exception as e:
                logger.error(f"Ошибка при изменении индикатора треда {thread_id}: {e}")
            logger.info(f"Медиафайл по /{custom_id} отправлен клиенту {client_id}")
        else:
            await message.reply_text("❌ Не удалось отправить медиафайл клиенту.")

    except Exception as e:
        logger.error(f"Ошибка при обработке медиа с custom_id: {e}")
        await message.reply_text(f"Ошибка: {e}")


# Обработчик команды ответа менеджера клиенту - команд вида /{num}, где num - номер треда
@business.on_message(filters.regex(r"^/\d+") & filters.chat(SUPPORT_GROUP_ID))
async def handle_thread_number_command(client, message):
    try:
        if not message.from_user:
            await message.reply_text("❌ Невозможно определить отправителя. Отключите анонимный режим администратора.")
            return
        manager_id = message.from_user.id
        
        # Извлекаем номер треда из первого слова текста, удаляя префикс "/"
        command_text = message.text.strip()
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
        
        # Парсим текст сообщения для получения сообщения
        if " " in command_text:
            reply_text = command_text.split(" ", 1)[1]
        else:
            await message.reply_text(
                f"Пожалуйста, укажите текст ответа после команды /{thread_id}."
            )
            return
        
        # Проверяем, это тред клиента или тред группы
        cursor = db_connection.cursor()
        
        # Проверяем в таблице клиентов
        cursor.execute('SELECT user_id FROM clients WHERE thread_id = ?', (thread_id,))
        client_data = cursor.fetchone()
        
        # Извлекаем данные о менеджере для работы
        _, emoji, name, position, extension, photo_file_id, auth_date, username, *_ = manager
        
        if client_data:
            # Это тред клиента - отправляем личное сообщение
            client_id = client_data[0]

            # Проверяем, есть ли reply на сообщение (для отправки клиенту как reply)
            reply_to_client_msg_id = None
            if message.reply_to_message:
                # Ищем маппинг: group_message_id -> client_message_id
                mapping = get_client_message_id(db_connection, message.reply_to_message.id, thread_id)
                if mapping:
                    reply_to_client_msg_id = mapping[0]
                    logger.info(f"Команда /{thread_id} с reply: group_msg {message.reply_to_message.id} -> client_msg {reply_to_client_msg_id}")

            # Отправляем ответ клиенту (с сохранением маппинга для reply)
            success = await send_manager_reply_to_client(
                client, manager_id, client_id, reply_text,
                group_message_id=message.id,
                thread_id=thread_id,
                reply_to_client_msg_id=reply_to_client_msg_id
            )

            if success:
                # Обновляем время последнего ответа менеджера
                update_manager_reply_time(db_connection, thread_id)
                
                # Пытаемся убрать индикатор срочности из заголовка треда
                try:
                    await mark_thread_urgent(client, thread_id, is_urgent=False)
                except Exception as e:
                    logger.error(f"Ошибка при изменении визуального индикатора треда {thread_id}: {e}")
                
                # Не отправляем подтверждение в группу, только записываем в лог
                logger.info(f"Ответ менеджера {manager_id} отправлен клиенту {client_id}, статус треда обновлен")
                
                # Сохраняем информацию о первом ответе менеджера, если это первый ответ
                if is_first_reply(db_connection, thread_id, manager_id):
                    save_first_reply(db_connection, thread_id, client_id, manager_id)
                    logger.info(f"Сохранена информация о первом ответе менеджера {manager_id} для треда {thread_id}")
            else:
                # Только в случае ошибки уведомляем менеджера
                await message.reply_text(
                    "❌ Не удалось отправить ответ клиенту."
                )
        else:
            # Проверяем в таблице групп
            cursor.execute('SELECT group_id, group_title FROM group_threads WHERE thread_id = ?', (thread_id,))
            group_data = cursor.fetchone()
            
            if group_data:
                # Это тред группы - отправляем сообщение в группу
                group_id, group_title = group_data

                # Формируем подпись менеджера (моноширинным)
                signature = f"\n\n`{emoji} {name}, {position}`"

                # Полное сообщение с подписью
                full_message = f"{reply_text}{signature}"

                try:
                    # Отправляем сообщение в группу
                    sent_message = await client.send_message(
                        chat_id=group_id,
                        text=full_message,
                        parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                    )
                    
                    # Обновляем время последнего ответа менеджера
                    update_manager_reply_time(db_connection, thread_id)
                    
                    # Если нет ответственного менеджера, назначаем его
                    cursor.execute('SELECT manager_username FROM duty_managers WHERE thread_id = ?', (thread_id,))
                    duty_manager = cursor.fetchone()
                    
                    if not duty_manager and username:
                        # Назначаем отвечающего менеджера ответственным
                        assign_duty_manager(db_connection, thread_id, username, manager_id)
                        logger.info(f"Автоматически назначен ответственный менеджер {username} для треда {thread_id}")
                    
                    # Отправляем подтверждение только менеджеру
                    await message.reply_text(f"✅ Ответ отправлен в группу {group_title}.")
                    
                    # Сохраняем информацию о первом ответе менеджера, если это первый ответ
                    cursor.execute('SELECT user_id FROM clients WHERE user_id = ?', (group_id,))  # Используем group_id как user_id
                    if not cursor.fetchone():
                        # Создаем запись в clients, чтобы можно было использовать first_replies
                        cursor.execute('''
                        INSERT OR IGNORE INTO clients (user_id, first_name, thread_id)
                        VALUES (?, ?, ?)
                        ''', (group_id, group_title, thread_id))
                        db_connection.commit()
                    
                    if is_first_reply(db_connection, thread_id, manager_id):
                        save_first_reply(db_connection, thread_id, group_id, manager_id)
                        logger.info(f"Сохранена информация о первом ответе менеджера {manager_id} для треда {thread_id}")
                    
                    logger.info(f"Ответ менеджера {manager_id} отправлен в группу {group_id}, статус треда обновлен")
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке сообщения в группу {group_id}: {e}")
                    await message.reply_text(f"❌ Ошибка при отправке сообщения в группу: {e}")
            else:
                # Fallback: пробуем найти клиента по user_id (если ввели client_id вместо thread_id)
                cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (thread_id,))
                client_by_user_id = cursor.fetchone()

                if client_by_user_id:
                    # Нашли клиента по user_id - используем его thread_id
                    actual_thread_id = client_by_user_id[0]
                    client_id = thread_id  # thread_id на самом деле был user_id

                    logger.info(f"Fallback: найден клиент {client_id} по user_id, thread_id={actual_thread_id}")

                    success = await send_manager_reply_to_client(
                        client, manager_id, client_id, reply_text,
                        group_message_id=message.id,
                        thread_id=actual_thread_id
                    )

                    if success:
                        update_manager_reply_time(db_connection, actual_thread_id)
                        try:
                            await mark_thread_urgent(client, actual_thread_id, is_urgent=False)
                        except Exception as e:
                            logger.error(f"Ошибка при изменении индикатора треда {actual_thread_id}: {e}")
                        logger.info(f"Ответ менеджера {manager_id} отправлен клиенту {client_id} (по user_id)")
                    else:
                        await message.reply_text("❌ Не удалось отправить ответ клиенту.")
                else:
                    await message.reply_text(
                        f"Не удалось найти клиента или группу для /{thread_id}.\n"
                        f"Используйте номер треда (маленькое число) или ID клиента."
                    )
    except ValueError:
        # Если номер треда не удалось преобразовать в число
        logger.error(f"Некорректный формат команды: {message.text}")
        await message.reply_text("Некорректный формат команды. Используйте: /{thread_id} {текст ответа}")
    except Exception as e:
        logger.error(f"Ошибка при обработке команды ответа: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")
        
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
        else:
            await message.reply_text("❌ Не удалось отправить карточку клиенту.")
            
    except Exception as e:
        logger.error(f"Ошибка при отправке карточки: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")

# Команда /id - получить/создать Custom ID для клиента в треде
@business.on_message(filters.command("id") & filters.chat(SUPPORT_GROUP_ID))
async def handle_set_custom_id(client, message):
    """Установить или показать Custom ID клиента.
    Использование: /id [thread_id] [ИмяКлиента] или /id [thread_id]"""
    try:
        if not message.from_user:
            await message.reply_text("❌ Невозможно определить отправителя.")
            return

        manager_id = message.from_user.id

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Парсим команду: /id [thread_id] [ИмяКлиента]
        command_parts = message.text.split()

        if len(command_parts) < 2:
            await message.reply_text(
                "**Использование:**\n"
                "`/id [thread_id]` - показать ID клиента\n"
                "`/id [thread_id] [ИмяКлиента]` - установить ID\n\n"
                "**Пример:** `/id 123456 Иванов`"
            )
            return

        # Получаем thread_id
        try:
            thread_id = int(command_parts[1])
        except ValueError:
            await message.reply_text("❌ thread_id должен быть числом")
            return

        # Получаем клиента по thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text(f"❌ Клиент не найден для треда #{thread_id}")
            return

        user_id = client_data[0]
        first_name = client_data[1] or ""
        last_name = client_data[2] or ""
        username = client_data[3] or ""
        client_name = f"{first_name} {last_name}".strip()
        if username:
            client_name += f" (@{username})"

        if len(command_parts) >= 3:
            # Устанавливаем новый ID
            new_id = command_parts[2].strip()

            result_id, error = set_custom_id(db_connection, user_id, new_id)

            if error:
                await message.reply_text(f"❌ {error}")
                return

            # Обновляем заголовок треда с новым custom_id
            from database import format_thread_title, get_company_name
            company_name = get_company_name(db_connection, thread_id)
            new_title = format_thread_title(thread_id, first_name, last_name, company_name, result_id)
            await edit_thread_title(client, thread_id, new_title)

            await message.reply_text(
                f"✅ **ID установлен:** `#{result_id}`\n"
                f"**Клиент:** {client_name}\n"
                f"**Thread:** {thread_id}\n\n"
                f"Используйте `/{result_id} текст` для ответа\n"
                f"Заголовок треда обновлён"
            )
            logger.info(f"Менеджер {manager_id} установил ID #{new_id} для клиента {user_id}")
        else:
            # Показываем текущий ID
            current_id = get_custom_id(db_connection, user_id)

            if current_id:
                await message.reply_text(
                    f"**ID клиента:** `#{current_id}`\n"
                    f"**Имя:** {client_name}\n"
                    f"**Thread:** {thread_id}\n\n"
                    f"Используйте `/{current_id} текст` для ответа\n"
                    f"Изменить: `/id {thread_id} НовыйID`"
                )
            else:
                await message.reply_text(
                    f"**Клиент:** {client_name}\n"
                    f"**Thread:** {thread_id}\n\n"
                    f"❌ ID не установлен\n"
                    f"Задать: `/id {thread_id} ИмяКлиента`\n"
                    f"_(только буквы и цифры)_"
                )

    except Exception as e:
        logger.error(f"Ошибка при работе с Custom ID: {e}")
        await message.reply_text(f"Ошибка: {e}")


@business.on_message(filters.command("company") & filters.chat(SUPPORT_GROUP_ID))
async def handle_set_company(client, message):
    """Установить название компании для клиента.
    Использование: /company [thread_id] [Название компании]"""
    try:
        if not message.from_user:
            await message.reply_text("❌ Невозможно определить отправителя.")
            return

        manager_id = message.from_user.id

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Парсим команду: /company [thread_id] [Название компании]
        command_parts = message.text.split(maxsplit=2)

        if len(command_parts) < 2:
            await message.reply_text(
                "**Использование:**\n"
                "`/company [thread_id]` - показать компанию\n"
                "`/company [thread_id] [Название]` - установить компанию\n\n"
                "**Пример:** `/company 4 ООО Ромашка`"
            )
            return

        # Получаем thread_id
        try:
            thread_id = int(command_parts[1])
        except ValueError:
            await message.reply_text("❌ thread_id должен быть числом")
            return

        # Получаем клиента по thread_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text(f"❌ Клиент не найден для треда #{thread_id}")
            return

        first_name = client_data[1] or ""
        last_name = client_data[2] or ""
        custom_id = client_data[8] if len(client_data) > 8 else None
        client_name = f"{first_name} {last_name}".strip()

        if len(command_parts) >= 3:
            # Устанавливаем название компании
            company_name = command_parts[2].strip()

            from database import set_company_name, format_thread_title
            success = set_company_name(db_connection, thread_id, company_name)

            if success:
                # Обновляем заголовок треда через централизованную функцию
                new_title = format_thread_title(thread_id, first_name, last_name, company_name, custom_id)
                await edit_thread_title(client, thread_id, new_title)

                await message.reply_text(
                    f"✅ **Компания установлена:** {company_name}\n"
                    f"**Клиент:** {client_name}\n"
                    f"**Thread:** {thread_id}\n\n"
                    f"Заголовок треда обновлён"
                )
                logger.info(f"Менеджер {manager_id} установил компанию '{company_name}' для треда {thread_id}")
            else:
                await message.reply_text(f"❌ Не удалось установить компанию для треда {thread_id}")
        else:
            # Показываем текущую компанию
            from database import get_company_name
            current_company = get_company_name(db_connection, thread_id)

            if current_company:
                await message.reply_text(
                    f"**Компания:** {current_company}\n"
                    f"**Клиент:** {client_name}\n"
                    f"**Thread:** {thread_id}\n\n"
                    f"Изменить: `/company {thread_id} НоваяКомпания`"
                )
            else:
                await message.reply_text(
                    f"**Клиент:** {client_name}\n"
                    f"**Thread:** {thread_id}\n\n"
                    f"❌ Компания не установлена\n"
                    f"Задать: `/company {thread_id} Название компании`"
                )

    except Exception as e:
        logger.error(f"Ошибка при работе с компанией: {e}")
        await message.reply_text(f"Ошибка: {e}")


# === Файлы и шаблоны ===

# Состояние ожидания текста шаблона
pending_template_text = {}  # user_id -> {"name": str, "message_id": int}


@business.on_message(filters.command("upload") & filters.chat(SUPPORT_GROUP_ID))
async def handle_upload_file(client, message):
    """Загрузить файл в хранилище. Только для админов.
    Использование: /upload (reply на файл с подписью = имя)"""
    try:
        if not message.from_user:
            return

        manager_id = message.from_user.id

        # Проверяем что это админ группы
        try:
            member = await client.get_chat_member(SUPPORT_GROUP_ID, manager_id)
            if member.status not in [pyrogram.enums.ChatMemberStatus.ADMINISTRATOR, pyrogram.enums.ChatMemberStatus.OWNER]:
                await message.reply_text("❌ Загружать файлы могут только администраторы")
                return
        except Exception:
            await message.reply_text("❌ Не удалось проверить права")
            return

        # Проверяем что это reply на сообщение с файлом
        if not message.reply_to_message:
            await message.reply_text(
                "**Загрузка файла в хранилище**\n\n"
                "Отправьте файл с подписью (имя файла), затем reply на него с `/upload`\n\n"
                "**Пример:**\n"
                "1. Отправить файл с подписью `реквизиты`\n"
                "2. Reply на него: `/upload`"
            )
            return

        reply_msg = message.reply_to_message

        # Проверяем что есть файл
        if not (reply_msg.document or reply_msg.photo or reply_msg.video or reply_msg.audio):
            await message.reply_text("❌ Ответьте на сообщение с файлом")
            return

        # Имя файла из подписи
        file_name = reply_msg.caption
        if not file_name:
            await message.reply_text("❌ У файла должна быть подпись (имя файла)")
            return

        file_name = file_name.strip().lower()

        # Определяем тип файла
        if reply_msg.document:
            file_type = "document"
            file_id = reply_msg.document.file_id
        elif reply_msg.photo:
            file_type = "photo"
            file_id = reply_msg.photo.file_id
        elif reply_msg.video:
            file_type = "video"
            file_id = reply_msg.video.file_id
        elif reply_msg.audio:
            file_type = "audio"
            file_id = reply_msg.audio.file_id

        # Проверяем существует ли файл с таким именем
        from database import get_file_template, save_file_template
        existing = get_file_template(db_connection, file_name)

        if existing:
            # Файл существует - спрашиваем обновить ли
            await message.reply_text(
                f"⚠️ **Файл `{file_name}` уже существует**\n\n"
                f"Шаблон: {existing[2] or '(нет)'}\n\n"
                f"Чтобы обновить, используйте `/upload update`"
            )
            return

        # Пересылаем файл в канал-хранилище
        try:
            forwarded = await reply_msg.forward(FILES_CHANNEL_ID)
            channel_message_id = forwarded.id
        except Exception as e:
            logger.error(f"Ошибка пересылки в канал: {e}")
            await message.reply_text(f"❌ Ошибка загрузки в хранилище: {e}")
            return

        # Сохраняем в БД
        save_file_template(db_connection, file_name, channel_message_id, None, file_type, manager_id)

        # Спрашиваем текст шаблона
        pending_template_text[manager_id] = {"name": file_name, "message_id": message.id}

        await message.reply_text(
            f"✅ **Файл `{file_name}` загружен**\n\n"
            f"Теперь введите текст шаблона для `/temp {file_name}`\n"
            f"или отправьте `-` чтобы пропустить"
        )
        logger.info(f"Файл '{file_name}' загружен менеджером {manager_id}")

    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        await message.reply_text(f"Ошибка: {e}")


@business.on_message(filters.command("file") & filters.chat(SUPPORT_GROUP_ID))
async def handle_send_file(client, message):
    """Отправить файл клиенту.
    Использование: /file [имя] или в треде просто /file [имя]"""
    try:
        if not message.from_user:
            return

        manager_id = message.from_user.id

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Парсим команду
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            # Показываем список файлов
            from database import list_file_templates
            files = list_file_templates(db_connection)
            if not files:
                await message.reply_text("📁 Хранилище пусто\n\nЗагрузите файлы командой `/upload`")
                return

            file_list = "\n".join([f"• `{f[0]}` {f[2] or ''}" for f in files])
            await message.reply_text(f"📁 **Доступные файлы:**\n\n{file_list}\n\n`/file [имя]` - отправить клиенту")
            return

        file_name = parts[1].strip().lower()

        # Получаем thread_id из топика
        thread_id = message.message_thread_id
        if not thread_id:
            await message.reply_text("❌ Используйте команду в треде клиента")
            return

        # Получаем client_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text("❌ Клиент не найден")
            return

        client_id = client_data[0]

        # Получаем файл
        from database import get_file_template
        file_info = get_file_template(db_connection, file_name)
        if not file_info:
            await message.reply_text(f"❌ Файл `{file_name}` не найден\n\nСписок файлов: `/file`")
            return

        channel_message_id = file_info[1]
        file_type = file_info[3]

        # Получаем сообщение из канала и пересылаем клиенту
        try:
            channel_msg = await client.get_messages(FILES_CHANNEL_ID, channel_message_id)

            if file_type == "photo":
                await client.send_photo(client_id, channel_msg.photo.file_id)
            elif file_type == "video":
                await client.send_video(client_id, channel_msg.video.file_id)
            elif file_type == "audio":
                await client.send_audio(client_id, channel_msg.audio.file_id)
            else:
                await client.send_document(client_id, channel_msg.document.file_id)

            await message.reply_text(f"✅ Файл `{file_name}` отправлен клиенту")
            logger.info(f"Файл '{file_name}' отправлен клиенту {client_id} менеджером {manager_id}")

        except Exception as e:
            logger.error(f"Ошибка отправки файла: {e}")
            await message.reply_text(f"❌ Ошибка отправки: {e}")

    except Exception as e:
        logger.error(f"Ошибка команды /file: {e}")
        await message.reply_text(f"Ошибка: {e}")


@business.on_message(filters.command("files") & filters.chat(SUPPORT_GROUP_ID))
async def handle_list_files(client, message):
    """Показать список доступных файлов и шаблонов"""
    try:
        from database import list_file_templates
        files = list_file_templates(db_connection)

        if not files:
            await message.reply_text(
                "📁 **Хранилище пусто**\n\n"
                "Загрузите файлы командой `/upload`"
            )
            return

        # Формируем список
        lines = []
        for f in files:
            name, template_text, file_type, uploaded_at = f
            type_emoji = {"photo": "🖼", "video": "🎬", "audio": "🎵", "document": "📄"}.get(file_type, "📁")
            has_template = "📝" if template_text else ""
            lines.append(f"{type_emoji} `{name}` {has_template}")

        file_list = "\n".join(lines)

        await message.reply_text(
            f"📁 **Доступные файлы ({len(files)}):**\n\n"
            f"{file_list}\n\n"
            f"📝 = есть шаблон\n\n"
            f"`/file [имя]` - отправить файл\n"
            f"`/temp [имя]` - отправить с шаблоном"
        )

    except Exception as e:
        logger.error(f"Ошибка команды /files: {e}")
        await message.reply_text(f"Ошибка: {e}")


@business.on_message(filters.command("temp") & filters.chat(SUPPORT_GROUP_ID))
async def handle_send_template(client, message):
    """Отправить файл + шаблонное сообщение клиенту.
    Использование: /temp [имя]"""
    try:
        if not message.from_user:
            return

        manager_id = message.from_user.id

        # Проверяем авторизацию
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Парсим команду
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            # Показываем список шаблонов
            from database import list_file_templates
            files = list_file_templates(db_connection)
            templates = [f for f in files if f[1]]  # Только с текстом шаблона
            if not templates:
                await message.reply_text("📝 Шаблонов нет\n\nЗагрузите файлы с шаблонами через `/upload`")
                return

            templ_list = "\n".join([f"• `{t[0]}`: _{t[1][:30]}..._" if len(t[1] or '') > 30 else f"• `{t[0]}`: _{t[1]}_" for t in templates])
            await message.reply_text(f"📝 **Доступные шаблоны:**\n\n{templ_list}\n\n`/temp [имя]` - отправить клиенту")
            return

        file_name = parts[1].strip().lower()

        # Получаем thread_id из топика
        thread_id = message.message_thread_id
        if not thread_id:
            await message.reply_text("❌ Используйте команду в треде клиента")
            return

        # Получаем client_id
        client_data = get_client_by_thread(db_connection, thread_id)
        if not client_data:
            await message.reply_text("❌ Клиент не найден")
            return

        client_id = client_data[0]

        # Получаем файл
        from database import get_file_template
        file_info = get_file_template(db_connection, file_name)
        if not file_info:
            await message.reply_text(f"❌ Шаблон `{file_name}` не найден\n\nСписок: `/temp`")
            return

        channel_message_id = file_info[1]
        template_text = file_info[2]
        file_type = file_info[3]

        if not template_text:
            await message.reply_text(f"❌ У файла `{file_name}` нет текста шаблона\n\nИспользуйте `/file {file_name}` для отправки без текста")
            return

        # Формируем подпись менеджера
        manager_name = manager[2] or "Менеджер"
        manager_position = manager[3] or ""
        manager_ext = manager[4] or ""

        signature_parts = [manager_name]
        if manager_position:
            signature_parts.append(manager_position)

        signature = f"`{manager[1]} {', '.join(signature_parts)}`"
        full_text = f"{template_text}\n—\n{signature}"

        # Получаем сообщение из канала и отправляем клиенту
        try:
            channel_msg = await client.get_messages(FILES_CHANNEL_ID, channel_message_id)

            if file_type == "photo":
                await client.send_photo(client_id, channel_msg.photo.file_id, caption=full_text)
            elif file_type == "video":
                await client.send_video(client_id, channel_msg.video.file_id, caption=full_text)
            elif file_type == "audio":
                await client.send_audio(client_id, channel_msg.audio.file_id, caption=full_text)
            else:
                await client.send_document(client_id, channel_msg.document.file_id, caption=full_text)

            await message.reply_text(f"✅ Шаблон `{file_name}` отправлен клиенту")
            logger.info(f"Шаблон '{file_name}' отправлен клиенту {client_id} менеджером {manager_id}")

        except Exception as e:
            logger.error(f"Ошибка отправки шаблона: {e}")
            await message.reply_text(f"❌ Ошибка отправки: {e}")

    except Exception as e:
        logger.error(f"Ошибка команды /temp: {e}")
        await message.reply_text(f"Ошибка: {e}")


@business.on_message(filters.text & filters.chat(SUPPORT_GROUP_ID))
async def handle_template_text_input(client, message):
    """Обработка ввода текста шаблона после /upload"""
    try:
        if not message.from_user:
            return

        # Пропускаем команды
        if message.text and message.text.startswith('/'):
            return

        manager_id = message.from_user.id

        # Проверяем ожидаем ли текст шаблона от этого менеджера
        if manager_id not in pending_template_text:
            return

        pending = pending_template_text[manager_id]
        file_name = pending["name"]

        # Удаляем из ожидания
        del pending_template_text[manager_id]

        text = message.text.strip()

        if text == "-":
            await message.reply_text(f"✅ Файл `{file_name}` сохранён без шаблона")
            return

        # Сохраняем текст шаблона
        from database import update_template_text
        update_template_text(db_connection, file_name, text)

        await message.reply_text(
            f"✅ **Шаблон сохранён для `{file_name}`**\n\n"
            f"Текст: _{text[:100]}{'...' if len(text) > 100 else ''}_\n\n"
            f"Используйте `/temp {file_name}` для отправки"
        )
        logger.info(f"Текст шаблона для '{file_name}' сохранён")

    except Exception as e:
        logger.error(f"Ошибка сохранения текста шаблона: {e}")


# Функция для изменения заголовка треда
async def edit_thread_title(client, thread_id, new_title):
    try:
        logger.info(f"Попытка изменить заголовок треда {thread_id} на '{new_title}'")

        peer = await client.resolve_peer(SUPPORT_GROUP_ID)
        await client.invoke(
            pyrogram.raw.functions.messages.EditForumTopic(
                peer=peer,
                topic_id=thread_id,
                title=new_title
            )
        )

        logger.info(f"Заголовок треда {thread_id} успешно изменен на '{new_title}'")
        return True
    except Exception as e:
        # Игнорируем ошибку "TOPIC_NOT_MODIFIED" - заголовок уже такой
        if "TOPIC_NOT_MODIFIED" in str(e):
            logger.debug(f"Тред {thread_id}: заголовок уже '{new_title}'")
            return True
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


# Команда /del - удалить сообщение у клиента (reply на своё сообщение)
@business.on_message(filters.command("del") & filters.chat(SUPPORT_GROUP_ID) & filters.reply)
async def handle_delete_message(client, message):
    """Удалить сообщение у клиента (менеджер делает reply на своё сообщение и пишет /del)"""
    try:
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("❌ Вы не авторизованы")
            return

        reply_to_msg = message.reply_to_message
        if not reply_to_msg:
            await message.reply_text("❌ Сделайте reply на сообщение, которое хотите удалить")
            return

        # Получаем thread_id
        thread_id = message.message_thread_id

        # Ищем маппинг: group_message_id -> client_message_id
        mapping = get_client_message_id(db_connection, reply_to_msg.id, thread_id)
        if not mapping:
            await message.reply_text("❌ Не найдено соответствующее сообщение у клиента")
            return

        client_msg_id, client_id = mapping

        # Удаляем сообщение у клиента
        try:
            await client.delete_messages(chat_id=client_id, message_ids=client_msg_id)
            await message.reply_text("✅ Сообщение удалено у клиента")
            logger.info(f"Менеджер {manager_id} удалил сообщение {client_msg_id} у клиента {client_id}")
        except Exception as e:
            await message.reply_text(f"❌ Не удалось удалить сообщение: {e}")
            logger.error(f"Ошибка при удалении сообщения: {e}")

    except Exception as e:
        logger.error(f"Ошибка при обработке команды /del: {e}")
        await message.reply_text(f"Ошибка: {e}")


# Обработчик команды /ok для сброса текущего уведомления
# Используем regex вместо command, т.к. command требует entity bot_command от Telegram
@business.on_message(filters.regex(r'^/ok(\s|$)') & filters.chat(SUPPORT_GROUP_ID) & ~filters.outgoing)
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
  
  # 3. Новая команда для просмотра ответственных менеджеров
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


# Команда /tier - установить тир клиента
@business.on_message(filters.command("tier") & filters.chat(SUPPORT_GROUP_ID))
async def handle_tier_command(client, message):
    """
    Установить тир клиента (tier-1, tier-2, tier-3).
    Использование: /tier [thread_id] [1|2|3]
    """
    try:
        # Проверяем, авторизован ли менеджер
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Используйте /auth для авторизации."
            )
            return

        # Парсим аргументы
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.reply_text(
                "**Использование:** `/tier [ID_треда] [1|2|3]`\n\n"
                "Устанавливает тир клиента:\n"
                "• tier-1 - базовый\n"
                "• tier-2 - средний\n"
                "• tier-3 - премиум\n\n"
                "Пример: `/tier 123 2`"
            )
            return

        try:
            thread_id = int(args[0])
            tier = int(args[1])
        except ValueError:
            await message.reply_text("ID треда и тир должны быть числами.")
            return

        if tier not in (1, 2, 3):
            await message.reply_text("Тир должен быть 1, 2 или 3.")
            return

        from database import set_client_tier, get_client_by_thread
        client_info = get_client_by_thread(db_connection, thread_id)
        if not client_info:
            await message.reply_text(f"Клиент с тредом {thread_id} не найден.")
            return

        success, error = set_client_tier(db_connection, thread_id, tier)
        if success:
            client_name = client_info[1] or "Клиент"
            await message.reply_text(f"✅ Клиенту **{client_name}** (тред {thread_id}) установлен **tier-{tier}**")
            logger.info(f"Менеджер {manager_id} установил tier-{tier} для треда {thread_id}")
        else:
            await message.reply_text(f"Ошибка: {error}")

    except Exception as e:
        logger.error(f"Ошибка при установке тира: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Команда /vacation - отправить менеджера в отпуск/на больничный
@business.on_message(filters.command("vacation") & filters.chat(SUPPORT_GROUP_ID))
async def handle_vacation_command(client, message):
    """
    Отправить менеджера в отпуск - передать его треды заместителю.
    Использование: /vacation @менеджер @заместитель [tier-1 tier-2 tier-3]
    """
    try:
        # Проверяем, авторизован ли менеджер
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Используйте /auth для авторизации."
            )
            return

        # Парсим аргументы
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.reply_text(
                "**Использование:** `/vacation @менеджер @заместитель [тиры]`\n\n"
                "Передаёт треды менеджера заместителю на время отпуска/больничного.\n\n"
                "**Опционально:** укажите тиры для частичной передачи:\n"
                "`/vacation @ivan @petr tier-1 tier-2`\n\n"
                "Для возврата используйте `/return @менеджер`"
            )
            return

        original_username = args[0].lstrip('@')
        substitute_username = args[1].lstrip('@')

        if original_username == substitute_username:
            await message.reply_text("Менеджер не может замещать сам себя.")
            return

        # Парсим тиры если указаны
        tiers = None
        if len(args) > 2:
            tiers = []
            for arg in args[2:]:
                arg_lower = arg.lower().replace('tier-', '').replace('tier', '')
                try:
                    tier = int(arg_lower)
                    if tier in (1, 2, 3):
                        tiers.append(tier)
                except ValueError:
                    pass
            if not tiers:
                tiers = None

        # Проверяем, не в отпуске ли уже менеджер
        from database import is_on_vacation, start_vacation, get_manager_threads
        if is_on_vacation(db_connection, original_username):
            await message.reply_text(
                f"@{original_username} уже в отпуске. "
                f"Сначала верните его командой `/return @{original_username}`"
            )
            return

        # Проверяем, есть ли треды у менеджера
        threads = get_manager_threads(db_connection, original_username)
        if not threads:
            await message.reply_text(f"У @{original_username} нет назначенных тредов.")
            return

        # Начинаем замещение
        count = start_vacation(db_connection, original_username, substitute_username, manager_id, tiers)

        tier_info = ""
        if tiers:
            tier_info = f" (тиры: {', '.join(f'tier-{t}' for t in tiers)})"

        await message.reply_text(
            f"🏖 **@{original_username}** в отпуске!\n\n"
            f"Передано **{count}** тредов{tier_info} → @{substitute_username}\n\n"
            f"Для возврата: `/return @{original_username}`"
        )
        logger.info(f"Менеджер {manager_id} отправил @{original_username} в отпуск, {count} тредов → @{substitute_username}")

    except Exception as e:
        logger.error(f"Ошибка при отправке в отпуск: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Команда /return - вернуть менеджера из отпуска
@business.on_message(filters.command("return") & filters.chat(SUPPORT_GROUP_ID))
async def handle_return_command(client, message):
    """
    Вернуть менеджера из отпуска - вернуть ему его треды.
    Использование: /return @менеджер
    """
    try:
        # Проверяем, авторизован ли менеджер
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Используйте /auth для авторизации."
            )
            return

        # Парсим аргументы
        args = message.text.split()[1:]
        if len(args) < 1:
            await message.reply_text(
                "**Использование:** `/return @менеджер`\n\n"
                "Возвращает менеджера из отпуска и отдаёт ему его треды.\n"
                "Примечание: треды, которые были переназначены другим менеджерам, не возвращаются."
            )
            return

        target_username = args[0].lstrip('@')

        # Проверяем, в отпуске ли менеджер
        from database import is_on_vacation, end_vacation, get_vacation_info
        if not is_on_vacation(db_connection, target_username):
            await message.reply_text(f"@{target_username} не в отпуске.")
            return

        # Получаем информацию о замещении для отчёта
        vacation_info = get_vacation_info(db_connection, target_username)

        # Завершаем замещение
        count = end_vacation(db_connection, target_username, manager_id)

        await message.reply_text(
            f"✅ **@{target_username}** вернулся!\n\n"
            f"Возвращено **{count}** тредов."
        )
        logger.info(f"Менеджер {manager_id} вернул @{target_username} из отпуска, возвращено {count} тредов")

    except Exception as e:
        logger.error(f"Ошибка при возврате из отпуска: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Команда /team - показать карточки всех менеджеров (для входящих сообщений от других менеджеров)
# Outgoing сообщения обрабатываются роутером command_router
@business.on_message(filters.command("team") & filters.chat(SUPPORT_GROUP_ID))
async def handle_team_command(client, message):
    """
    Показать карточки всех менеджеров.
    Для увольнения: reply на карточку + /fire @новый_менеджер
    """
    try:
        # Проверяем, авторизован ли менеджер
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Используйте /auth для авторизации."
            )
            return

        # Получаем всех менеджеров
        from database import get_all_managers, get_manager_threads, is_on_vacation
        managers = get_all_managers(db_connection)

        if not managers:
            await message.reply_text("Нет зарегистрированных менеджеров.")
            return

        await message.reply_text(
            f"👥 **Команда менеджеров** ({len(managers)})\n\n"
            "Для увольнения: reply на карточку + `/fire @новый_менеджер`"
        )

        # Отправляем карточку каждого менеджера
        for mgr in managers:
            mgr_id, username, name = mgr
            threads = get_manager_threads(db_connection, username) if username else []
            on_vacation = is_on_vacation(db_connection, username) if username else False

            status = "🏖 В отпуске" if on_vacation else "✅ Активен"
            username_str = f"@{username}" if username else "без username"

            card_text = (
                f"**{name or 'Без имени'}**\n"
                f"ID: `{mgr_id}`\n"
                f"Username: {username_str}\n"
                f"Тредов: {len(threads)}\n"
                f"Статус: {status}"
            )

            await client.send_message(
                chat_id=message.chat.id,
                text=card_text,
                message_thread_id=message.message_thread_id
            )

    except Exception as e:
        logger.error(f"Ошибка при показе команды: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Команда /fire - уволить менеджера (reply на карточку из /team)
@business.on_message(filters.command("fire") & filters.chat(SUPPORT_GROUP_ID))
async def handle_fire_command(client, message):
    """
    Уволить менеджера - передать треды и удалить из группы.
    Использование: reply на карточку менеджера + /fire @новый_менеджер
    """
    try:
        # Проверяем, авторизован ли менеджер
        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text(
                "Вы не авторизованы в системе. Используйте /auth для авторизации."
            )
            return

        # Проверяем, что это reply на сообщение
        if not message.reply_to_message:
            await message.reply_text(
                "**Использование:**\n"
                "1. Вызовите `/team` для просмотра карточек менеджеров\n"
                "2. Сделайте reply на карточку увольняемого\n"
                "3. Напишите `/fire @новый_менеджер`\n\n"
                "Треды увольняемого будут переданы новому менеджеру."
            )
            return

        # Парсим аргументы
        args = message.text.split()[1:]
        if len(args) < 1:
            await message.reply_text(
                "Укажите менеджера для передачи тредов: `/fire @новый_менеджер`"
            )
            return

        new_manager_username = args[0].lstrip('@')

        # Извлекаем ID менеджера из карточки
        reply_text = message.reply_to_message.text or ""
        import re
        id_match = re.search(r'ID:\s*`?(\d+)`?', reply_text)

        if not id_match:
            await message.reply_text(
                "Не удалось определить менеджера из карточки.\n"
                "Используйте `/team` и сделайте reply на карточку менеджера."
            )
            return

        fired_manager_id = int(id_match.group(1))

        # Получаем данные увольняемого менеджера
        fired_manager = get_manager(db_connection, fired_manager_id)
        if not fired_manager:
            await message.reply_text("Менеджер не найден в системе.")
            return

        # fired_manager: (manager_id, emoji, name, position, extension, photo_file_id, auth_date, username)
        fired_username = fired_manager[7] if len(fired_manager) > 7 else None
        fired_name = fired_manager[2] or "Менеджер"

        if not fired_username:
            await message.reply_text("У увольняемого менеджера нет username.")
            return

        if fired_username == new_manager_username:
            await message.reply_text("Нельзя передать треды самому себе.")
            return

        # Переназначаем все треды
        from database import reassign_all_threads, remove_manager as db_remove_manager
        threads_count = reassign_all_threads(db_connection, fired_username, new_manager_username, manager_id)

        # Удаляем менеджера из системы
        success, _ = db_remove_manager(db_connection, fired_username)

        # Пытаемся удалить из группы
        kick_result = ""
        try:
            await client.ban_chat_member(
                chat_id=SUPPORT_GROUP_ID,
                user_id=fired_manager_id
            )
            # Сразу разбаним, чтобы мог вернуться если нужно
            await client.unban_chat_member(
                chat_id=SUPPORT_GROUP_ID,
                user_id=fired_manager_id
            )
            kick_result = "\n🚪 Удалён из группы"
        except Exception as kick_error:
            logger.warning(f"Не удалось удалить из группы: {kick_error}")
            kick_result = "\n⚠️ Не удалось удалить из группы (нет прав)"

        await message.reply_text(
            f"🔥 **{fired_name}** (@{fired_username}) уволен!\n\n"
            f"Передано **{threads_count}** тредов → @{new_manager_username}\n"
            f"Удалён из базы данных{kick_result}"
        )
        logger.info(f"Менеджер {manager_id} уволил @{fired_username}, {threads_count} тредов → @{new_manager_username}")

    except Exception as e:
        logger.error(f"Ошибка при увольнении менеджера: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# Команда /newclient - создать тред для нового клиента по username
@business.on_message(filters.command("newclient") & filters.chat(SUPPORT_GROUP_ID))
async def handle_newclient_command(client, message):
    """
    Создать тред для нового клиента по username.
    Использование: /newclient @username (только в general)
    """
    try:
        # Проверяем что команда вызвана в general (не в треде)
        if message.message_thread_id:
            await message.reply_text(
                "❌ Команда `/newclient` работает только в **General**.\n"
                "Выйдите из треда и отправьте команду снова."
            )
            return

        # Проверяем авторизацию менеджера
        if not message.from_user:
            await message.reply_text("❌ Невозможно определить отправителя.")
            return

        manager_id = message.from_user.id
        manager = get_manager(db_connection, manager_id)
        if not manager:
            await message.reply_text("Вы не авторизованы. Используйте /auth")
            return

        # Парсим username
        args = message.text.split()[1:]
        if not args:
            await message.reply_text(
                "**Использование:**\n"
                "`/newclient @username`\n\n"
                "Создаёт тред для клиента. После создания можно писать клиенту через `/[thread_id] текст`"
            )
            return

        username = args[0].lstrip('@')
        if not username:
            await message.reply_text("❌ Укажите username клиента")
            return

        # Получаем информацию о пользователе
        try:
            user = await client.get_users(username)
        except Exception as e:
            logger.error(f"Ошибка при получении пользователя @{username}: {e}")
            await message.reply_text(
                f"❌ Не удалось найти пользователя @{username}\n\n"
                "Возможные причины:\n"
                "- Неверный username\n"
                "- Пользователь никогда не писал боту"
            )
            return

        # Проверяем, есть ли уже клиент в БД
        cursor = db_connection.cursor()
        cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (user.id,))
        existing = cursor.fetchone()

        if existing and existing[0]:
            await message.reply_text(
                f"ℹ️ Клиент **{user.first_name}** (@{user.username}) уже есть в системе.\n"
                f"Тред: **#{existing[0]}**"
            )
            return

        # Создаём тред для клиента
        await message.reply_text(f"⏳ Создаю тред для @{username}...")

        thread_id = await create_thread_for_client(client, user)

        if not thread_id:
            await message.reply_text("❌ Не удалось создать тред. Проверьте логи.")
            return

        # Сохраняем/обновляем клиента в БД
        if existing:
            # Клиент есть, но без треда - обновляем
            update_client_thread(db_connection, user.id, thread_id)
        else:
            # Новый клиент - создаём запись
            cursor.execute('''
                INSERT INTO clients (user_id, first_name, last_name, username, first_contact, last_contact, thread_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (user.id, user.first_name, user.last_name, user.username,
                  datetime.datetime.now(), datetime.datetime.now(), thread_id))
            db_connection.commit()

        client_name = f"{user.first_name}"
        if user.last_name:
            client_name += f" {user.last_name}"

        await message.reply_text(
            f"✅ **Тред создан!**\n\n"
            f"**Клиент:** {client_name} (@{user.username})\n"
            f"**ID клиента:** `{user.id}`\n"
            f"**Тред:** **#{thread_id}**\n\n"
            f"Чтобы написать клиенту:\n"
            f"`/{thread_id} Ваше сообщение`"
        )

        logger.info(f"Создан тред #{thread_id} для @{username} (ID: {user.id}) менеджером {manager_id}")

    except Exception as e:
        logger.error(f"Ошибка при создании клиента: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


# === Мониторинг и Heartbeat ===
bot_start_time = None

async def send_monitoring_message(msg_type: str):
    """Отправить сообщение мониторинга в группу"""
    if not MONITORING_ENABLED:
        return

    try:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if msg_type == "start":
            text = f"🟢 **Бот запущен**\n`{now}`"
        elif msg_type == "stop":
            text = f"🔴 **Бот остановлен**\n`{now}`"
        elif msg_type == "heartbeat":
            uptime = ""
            if bot_start_time:
                delta = datetime.datetime.now() - bot_start_time
                hours, remainder = divmod(int(delta.total_seconds()), 3600)
                minutes, _ = divmod(remainder, 60)
                uptime = f"\nUptime: {hours}ч {minutes}м"
            text = f"💚 **Heartbeat**\n`{now}`{uptime}"
        else:
            text = f"ℹ️ {msg_type}\n`{now}`"

        await business.send_message(
            chat_id=MONITORING_CHAT_ID,
            text=text
        )
        logger.info(f"Мониторинг: отправлено уведомление типа '{msg_type}'")
    except Exception as e:
        logger.error(f"Ошибка отправки мониторинга: {e}")


async def heartbeat_task():
    """Периодическая отправка heartbeat"""
    if not MONITORING_ENABLED or HEARTBEAT_INTERVAL <= 0:
        return

    logger.info(f"Запуск heartbeat мониторинга (интервал: {HEARTBEAT_INTERVAL} минут)")

    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL * 60)
        try:
            await send_monitoring_message("heartbeat")
        except Exception as e:
            logger.error(f"Ошибка heartbeat: {e}")


# функция планировщика проверок
async def schedule_checks():
    global bot_start_time

    # Начальная задержка для полной инициализации клиента
    await asyncio.sleep(5)

    # Записываем время запуска и отправляем уведомление
    bot_start_time = datetime.datetime.now()
    try:
        await send_monitoring_message("start")
    except Exception as e:
        logger.error(f"Ошибка отправки start-уведомления: {e}")

    # Запускаем heartbeat в отдельной задаче
    asyncio.create_task(heartbeat_task())

    # Дополнительная задержка перед первой проверкой
    await asyncio.sleep(25)

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

# Обработчик команды помощи (для входящих сообщений от других менеджеров)
# Outgoing сообщения обрабатываются роутером command_router
# Используем regex вместо command, т.к. command требует entity bot_command от Telegram
@business.on_message(filters.regex(r'^/help(\s|$)') & filters.chat(SUPPORT_GROUP_ID) & ~filters.outgoing)
async def handle_help_command(client, message):
    try:
        logger.info(f"Получена команда /help от пользователя {message.from_user.id}")
        
        help_text = """
📋 **Доступные команды**:

⚙️ **Ответы клиентам**:
- `/[thread_id] [текст]` - Ответить по номеру треда
- `/[ИмяКлиента] [текст]` - Ответить по ID клиента (русские буквы)
- `/id [thread_id] [Имя]` - Задать ID клиенту
- `/company [thread_id] [Компания]` - Задать название компании
- `/card [thread_id]` - Отправить визитку клиенту

⚙️ **Управление**:
- `/auth [эмодзи], [Имя], [Должность]` - Авторизоваться
- `/newclient @username` - Создать тред для нового клиента (только в General)
- `/onduty @username [ID_треда]` - Назначить ответственного
- `/ok [ID_треда]` - Сбросить уведомления для треда
- `/duties` - Список ответственных менеджеров
- `/threads` - Список активных тредов
- `/tier [ID_треда] [1|2|3]` - Установить тир клиента

👥 **Менеджеры**:
- `/vacation @менеджер @заместитель [tier-1 tier-2]` - Отпуск/больничный
- `/return @менеджер` - Вернуть из отпуска
- `/team` - Карточки всех менеджеров
- `/fire @новый` (reply на карточку) - Уволить менеджера

📊 **Информация**:
- `/myinfo` - Ваша информация в системе
- `/help` - Краткая справка

ℹ️ **Подсказки**:
- ID клиента: русские буквы и цифры (например: Иванов, Клиент123)
- При ответе через /{номер} или /ИмяКлиента менеджер становится ответственным
"""
        
        # Отправляем список команд
        await message.reply_text(help_text)
        logger.info("Отправлен список команд")
    except Exception as e:
        logger.error(f"Ошибка при отправке справки: {e}")
        await message.reply_text(f"Произошла ошибка: {e}")


@business.on_message(filters.command("readme") & filters.chat(SUPPORT_GROUP_ID))
async def handle_readme_command(client, message):
    """Отправить файл README с полным описанием бота"""
    try:
        logger.info(f"Получена команда /readme от пользователя {message.from_user.id}")

        readme_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.txt")

        if os.path.exists(readme_path):
            await message.reply_document(
                document=readme_path,
                caption="Руководство по использованию CFRD Support Bot v2"
            )
            logger.info("README.txt отправлен")
        else:
            await message.reply_text("Файл README.txt не найден")
            logger.error(f"README.txt не найден по пути: {readme_path}")

    except Exception as e:
        logger.error(f"Ошибка при отправке README: {e}")
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


# Обработчик редактирования сообщений клиента
@business.on_edited_message(filters.private)
async def handle_edited_message(client, message):
    """Когда клиент редактирует сообщение - уведомляем группу"""
    try:
        user = message.from_user
        if not user:
            return

        user_id = user.id
        client_msg_id = message.id
        new_text = message.text or message.caption or ""

        # Ищем маппинг для этого сообщения
        mapping = get_group_message_id(db_connection, client_msg_id, user_id)
        if not mapping:
            logger.debug(f"Маппинг для сообщения {client_msg_id} от {user_id} не найден")
            return

        group_msg_id, thread_id, old_text = mapping

        # Формируем имя клиента
        client_name = user.first_name or ""
        if user.last_name:
            client_name += f" {user.last_name}"
        username_str = f" @{user.username}" if user.username else ""

        # Формируем уведомление (только новый текст, т.к. это reply на оригинал)
        new_preview = new_text[:200] + "..." if new_text and len(new_text) > 200 else new_text
        if new_text:
            notification = f"✏️ **{client_name}{username_str}** изменил на:\n_{escape_markdown(new_preview)}_"
        else:
            notification = f"✏️ **{client_name}{username_str}** изменил медиа"

        # Отправляем уведомление в тред как reply на оригинальное сообщение
        await client.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=notification,
            reply_to_message_id=group_msg_id,
            parse_mode=pyrogram.enums.ParseMode.MARKDOWN
        )

        # Обновляем текст в маппинге
        update_message_text(db_connection, client_msg_id, user_id, new_text)

        logger.info(f"Клиент {user_id} отредактировал сообщение {client_msg_id}, уведомление в тред {thread_id}")

    except Exception as e:
        logger.error(f"Ошибка при обработке редактирования сообщения: {e}")


# Обработчик удаления сообщений клиента
@business.on_deleted_messages(filters.private)
async def handle_deleted_message(client, messages):
    """Когда клиент удаляет сообщение - уведомляем группу"""
    try:
        for message in messages:
            # message может быть просто ID или объект с ограниченной информацией
            if hasattr(message, 'id'):
                client_msg_id = message.id
                user_id = message.chat.id if hasattr(message, 'chat') and message.chat else None
            else:
                client_msg_id = message
                user_id = None

            if not user_id:
                # Пробуем найти user_id по message_id в маппинге
                cursor = db_connection.cursor()
                cursor.execute('''
                SELECT user_id, group_message_id, thread_id, message_text FROM message_mapping
                WHERE client_message_id = ?
                ORDER BY id DESC LIMIT 1
                ''', (client_msg_id,))
                result = cursor.fetchone()
                if result:
                    user_id, group_msg_id, thread_id, old_text = result
                else:
                    continue
            else:
                # Ищем маппинг для этого сообщения
                mapping = get_group_message_id(db_connection, client_msg_id, user_id)
                if not mapping:
                    continue
                group_msg_id, thread_id, old_text = mapping

            # Получаем информацию о клиенте
            client_info = get_client_info_by_thread(db_connection, thread_id)
            if client_info:
                first_name, last_name, username = client_info
                client_name = first_name or ""
                if last_name:
                    client_name += f" {last_name}"
                username_str = f" @{username}" if username else ""
            else:
                client_name = "Клиент"
                username_str = ""

            # Формируем уведомление
            notification = f"🗑 **{client_name}{username_str}** удалил сообщение"

            # Отправляем уведомление как reply на оригинальное сообщение
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=notification,
                reply_to_message_id=group_msg_id,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN
            )

            logger.info(f"Клиент {user_id} удалил сообщение {client_msg_id}, уведомление в тред {thread_id}")

    except Exception as e:
        logger.error(f"Ошибка при обработке удаления сообщения: {e}")




# Обработчик всех сообщений в личных чатах (кроме команд)
@business.on_message(filters.private & ~filters.command(["start", "check_forum", "list_topics", "create_test_topic", "help"]))
async def handle_private_messages(client, message):
    try:
        # Получаем информацию о сообщении
        user = message.from_user

        # Проверяем, является ли сообщение частью медиа-группы
        if message.media_group_id:
            # Получаем thread_id для клиента
            thread_id = save_client(db_connection, user)
            # Обрабатываем как медиа-группу
            await handle_client_media_group(client, message, thread_id)
            return

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
                
                # Отправляем ответ клиенту при первом обращении (только когда новый клиент)
                cursor = db_connection.cursor()
                cursor.execute('SELECT message_count FROM clients WHERE user_id = ?', (user.id,))
                result = cursor.fetchone()
                if result and result[0] == 1:  # Если это первое сообщение
                    await message.reply_text(
                        "Спасибо за ваше обращение! Наши менеджеры скоро с вами свяжутся."
                    )
                
                logger.info(f"Создан новый тред {new_thread_id} для клиента {user.id}")
            else:
                # Если не удалось создать тред, пересылаем сообщение в основную группу
                logger.warning("Не удалось создать тред, пересылаем сообщение без треда")
                await forward_message_to_support(client, message)
                
                # Отправляем ответ клиенту при первом обращении (только когда новый клиент)
                cursor = db_connection.cursor()
                cursor.execute('SELECT message_count FROM clients WHERE user_id = ?', (user.id,))
                result = cursor.fetchone()
                if result and result[0] == 1:  # Если это первое сообщение
                    await message.reply_text(
                        "Спасибо за ваше обращение! Наши менеджеры скоро с вами свяжутся."
                    )
                
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
@business.on_message(filters.command("setup_commands") & filters.chat(SUPPORT_GROUP_ID))
async def handle_setup_commands(client, message):
    try:
        await message.reply_text("Настраиваю подсказки команд...")
        await setup_bot_commands(client)
        await message.reply_text("✅ Подсказки команд настроены")
    except Exception as e:
        logger.error(f"Ошибка при настройке команд: {e}")
        await message.reply_text(f"❌ Ошибка при настройке команд: {e}")

# Обработчик реакций от клиентов через raw updates
async def handle_client_reactions(client, update, users, chats):
    """Обработчик raw updates для отслеживания реакций клиентов"""
    try:
        update_type = type(update).__name__

        # Логируем только реакции
        if "Reaction" in update_type:
            logger.info(f"=== CLIENT REACTION === {update_type}")

            # UpdateBotMessageReaction или UpdateMessageReactions
            if hasattr(update, 'reactions') or hasattr(update, 'new_reactions'):
                msg_id = getattr(update, 'msg_id', None)
                peer = getattr(update, 'peer', None)

                # Получаем реакции
                reactions = getattr(update, 'reactions', None) or getattr(update, 'new_reactions', [])

                if reactions:
                    # Форматируем реакции
                    emoji_list = []
                    for r in reactions:
                        if hasattr(r, 'reaction'):
                            reaction = r.reaction
                            if hasattr(reaction, 'emoticon'):
                                emoji_list.append(reaction.emoticon)
                        elif hasattr(r, 'emoticon'):
                            emoji_list.append(r.emoticon)

                    if emoji_list:
                        emoji_str = " ".join(emoji_list)
                        logger.info(f"Реакции на сообщение {msg_id}: {emoji_str}")

                        # Отправляем уведомление в группу
                        try:
                            # Определяем thread_id из peer если возможно
                            notification = f"👍 Клиент поставил реакцию: {emoji_str}"

                            # Попробуем найти thread_id по сообщению (если есть маппинг)
                            # Пока просто логируем
                            logger.info(notification)
                        except Exception as e:
                            logger.error(f"Ошибка отправки уведомления о реакции: {e}")

    except Exception as e:
        logger.debug(f"Ошибка обработки raw update: {e}")


# Обработчик реакций через on_message_reaction_updated
@business.on_message_reaction_updated()
async def on_client_reaction(client, update):
    """Обработчик реакций клиентов на сообщения"""
    try:
        logger.info(f"=== REACTION UPDATE === {update}")

        chat = update.chat
        user = getattr(update, 'user', None) or getattr(update, 'actor', None)
        old_reaction = update.old_reaction
        new_reaction = update.new_reaction

        # Получаем имя пользователя
        user_name = "Клиент"
        user_link = ""
        if user:
            user_name = user.first_name or ""
            if user.last_name:
                user_name += f" {user.last_name}"
            user_link = f" [↗](tg://user?id={user.id})"

        # Определяем добавленные реакции
        new_emojis = set()
        if new_reaction:
            for r in new_reaction:
                if hasattr(r, 'emoji') and r.emoji:
                    new_emojis.add(r.emoji)

        old_emojis = set()
        if old_reaction:
            for r in old_reaction:
                if hasattr(r, 'emoji') and r.emoji:
                    old_emojis.add(r.emoji)

        added = new_emojis - old_emojis

        if added:
            emoji_str = " ".join(added)
            notification = f"👍 **{user_name}{user_link}** поставил реакцию: {emoji_str}"
            logger.info(notification)

            # Если это личный чат (business), отправляем уведомление в группу
            if chat and chat.type in [pyrogram.enums.ChatType.PRIVATE]:
                # Ищем thread_id клиента
                client_data = get_client_by_thread(db_connection, None)  # TODO: найти по user_id
                cursor = db_connection.cursor()
                cursor.execute('SELECT thread_id FROM clients WHERE user_id = ?', (user.id if user else 0,))
                result = cursor.fetchone()

                if result and result[0]:
                    thread_id = result[0]
                    try:
                        await client.send_message(
                            chat_id=SUPPORT_GROUP_ID,
                            text=notification,
                            reply_to_message_id=thread_id,
                            parse_mode=pyrogram.enums.ParseMode.MARKDOWN
                        )
                        logger.info(f"Уведомление о реакции отправлено в тред {thread_id}")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления о реакции: {e}")

    except Exception as e:
        logger.error(f"Ошибка обработки реакции клиента: {e}")


# === Graceful Shutdown ===

async def graceful_shutdown(sig_name):
    """Корректное завершение работы бота"""
    global shutdown_requested
    shutdown_requested = True

    logger.info(f"Получен сигнал {sig_name}, начинаем graceful shutdown...")

    # Ждём завершения обработки текущих медиа-групп
    max_wait = 5  # максимум 5 секунд
    waited = 0
    while (client_media_groups or manager_media_groups) and waited < max_wait:
        pending_client = len(client_media_groups)
        pending_manager = len(manager_media_groups)
        logger.info(f"Ожидание завершения медиа-групп: клиенты={pending_client}, менеджеры={pending_manager}")
        await asyncio.sleep(0.5)
        waited += 0.5

    if client_media_groups or manager_media_groups:
        logger.warning(f"Таймаут ожидания медиа-групп, незавершённых: клиенты={len(client_media_groups)}, менеджеры={len(manager_media_groups)}")

    # Сохраняем состояние thread_title_states в БД (опционально для восстановления)
    try:
        if thread_title_states:
            logger.info(f"Сохранение состояния {len(thread_title_states)} тредов...")
            # Состояние можно восстановить из БД при следующем запуске
    except Exception as e:
        logger.error(f"Ошибка сохранения состояния: {e}")

    logger.info("Graceful shutdown завершён, останавливаем бота...")
    await business.stop()


def signal_handler(sig, frame):
    """Обработчик сигналов для graceful shutdown"""
    sig_name = signal.Signals(sig).name
    logger.info(f"Получен сигнал {sig_name}")

    # Запускаем graceful shutdown в event loop
    if business.is_connected:
        asyncio.get_event_loop().create_task(graceful_shutdown(sig_name))
    else:
        logger.info("Бот не подключен, выход...")
        exit(0)


# Запускаем клиент
if __name__ == "__main__":
    try:
        logger.info("Запуск бизнес-аккаунта Telegram...")
        logger.info(f"База данных клиентов настроена. Группа поддержки: {SUPPORT_GROUP_ID}")

        # Регистрируем обработчики сигналов для graceful shutdown
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        logger.info("Обработчики SIGTERM/SIGINT зарегистрированы для graceful shutdown")

        # Добавляем raw handler для реакций
        business.add_handler(RawUpdateHandler(handle_client_reactions), group=-1)

        # Запускаем периодическую проверку неотвеченных сообщений
        business.loop.create_task(schedule_checks())

        business.run()
    except KeyboardInterrupt:
        logger.info("Получен KeyboardInterrupt, завершение...")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {e}")
