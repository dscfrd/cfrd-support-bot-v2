#!/usr/bin/env python3
"""
CFRD Reactions Bot - бот для отслеживания реакций и будущих WebRTC звонков
"""

import asyncio
import logging
import signal
import sys
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.handlers import RawUpdateHandler
from pyrogram.raw.types import UpdateBotMessageReaction, UpdateBotMessageReactions

# Конфигурация
BOT_TOKEN = "8495087622:AAEE4iYvrqO-Om6S1ohEkn_uBgJLeTkIR3c"  # Business bot
API_ID = 27337424
API_HASH = "4f5d8461e55fc3578c7659195a107def"
SUPPORT_GROUP_ID = -1003317645437

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создаем клиент бота
bot = Client(
    "reactions_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Подключение к БД основного бота
import sqlite3
DATABASE_NAME = "clients_test.db"

def get_db_connection():
    return sqlite3.connect(DATABASE_NAME)


def get_client_by_thread(conn, thread_id):
    """Получить клиента по thread_id"""
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, first_name, last_name, username FROM clients WHERE thread_id = ?', (thread_id,))
    return cursor.fetchone()


def get_group_message_info(conn, group_message_id, thread_id):
    """Получить информацию о сообщении по ID в группе"""
    cursor = conn.cursor()
    cursor.execute('''
    SELECT client_message_id, user_id, message_text FROM message_mapping
    WHERE group_message_id = ? AND thread_id = ?
    ORDER BY id DESC LIMIT 1
    ''', (group_message_id, thread_id))
    return cursor.fetchone()


async def handle_raw_update(client, update, users, chats):
    """Обработчик raw updates для реакций"""
    try:
        update_type = type(update).__name__

        # Логируем ВСЕ обновления для отладки
        logger.info(f"=== RAW UPDATE === {update_type}")

        # Для Business updates логируем полностью
        if "Business" in update_type:
            logger.info(f"Business update: {update}")

        # Логируем все типы обновлений для отладки
        if "Reaction" in update_type:
            logger.info(f"=== RAW REACTION === {update_type}")
            logger.info(f"Update: {update}")

            # UpdateBotMessageReaction - реакция на сообщение бота
            if isinstance(update, UpdateBotMessageReaction):
                peer = update.peer
                msg_id = update.msg_id
                actor = update.actor
                old_reactions = update.old_reactions
                new_reactions = update.new_reactions

                logger.info(f"Реакция на сообщение {msg_id}")
                logger.info(f"Peer: {peer}, Actor: {actor}")
                logger.info(f"Old: {old_reactions}, New: {new_reactions}")

                # Определяем добавленные/удаленные реакции
                old_emojis = set()
                new_emojis = set()

                for r in old_reactions:
                    if hasattr(r, 'emoticon'):
                        old_emojis.add(r.emoticon)
                    elif hasattr(r, 'document_id'):
                        old_emojis.add('✨')  # Кастомный эмодзи

                for r in new_reactions:
                    if hasattr(r, 'emoticon'):
                        new_emojis.add(r.emoticon)
                    elif hasattr(r, 'document_id'):
                        new_emojis.add('✨')

                added = new_emojis - old_emojis
                removed = old_emojis - new_emojis

                # Получаем информацию о пользователе
                user_name = "Пользователь"
                user_id = None

                if hasattr(actor, 'user_id'):
                    user_id = actor.user_id
                    if user_id in users:
                        user = users[user_id]
                        user_name = user.first_name or ""
                        if hasattr(user, 'last_name') and user.last_name:
                            user_name += f" {user.last_name}"

                # Формируем уведомление
                if added:
                    emoji_str = " ".join(added)
                    notification = f"👍 **{user_name}** поставил реакцию: {emoji_str}"
                    logger.info(notification)
                elif removed:
                    notification = f"➖ **{user_name}** убрал реакцию"
                    logger.info(notification)

    except Exception as e:
        logger.error(f"Ошибка обработки raw update: {e}")


@bot.on_message(filters.command("start"))
async def start_command(client, message):
    await message.reply_text(
        "🤖 **CFRD Reactions Bot**\n\n"
        "Этот бот отслеживает реакции на сообщения.\n"
        "В будущем: WebRTC звонки."
    )


@bot.on_message(filters.command("status"))
async def status_command(client, message):
    await message.reply_text("✅ Бот работает")


# Обработчик реакций через message_reaction_updated
@bot.on_message_reaction_updated()
async def on_reaction_updated(client, update):
    """Обработчик обновления реакций"""
    try:
        logger.info(f"=== REACTION UPDATE === {update}")

        chat_id = update.chat.id if update.chat else None
        # Pyrofork использует id вместо message_id
        message_id = getattr(update, 'id', None) or getattr(update, 'message_id', None)
        user = getattr(update, 'user', None) or getattr(update, 'actor', None)
        old_reaction = update.old_reaction
        new_reaction = update.new_reaction

        logger.info(f"Chat: {chat_id}, Message: {message_id}")
        logger.info(f"User: {user}")
        logger.info(f"Old: {old_reaction}, New: {new_reaction}")

        # Получаем имя пользователя
        user_name = "Пользователь"
        user_link = ""
        if user:
            user_name = user.first_name or ""
            if user.last_name:
                user_name += f" {user.last_name}"
            user_link = f" [↗](tg://user?id={user.id})"

        # Определяем добавленные/удаленные реакции
        old_emojis = set()
        new_emojis = set()

        if old_reaction:
            for r in old_reaction:
                if hasattr(r, 'emoji') and r.emoji:
                    old_emojis.add(r.emoji)
                elif hasattr(r, 'custom_emoji_id'):
                    old_emojis.add('✨')

        if new_reaction:
            for r in new_reaction:
                if hasattr(r, 'emoji') and r.emoji:
                    new_emojis.add(r.emoji)
                elif hasattr(r, 'custom_emoji_id'):
                    new_emojis.add('✨')

        added = new_emojis - old_emojis
        removed = old_emojis - new_emojis

        # Формируем уведомление
        notification = None
        if added:
            emoji_str = " ".join(added)
            notification = f"👍 **{user_name}{user_link}** поставил реакцию: {emoji_str}"
        elif removed:
            notification = f"➖ **{user_name}{user_link}** убрал реакцию"

        if notification and chat_id:
            # Отправляем уведомление в тот же чат как reply
            try:
                await client.send_message(
                    chat_id=chat_id,
                    text=notification,
                    reply_to_message_id=message_id,
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info(f"Отправлено уведомление о реакции: {notification}")
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления: {e}")

    except Exception as e:
        logger.error(f"Ошибка обработки реакции: {e}")


def signal_handler(sig, frame):
    logger.info("Получен сигнал завершения, останавливаю бота...")
    sys.exit(0)


async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Запуск Reactions Bot...")

    # Добавляем raw handler для отладки
    bot.add_handler(RawUpdateHandler(handle_raw_update), group=-1)

    await bot.start()
    logger.info("Reactions Bot запущен!")

    me = await bot.get_me()
    logger.info(f"Бот: @{me.username} (ID: {me.id})")

    # Держим бота запущенным
    await asyncio.Event().wait()


if __name__ == "__main__":
    bot.run(main())
