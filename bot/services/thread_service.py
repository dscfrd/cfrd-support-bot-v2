"""Thread management service"""

import random
import logging
import pyrogram
from config import SUPPORT_GROUP_ID

logger = logging.getLogger(__name__)


async def check_forum_capabilities(client):
    """Check if the support group is configured as a forum"""
    try:
        chat = await client.get_chat(SUPPORT_GROUP_ID)

        if hasattr(chat, 'is_forum') and chat.is_forum:
            logger.info("Группа настроена как форум")
            return True
        else:
            logger.warning("Группа НЕ настроена как форум")
            return False
    except Exception as e:
        logger.error(f"Ошибка при проверке группы: {e}")
        return False


async def create_support_thread(client, thread_title):
    """Create a new support thread (forum topic)"""
    try:
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

        if result and hasattr(result, "updates"):
            for update in result.updates:
                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                    thread_id = update.message.id
                    logger.info(f"Создан тред с ID: {thread_id}, название: '{thread_title}'")
                    return thread_id

        logger.warning("Не удалось извлечь thread_id из ответа API")
        return None

    except Exception as e:
        logger.error(f"Ошибка при создании треда: {e}")
        return None


async def create_thread_for_client(client, user):
    """Create a thread for a client"""
    # Формируем заголовок треда
    thread_title_base = f"{user.first_name}"
    if user.last_name:
        thread_title_base += f" {user.last_name}"
    if user.username:
        thread_title_base += f" (@{user.username})"

    # Ограничиваем длину заголовка (максимум для Telegram - 128 символов)
    if len(thread_title_base) > 120:
        thread_title_base = thread_title_base[:120] + "..."

    # Создаем тред
    thread_id = await create_support_thread(client, thread_title_base)

    return thread_id


async def edit_thread_title(client, thread_id, new_title):
    """Edit thread title"""
    try:
        logger.info(f"Попытка изменить заголовок треда {thread_id} на '{new_title}'")

        peer = await client.resolve_peer(SUPPORT_GROUP_ID)

        await client.invoke(
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


async def mark_thread_urgent(client, thread_id, is_urgent=True):
    """Mark thread as urgent or normal"""
    try:
        peer = await client.resolve_peer(SUPPORT_GROUP_ID)

        # Используем красную иконку для срочных сообщений, синюю для обычных
        icon_emoji_id = 5310132492618974465 if is_urgent else 5312536423851630001

        await client.invoke(
            pyrogram.raw.functions.channels.EditForumTopic(
                channel=peer,
                topic_id=thread_id,
                icon_emoji_id=icon_emoji_id
            )
        )

        logger.info(f"Тред {thread_id} отмечен как {'срочный' if is_urgent else 'обычный'}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при изменении иконки треда: {e}")
        return False


async def update_thread_title(client, thread_id, client_name, custom_id=None):
    """Update thread title with client info"""
    if custom_id:
        new_title = f"{custom_id} | {client_name} | тред {thread_id}"
    else:
        new_title = f"{thread_id}: {client_name}"

    await edit_thread_title(client, thread_id, new_title)
