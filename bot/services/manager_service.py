"""Manager operations service"""

import logging
from bot.database.queries import get_manager, save_message
from bot.utils import format_signature_with_custom_emoji, format_card_with_custom_emoji

logger = logging.getLogger(__name__)


async def send_manager_reply_to_client(client, manager_id, client_id, reply_text):
    """Send manager's reply to client"""
    try:
        from bot.database import get_connection
        db_connection = get_connection()

        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)

        if not manager:
            logger.error(f"Менеджер {manager_id} не найден в базе данных")
            return False

        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager

        # Формируем подпись с кастомными эмодзи
        signature_text, signature_entities = format_signature_with_custom_emoji(
            emoji, name, position, extension
        )

        # Формируем полное сообщение с подписью менеджера
        full_message = f"{reply_text}\n\n{signature_text}"

        # Отправляем сообщение клиенту с entities для кастомных эмодзи
        await client.send_message(
            chat_id=client_id,
            text=full_message,
            entities=signature_entities
        )

        # Сохраняем сообщение в базу данных
        save_message(db_connection, client_id, reply_text, is_from_user=False)

        logger.info(f"Ответ менеджера {manager_id} отправлен клиенту {client_id}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при отправке ответа клиенту: {e}")
        return False


async def send_manager_media_to_client(client, manager_id, client_id, file_id, caption, media_type):
    """Send media file from manager to client"""
    try:
        from bot.database import get_connection
        db_connection = get_connection()

        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)

        if not manager:
            logger.error(f"Менеджер {manager_id} не найден в базе данных")
            return False

        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager

        # Формируем подпись с кастомными эмодзи
        signature_text, signature_entities = format_signature_with_custom_emoji(
            emoji, name, position, extension
        )

        # Добавляем текст к подписи, если он есть
        if caption:
            full_caption = f"{caption}\n\n{signature_text}"
        else:
            full_caption = signature_text

        # Отправляем медиафайл в зависимости от типа
        if media_type == "photo":
            await client.send_photo(
                chat_id=client_id,
                photo=file_id,
                caption=full_caption,
                caption_entities=signature_entities
            )
        elif media_type == "document":
            await client.send_document(
                chat_id=client_id,
                document=file_id,
                caption=full_caption,
                caption_entities=signature_entities
            )
        elif media_type == "video":
            await client.send_video(
                chat_id=client_id,
                video=file_id,
                caption=full_caption,
                caption_entities=signature_entities
            )
        elif media_type == "audio":
            await client.send_audio(
                chat_id=client_id,
                audio=file_id,
                caption=full_caption,
                caption_entities=signature_entities
            )
        elif media_type == "voice":
            await client.send_voice(
                chat_id=client_id,
                voice=file_id,
                caption=full_caption,
                caption_entities=signature_entities
            )
        else:
            logger.error(f"Неизвестный тип медиа: {media_type}")
            return False

        # Сохраняем информацию в базу данных
        save_message(db_connection, client_id, caption or "", is_from_user=False, media_type=media_type.upper())

        logger.info(f"Медиафайл ({media_type}) от менеджера {manager_id} отправлен клиенту {client_id}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при отправке медиафайла клиенту: {e}")
        return False


async def send_manager_card_to_client(client, manager_id, client_id):
    """Send manager's contact card to client"""
    try:
        from bot.database import get_connection
        db_connection = get_connection()

        # Получаем информацию о менеджере
        manager = get_manager(db_connection, manager_id)

        if not manager:
            logger.error(f"Менеджер {manager_id} не найден в базе данных")
            return False

        # Распаковываем данные менеджера
        _, emoji, name, position, extension, photo_file_id, auth_date, username = manager

        # Формируем текст карточки с кастомными эмодзи
        card_text, card_entities = format_card_with_custom_emoji(
            emoji, name, position, extension, username
        )

        # Отправляем карточку с фото, если оно есть
        if photo_file_id:
            await client.send_photo(
                chat_id=client_id,
                photo=photo_file_id,
                caption=card_text,
                caption_entities=card_entities
            )
        else:
            # Если фото нет, отправляем просто текст
            await client.send_message(
                chat_id=client_id,
                text=card_text,
                entities=card_entities
            )

        logger.info(f"Карточка менеджера {manager_id} отправлена клиенту {client_id}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при отправке карточки менеджера: {e}")
        return False
