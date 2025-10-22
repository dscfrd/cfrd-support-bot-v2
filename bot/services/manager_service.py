"""Manager operations service"""

import logging
from pyrogram import types
from bot.database.queries import get_manager, save_message
from bot.utils import format_signature_with_custom_emoji, format_card_with_custom_emoji

logger = logging.getLogger(__name__)


def get_utf16_length(text: str) -> int:
    """
    Получить длину текста в UTF-16 code units (как требует Telegram API)

    Args:
        text: Текст для измерения

    Returns:
        Длина в UTF-16 code units
    """
    return len(text.encode('utf-16-le')) // 2


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

        # Корректируем offset'ы entities для полного сообщения
        adjusted_entities = None
        if signature_entities:
            # Длина текста до подписи (reply_text + "\n\n") в UTF-16
            offset_adjustment = get_utf16_length(reply_text + "\n\n")
            adjusted_entities = []
            for entity in signature_entities:
                # Создаем новый entity с скорректированным offset
                adjusted_entity = types.MessageEntity(
                    type=entity.type,
                    offset=entity.offset + offset_adjustment,
                    length=entity.length,
                    custom_emoji_id=entity.custom_emoji_id
                )
                adjusted_entities.append(adjusted_entity)
            logger.info(f"📝 Скорректировано {len(adjusted_entities)} entities с offset +{offset_adjustment} (UTF-16)")
            logger.info(f"📝 Full message: {full_message}")
            logger.info(f"📝 Entities: {adjusted_entities}")

        # Отправляем сообщение клиенту с entities для кастомных эмодзи
        await client.send_message(
            chat_id=client_id,
            text=full_message,
            entities=adjusted_entities
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

        # Корректируем offset'ы entities для полного caption
        adjusted_entities = None
        if signature_entities:
            if caption:
                # Длина текста до подписи (caption + "\n\n") в UTF-16
                offset_adjustment = get_utf16_length(caption + "\n\n")
                adjusted_entities = []
                for entity in signature_entities:
                    # Создаем новый entity с скорректированным offset
                    adjusted_entity = types.MessageEntity(
                        type=entity.type,
                        offset=entity.offset + offset_adjustment,
                        length=entity.length,
                        custom_emoji_id=entity.custom_emoji_id
                    )
                    adjusted_entities.append(adjusted_entity)
                logger.info(f"📝 Скорректировано {len(adjusted_entities)} entities для caption с offset +{offset_adjustment} (UTF-16)")
                logger.info(f"📝 Full caption: {full_caption}")
                logger.info(f"📝 Entities: {adjusted_entities}")
            else:
                # Если caption нет, используем entities как есть
                adjusted_entities = signature_entities
                logger.info(f"📝 Используем {len(adjusted_entities)} entities без коррекции (нет caption)")
                logger.info(f"📝 Entities: {adjusted_entities}")

        # Отправляем медиафайл в зависимости от типа
        if media_type == "photo":
            await client.send_photo(
                chat_id=client_id,
                photo=file_id,
                caption=full_caption,
                caption_entities=adjusted_entities
            )
        elif media_type == "document":
            await client.send_document(
                chat_id=client_id,
                document=file_id,
                caption=full_caption,
                caption_entities=adjusted_entities
            )
        elif media_type == "video":
            await client.send_video(
                chat_id=client_id,
                video=file_id,
                caption=full_caption,
                caption_entities=adjusted_entities
            )
        elif media_type == "audio":
            await client.send_audio(
                chat_id=client_id,
                audio=file_id,
                caption=full_caption,
                caption_entities=adjusted_entities
            )
        elif media_type == "voice":
            await client.send_voice(
                chat_id=client_id,
                voice=file_id,
                caption=full_caption,
                caption_entities=adjusted_entities
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
