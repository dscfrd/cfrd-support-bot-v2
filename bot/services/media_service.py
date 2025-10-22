"""Media group processing service"""

import asyncio
import datetime
import logging

logger = logging.getLogger(__name__)

# Global state for media groups
manager_media_groups = {}
pending_media_groups = {}


async def process_manager_media_group_after_delay(client, media_group_id, delay=3):
    """Process media group after delay"""
    try:
        await asyncio.sleep(delay)

        if media_group_id not in manager_media_groups:
            logger.warning(f"Media group {media_group_id} not found")
            return

        group_data = manager_media_groups[media_group_id]
        client_id = group_data["client_id"]
        caption = group_data.get("caption")
        files = group_data["files"]

        logger.info(f"Отправка медиа-группы {media_group_id} клиенту {client_id}, файлов: {len(files)}")

        # Отправляем каждый файл клиенту
        for i, file_msg in enumerate(files):
            try:
                file_caption = caption if i == 0 else None  # Подпись только к первому файлу

                await client.copy_message(
                    chat_id=client_id,
                    from_chat_id=file_msg.chat.id,
                    message_id=file_msg.id,
                    caption=file_caption
                )
            except Exception as e:
                logger.error(f"Ошибка при отправке файла из группы: {e}")

        # Удаляем обработанную группу
        del manager_media_groups[media_group_id]

    except Exception as e:
        logger.error(f"Ошибка при обработке медиа-группы: {e}")


async def cleanup_media_groups():
    """Clean up old media groups"""
    while True:
        try:
            await asyncio.sleep(300)  # Каждые 5 минут

            current_time = datetime.datetime.now()
            expired_groups = []

            for media_group_id, group_data in manager_media_groups.items():
                timestamp = group_data.get("timestamp")
                if timestamp and (current_time - timestamp).total_seconds() > 600:
                    expired_groups.append(media_group_id)

            for media_group_id in expired_groups:
                del manager_media_groups[media_group_id]
                logger.info(f"Удалена устаревшая медиа-группа {media_group_id}")

        except Exception as e:
            logger.error(f"Ошибка при очистке медиа-групп: {e}")


async def handle_client_media_group(client, message):
    """Handle media group from client"""
    # TODO: Implement media group handling from clients
    pass
