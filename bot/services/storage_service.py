"""File storage service"""

import logging
import datetime
from config import STORAGE_CHANNEL_ID

logger = logging.getLogger(__name__)


async def upload_file_to_storage(client, message, file_name):
    """Upload file to storage channel"""
    try:
        from bot.database import get_connection
        db_connection = get_connection()
        cursor = db_connection.cursor()

        # Проверяем, существует ли файл с таким именем
        cursor.execute('SELECT file_name FROM storage_files WHERE file_name = ?', (file_name,))
        if cursor.fetchone():
            await message.reply_text(f"❌ Файл с именем '{file_name}' уже существует. Используйте /replace для замены.")
            return False, None

        # Загружаем файл в канал хранилища
        sent_message = await client.copy_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=message.chat.id,
            message_id=message.id,
            caption=f"📁 {file_name}"
        )

        # Сохраняем информацию о файле в БД
        file_id = None
        file_type = None

        if message.document:
            file_id = message.document.file_id
            file_type = "document"
        elif message.photo:
            file_id = message.photo.file_id
            file_type = "photo"
        elif message.video:
            file_id = message.video.file_id
            file_type = "video"

        cursor.execute('''
        INSERT INTO storage_files (file_name, file_id, message_id, file_type, upload_date, uploaded_by)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_name, file_id, sent_message.id, file_type, datetime.datetime.now(), message.from_user.id))

        db_connection.commit()

        await message.reply_text(f"✅ Файл '{file_name}' успешно загружен в хранилище!")
        logger.info(f"Файл '{file_name}' загружен в хранилище")

        return True, file_name

    except Exception as e:
        logger.error(f"Ошибка при загрузке файла в хранилище: {e}")
        await message.reply_text(f"❌ Ошибка при загрузке файла: {e}")
        return False, None


async def get_files_list(client, message):
    """Get list of files in storage"""
    try:
        from bot.database import get_connection
        db_connection = get_connection()
        cursor = db_connection.cursor()

        cursor.execute('SELECT file_name, file_type, upload_date FROM storage_files ORDER BY upload_date DESC')
        files = cursor.fetchall()

        if not files:
            await message.reply_text("📁 Хранилище пусто")
            return

        response = "📁 **Файлы в хранилище**:\n\n"
        for file_name, file_type, upload_date in files:
            response += f"• `{file_name}` ({file_type})\n"

        await message.reply_text(response)

    except Exception as e:
        logger.error(f"Ошибка при получении списка файлов: {e}")
        await message.reply_text(f"❌ Ошибка: {e}")


async def send_file_to_client(client, identifier, file_name, message, is_thread_id=True):
    """Send file from storage to client"""
    try:
        from bot.database import get_connection, get_client_by_thread
        db_connection = get_connection()
        cursor = db_connection.cursor()

        # Получаем файл из хранилища
        cursor.execute('SELECT message_id FROM storage_files WHERE file_name = ?', (file_name,))
        result = cursor.fetchone()

        if not result:
            await message.reply_text(f"❌ Файл '{file_name}' не найден в хранилище")
            return

        storage_message_id = result[0]

        # Определяем client_id
        if is_thread_id:
            client_data = get_client_by_thread(db_connection, identifier)
            if not client_data:
                await message.reply_text(f"❌ Клиент для треда {identifier} не найден")
                return
            client_id = client_data[0]
        else:
            # identifier это custom_id
            cursor.execute('SELECT user_id FROM clients WHERE custom_id = ?', (identifier,))
            result = cursor.fetchone()
            if not result:
                await message.reply_text(f"❌ Клиент с ID '{identifier}' не найден")
                return
            client_id = result[0]

        # Копируем файл клиенту
        await client.copy_message(
            chat_id=client_id,
            from_chat_id=STORAGE_CHANNEL_ID,
            message_id=storage_message_id
        )

        await message.reply_text(f"✅ Файл '{file_name}' отправлен клиенту")
        logger.info(f"Файл '{file_name}' отправлен клиенту {client_id}")

    except Exception as e:
        logger.error(f"Ошибка при отправке файла клиенту: {e}")
        await message.reply_text(f"❌ Ошибка: {e}")


async def replace_file_in_storage(client, message, file_name):
    """Replace existing file in storage"""
    # TODO: Implement file replacement
    await message.reply_text("⚠️ Функция замены файлов в разработке")
    return False, None
