"""Handlers for client private messages"""
import logging
from pyrogram import filters

from bot.database import (
    get_connection,
    save_client,
    save_message,
    update_client_thread,
    update_client_message_time,
)
from bot.services import create_thread_for_client, edit_thread_title
from config import SUPPORT_GROUP_ID

logger = logging.getLogger(__name__)




async def forward_message_to_support(client, message, thread_id=None):
    """Forward message to support group"""
    try:
        user = message.from_user

        # Формируем информацию о клиенте
        client_name = f"{user.first_name or ''}"
        if user.last_name:
            client_name += f" {user.last_name}"
        if user.username:
            client_name += f" (@{user.username})"

        # Определяем тип сообщения
        message_type = "текстовое"
        if message.photo:
            message_type = "фото"
        elif message.video:
            message_type = "видео"
        elif message.document:
            message_type = "документ"
        elif message.voice:
            message_type = "голосовое"
        elif message.audio:
            message_type = "аудио"

        # Формируем заголовок сообщения
        header_text = f"**Сообщение от {client_name}** ({message_type})"

        if message.text:
            header_text += f":\n\n{message.text}"

        # Если есть thread_id, отправляем в него
        if thread_id:
            try:
                # Отправляем заголовок
                if message.text:
                    await client.send_message(
                        chat_id=SUPPORT_GROUP_ID,
                        text=header_text,
                        reply_to_message_id=thread_id
                    )
                else:
                    # Для медиафайлов сначала отправляем заголовок
                    await client.send_message(
                        chat_id=SUPPORT_GROUP_ID,
                        text=header_text,
                        reply_to_message_id=thread_id
                    )
                    # Затем копируем медиафайл
                    await client.copy_message(
                        chat_id=SUPPORT_GROUP_ID,
                        from_chat_id=message.chat.id,
                        message_id=message.id,
                        reply_to_message_id=thread_id
                    )

                logger.info(f"Сообщение переслано в тред {thread_id}")
                return True

            except Exception as e:
                if "TOPIC_DELETED" in str(e) or "TOPIC_CLOSED" in str(e):
                    logger.warning(f"Тред {thread_id} удален или закрыт")
                    return "TOPIC_DELETED"
                raise
        else:
            # Если thread_id нет, отправляем в основную группу
            await client.send_message(
                chat_id=SUPPORT_GROUP_ID,
                text=f"⚠️ {header_text}\n\n(Тред не создан)"
            )
            return False

    except Exception as e:
        logger.error(f"Ошибка при пересылке сообщения в поддержку: {e}")
        return False


def setup_client_handlers(app):
    """Setup handlers for client messages"""
    
    logger.info("=== РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ CLIENT_MESSAGES ===")

    @app.on_message(filters.private & ~filters.command(["start"]))
    async def handle_private_messages(client, message):
        """Handle private messages from clients"""
        try:
            user = message.from_user
            db_connection = get_connection()

            # Определяем тип сообщения
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
            else:
                message_text = "[НЕИЗВЕСТНЫЙ ТИП]"

            logger.info(f"Получено сообщение от клиента: {user.id}, {user.first_name}")

            # Проверяем, новый ли это клиент
            cursor = db_connection.cursor()
            cursor.execute('SELECT message_count FROM clients WHERE user_id = ?', (user.id,))
            result = cursor.fetchone()
            is_new_client = not result or result[0] == 0

            # Сохраняем клиента и сообщение
            thread_id = save_client(db_connection, user)
            save_message(db_connection, user.id, message_text, is_from_user=True, media_type=media_type)

            # Обрабатываем сообщение
            if thread_id:
                # Обновляем время последнего сообщения
                update_client_message_time(db_connection, thread_id)

                # Пересылаем в существующий тред
                result = await forward_message_to_support(client, message, thread_id)

                if result == "TOPIC_DELETED":
                    # Создаем новый тред
                    new_thread_id = await create_thread_for_client(client, user)
                    if new_thread_id:
                        update_client_thread(db_connection, user.id, new_thread_id)
                        await forward_message_to_support(client, message, new_thread_id)

                        # Обновляем заголовок
                        client_name = f"{user.first_name}"
                        if user.last_name:
                            client_name += f" {user.last_name}"
                        if user.username:
                            client_name += f" (@{user.username})"

                        await edit_thread_title(client, new_thread_id, f"{new_thread_id}: {client_name}")
            else:
                # Создаем новый тред
                new_thread_id = await create_thread_for_client(client, user)
                if new_thread_id:
                    update_client_thread(db_connection, user.id, new_thread_id)
                    await forward_message_to_support(client, message, new_thread_id)

                    # Отправляем приветствие новым клиентам
                    if is_new_client:
                        greeting = (
                            "Здравствуйте! Спасибо за ваше обращение.\n\n"
                            "Ваше сообщение получено и передано команде поддержки. "
                            "Скоро с вами свяжется персональный менеджер.\n\n"
                            "Рабочее время: пн-пт с 10:00 до 19:00 МСК."
                        )
                        await message.reply_text(greeting)

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")
            await message.reply_text(
                "Извините, произошла ошибка. Пожалуйста, попробуйте позже."
            )

    @app.on_message(filters.command("start") & filters.private)
    async def handle_start(client, message):
        """Handle /start command"""
        await message.reply_text(
            "Здравствуйте! Это бот поддержки CFRD.\n\n"
            "Напишите ваш вопрос, и мы вам обязательно поможем!"
        )
