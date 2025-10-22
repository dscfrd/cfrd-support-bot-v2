"""Handlers for manager commands"""

import datetime
import logging
import re
from pyrogram import filters

from bot.database import (
    get_connection,
    get_manager,
    save_manager,
    update_manager_photo,
    get_client_by_thread,
    get_all_active_threads,
    assign_duty_manager,
    get_duty_manager,
    is_first_reply,
    save_first_reply,
    update_manager_reply_time,
    unpack_manager_data,
)
from bot.services import (
    send_manager_reply_to_client,
    send_manager_card_to_client,
    mark_thread_urgent,
    edit_thread_title,
)
from bot.utils import generate_client_id
from config import SUPPORT_GROUP_ID

logger = logging.getLogger(__name__)

# Состояние авторизации менеджеров
manager_auth_state = {}


def setup_manager_handlers(app):
    """Setup handlers for manager commands"""

    @app.on_message(filters.command("auth") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_auth(client, message):
        """Handle manager authorization"""
        try:
            manager_id = message.from_user.id
            manager_username = message.from_user.username

            logger.info(f"Команда /auth от {manager_id} (username: {manager_username})")

            command_text = message.text.strip()

            if " " not in command_text:
                await message.reply_text(
                    "Неверный формат. Используйте:\n"
                    "/auth [эмодзи], [Имя], [Должность], [4 цифры]\n"
                    "Например: /auth 🔧, Иван Петров, Технический специалист, 1234"
                )
                return

            auth_data = command_text.split(" ", 1)[1]
            parts = [part.strip() for part in auth_data.split(",")]

            if len(parts) < 4:
                await message.reply_text(
                    "Неверный формат. Требуется: [эмодзи], [Имя], [Должность], [4 цифры]"
                )
                return

            emoji = parts[0]
            name = parts[1]
            position = ", ".join(parts[2:-1])
            extension = parts[-1].strip()

            if not re.match(r'^\d{4}$', extension):
                await message.reply_text("Добавочный номер должен состоять из 4 цифр")
                return

            # Сохраняем менеджера
            db_connection = get_connection()
            save_manager(db_connection, manager_id, emoji, name, position, extension, username=manager_username)

            # Запрашиваем фото
            await message.reply_text(
                "Спасибо! Теперь отправьте фотографию для вашего профиля.\n"
                "Фото будет показано клиентам при ответе."
            )

            manager_auth_state[manager_id] = "waiting_photo"

            logger.info(f"Менеджер {manager_id} ({name}) зарегистрирован")

        except Exception as e:
            logger.error(f"Ошибка при авторизации: {e}")
            await message.reply_text(f"Произошла ошибка: {e}")

    @app.on_message(filters.photo & filters.chat(SUPPORT_GROUP_ID), group=-1)
    async def handle_manager_photo(client, message):
        """Handle manager photo upload"""
        try:
            manager_id = message.from_user.id

            if manager_id in manager_auth_state and manager_auth_state[manager_id] == "waiting_photo":
                db_connection = get_connection()
                manager = get_manager(db_connection, manager_id)

                if manager:
                    photo_file_id = message.photo.file_id
                    update_manager_photo(db_connection, manager_id, photo_file_id)

                    del manager_auth_state[manager_id]

                    await message.reply_text(
                        "✅ Фото успешно добавлено! Авторизация завершена.\n"
                        "Теперь вы можете отвечать клиентам."
                    )

                    logger.info(f"Фото менеджера {manager_id} сохранено")
                    return True

        except Exception as e:
            logger.error(f"Ошибка при обработке фото: {e}")

        return False

    @app.on_message(filters.regex(r"^/\d+") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_thread_command(client, message):
        """Handle /{thread_id} command"""
        try:
            manager_id = message.from_user.id
            command_text = message.text.strip()

            # Извлекаем thread_id
            first_word = command_text.split()[0]
            thread_id = int(first_word[1:])

            logger.info(f"Команда /{thread_id} от менеджера {manager_id}")

            # Проверяем авторизацию
            db_connection = get_connection()
            manager = get_manager(db_connection, manager_id)

            if not manager:
                await message.reply_text(
                    "Вы не авторизованы. Используйте /auth для авторизации."
                )
                return

            # Получаем клиента
            client_data = get_client_by_thread(db_connection, thread_id)

            if not client_data:
                await message.reply_text(
                    f"Клиент для треда {thread_id} не найден."
                )
                return

            client_id = client_data[0]

            # Получаем текст ответа
            if " " in command_text:
                reply_text = command_text.split(" ", 1)[1]
            else:
                await message.reply_text("Пожалуйста, добавьте текст после команды.")
                return

            # Отправляем ответ
            success = await send_manager_reply_to_client(client, manager_id, client_id, reply_text)

            if success:
                update_manager_reply_time(db_connection, thread_id)
                await mark_thread_urgent(client, thread_id, is_urgent=False)

                # Обрабатываем первый ответ
                is_first = is_first_reply(db_connection, thread_id, manager_id)

                if is_first:
                    save_first_reply(db_connection, thread_id, client_id, manager_id)

                    _, emoji, name, position, extension, photo_file_id, auth_date, username = unpack_manager_data(manager)

                    if username:
                        assign_duty_manager(db_connection, thread_id, username, manager_id)
                        await message.reply_text(
                            f"✅ Ответ отправлен. Вы назначены ответственным за клиента (тред #{thread_id})."
                        )

                        # Отправляем карточку при первом ответе
                        await send_manager_card_to_client(client, manager_id, client_id)
                    else:
                        await message.reply_text("✅ Ответ отправлен.")
                else:
                    await message.reply_text("✅ Ответ отправлен.")

                logger.info(f"Ответ менеджера {manager_id} отправлен клиенту {client_id}")
            else:
                await message.reply_text("❌ Не удалось отправить ответ.")

        except ValueError:
            logger.error(f"Некорректный формат команды: {message.text}")
            await message.reply_text("Некорректный формат. Используйте: /{thread_id} {текст}")
        except Exception as e:
            logger.error(f"Ошибка при обработке команды: {e}")
            await message.reply_text(f"Произошла ошибка: {e}")

    @app.on_message(filters.command("wtt") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_wtt(client, message):
        """Handle /wtt (what thread this) command"""
        try:
            db_connection = get_connection()

            # Определяем thread_id
            thread_id = None

            if hasattr(message, 'message_thread_id') and message.message_thread_id:
                thread_id = message.message_thread_id
            elif message.reply_to_message:
                thread_id = message.reply_to_message.id

            if not thread_id:
                await message.reply_text(
                    "Не удалось определить ID треда. Используйте команду в треде клиента."
                )
                return

            logger.info(f"Команда /wtt для треда {thread_id}")

            # Получаем информацию о клиенте
            client_data = get_client_by_thread(db_connection, thread_id)

            if not client_data:
                await message.reply_text(f"Информация о треде #{thread_id} не найдена.")
                return

            user_id, first_name, last_name, username, first_contact, last_contact, message_count, _, custom_id = client_data

            # Формируем имя
            client_name = f"{first_name or ''}"
            if last_name:
                client_name += f" {last_name}"
            if username:
                client_name += f" (@{username})"

            # Получаем ответственного
            duty_manager = get_duty_manager(db_connection, thread_id)

            # Формируем ответ
            info = f"📊 **Информация о треде #{thread_id}**\n\n"
            info += f"**Клиент**: {client_name}\n"
            info += f"**ID пользователя**: `{user_id}`\n"

            if custom_id:
                info += f"**ID клиента**: `{custom_id}`\n"

            info += f"**Первое обращение**: {first_contact}\n"
            info += f"**Последнее обращение**: {last_contact}\n"
            info += f"**Сообщений**: {message_count}\n"
            info += f"**Ответственный**: {f'@{duty_manager}' if duty_manager else 'Не назначен'}\n\n"

            info += "**Для ответа используйте**:\n"

            if custom_id:
                info += f"`/#{custom_id} текст` (по ID клиента)\n"

            info += f"`/{thread_id} текст` (по номеру треда)"

            await message.reply_text(info)

        except Exception as e:
            logger.error(f"Ошибка при обработке /wtt: {e}")
            await message.reply_text(f"Произошла ошибка: {e}")

    @app.on_message(filters.command("threads") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_threads(client, message):
        """Handle /threads command"""
        try:
            db_connection = get_connection()

            manager = get_manager(db_connection, message.from_user.id)
            if not manager:
                await message.reply_text(
                    "Вы не авторизованы. Используйте /auth."
                )
                return

            threads = get_all_active_threads(db_connection)

            if not threads:
                await message.reply_text("Нет активных тредов.")
                return

            response = "📋 **Список активных тредов**:\n\n"

            for thread in threads:
                thread_id, user_id, first_name, last_name, username, assigned_manager, last_message_time = thread

                client_name = f"{first_name or ''}"
                if last_name:
                    client_name += f" {last_name}"
                if username:
                    client_name += f" (@{username})"

                response += f"🔹 **Тред #{thread_id}** - {client_name}\n"
                response += f"   👨‍💼 Ответственный: {assigned_manager or 'Не назначен'}\n\n"

            await message.reply_text(response)

            logger.info(f"Список тредов отправлен, всего {len(threads)} тредов")

        except Exception as e:
            logger.error(f"Ошибка при получении списка тредов: {e}")
            await message.reply_text(f"Произошла ошибка: {e}")

    @app.on_message(filters.command("help") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_help(client, message):
        """Handle /help command"""
        help_text = """
📋 **Доступные команды**:

⚙️ **Команды для работы с клиентами**:
- `/{ID_треда} [текст]` - Ответить клиенту
- `/wtt` - Информация о текущем треде
- `/card {ID_треда}` - Отправить свою карточку
- `/threads` - Список всех активных тредов

⚙️ **Команды управления**:
- `/auth [эмодзи], [Имя], [Должность], [4 цифры]` - Авторизация
- `/help` - Показать эту справку

ℹ️ **О назначении ответственных**:
- Первый ответивший менеджер автоматически назначается ответственным
- Ответственный получает уведомления о новых сообщениях
"""
        await message.reply_text(help_text)

    @app.on_message(filters.command("myinfo") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_myinfo(client, message):
        """Handle /myinfo command"""
        try:
            manager_id = message.from_user.id
            db_connection = get_connection()

            manager = get_manager(db_connection, manager_id)

            if not manager:
                await message.reply_text("Вы не зарегистрированы. Используйте /auth.")
                return

            _, emoji, name, position, extension, photo_file_id, auth_date, username = manager

            info = "Ваша информация:\n\n"
            info += f"ID: {manager_id}\n"
            info += f"Emoji: {emoji}\n"
            info += f"Имя: {name}\n"
            info += f"Должность: {position}\n"
            info += f"Добавочный: {extension}\n"
            info += f"Username: {username}\n"
            info += f"Фото: {'Загружено' if photo_file_id else 'Не загружено'}\n"

            await message.reply_text(info)

        except Exception as e:
            logger.error(f"Ошибка при обработке /myinfo: {e}")
            await message.reply_text(f"Произошла ошибка: {e}")

    @app.on_message(filters.command("set_id") & filters.chat(SUPPORT_GROUP_ID))
    async def handle_set_id(client, message):
        """Handle /set_id command"""
        try:
            manager_id = message.from_user.id
            db_connection = get_connection()

            manager = get_manager(db_connection, manager_id)
            if not manager:
                await message.reply_text("Вы не авторизованы. Используйте /auth.")
                return

            parts = message.text.split()

            if len(parts) < 2:
                await message.reply_text(
                    "Формат: /set_id {ID_треда} [желаемый_ID]"
                )
                return

            thread_id = int(parts[1])

            client_data = get_client_by_thread(db_connection, thread_id)
            if not client_data:
                await message.reply_text(f"Клиент для треда {thread_id} не найден.")
                return

            client_id = client_data[0]

            # Генерируем или используем указанный ID
            if len(parts) > 2:
                custom_id = parts[2].upper()

                # Проверяем, не занят ли ID
                cursor = db_connection.cursor()
                cursor.execute('SELECT user_id FROM clients WHERE custom_id = ? AND user_id != ?',
                             (custom_id, client_id))

                if cursor.fetchone():
                    await message.reply_text(f"ID {custom_id} уже занят.")
                    return

                cursor.execute('UPDATE clients SET custom_id = ? WHERE user_id = ?',
                             (custom_id, client_id))
                db_connection.commit()
            else:
                custom_id = generate_client_id(db_connection, client_id, manager_id)

            # Обновляем заголовок треда
            client_name = f"{client_data[1] or ''}"
            if client_data[2]:
                client_name += f" {client_data[2]}"
            if client_data[3]:
                client_name += f" (@{client_data[3]})"

            new_title = f"{custom_id} | {client_name} | тред {thread_id}"
            await edit_thread_title(client, thread_id, new_title)

            await message.reply_text(
                f"✅ Клиенту назначен ID: **{custom_id}**\n\n"
                f"Теперь можно отвечать командой: `/#{custom_id} текст`"
            )

            logger.info(f"Клиенту {client_id} назначен ID {custom_id}")

        except Exception as e:
            logger.error(f"Ошибка при назначении ID: {e}")
            await message.reply_text(f"Произошла ошибка: {e}")
