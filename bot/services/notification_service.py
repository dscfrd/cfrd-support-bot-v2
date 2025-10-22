"""Notification service for unanswered messages"""

import datetime
import logging
from config import (
    SUPPORT_GROUP_ID,
    URGENT_WAIT_TIME,
    FIRST_NOTIFICATION_DELAY,
    NOTIFICATION_INTERVAL
)
from bot.database.queries import get_duty_manager
from .thread_service import mark_thread_urgent

logger = logging.getLogger(__name__)


async def check_unanswered_messages(client):
    """Check for unanswered messages and send notifications"""
    try:
        from bot.database import get_connection
        db_connection = get_connection()

        cursor = db_connection.cursor()
        current_time = datetime.datetime.now()

        # Получаем все треды, требующие внимания
        cursor.execute('''
        SELECT
            ts.thread_id,
            ts.last_client_message,
            ts.last_manager_reply,
            ts.is_notified,
            ts.last_notification,
            ts.notification_disabled,
            c.first_name,
            c.last_name,
            c.username,
            c.custom_id
        FROM thread_status ts
        LEFT JOIN clients c ON ts.thread_id = c.thread_id
        WHERE ts.notification_disabled = 0
        AND ts.last_client_message IS NOT NULL
        AND (ts.last_manager_reply IS NULL OR ts.last_client_message > ts.last_manager_reply)
        ''')

        threads_to_notify = cursor.fetchall()

        for thread_data in threads_to_notify:
            (thread_id, last_client_msg, last_manager_reply, is_notified,
             last_notification, notification_disabled, first_name, last_name, username, custom_id) = thread_data

            # Преобразуем строки времени в datetime
            if isinstance(last_client_msg, str):
                last_client_msg = datetime.datetime.strptime(last_client_msg, '%Y-%m-%d %H:%M:%S.%f')

            # Вычисляем время с момента последнего сообщения клиента
            time_since_message = (current_time - last_client_msg).total_seconds() / 60

            # Проверяем, нужно ли пометить тред как срочный
            if time_since_message >= URGENT_WAIT_TIME:
                await mark_thread_urgent(client, thread_id, is_urgent=True)

            # Проверяем, нужно ли отправлять уведомление
            should_notify = False

            if not is_notified and time_since_message >= FIRST_NOTIFICATION_DELAY:
                # Первое уведомление
                should_notify = True
            elif is_notified and last_notification:
                # Повторное уведомление
                if isinstance(last_notification, str):
                    last_notification = datetime.datetime.strptime(last_notification, '%Y-%m-%d %H:%M:%S.%f')

                time_since_last_notification = (current_time - last_notification).total_seconds() / 60

                if time_since_last_notification >= NOTIFICATION_INTERVAL:
                    should_notify = True

            if should_notify:
                # Формируем имя клиента
                client_name = f"{first_name or ''}"
                if last_name:
                    client_name += f" {last_name}"
                if username:
                    client_name += f" (@{username})"

                # Получаем ответственного менеджера
                duty_manager = get_duty_manager(db_connection, thread_id)

                # Формируем уведомление
                notification_text = f"⚠️ **Непрочитанное сообщение!**\n\n"
                notification_text += f"**Клиент**: {client_name}\n"

                if custom_id:
                    notification_text += f"**ID клиента**: {custom_id}\n"

                notification_text += f"**Тред**: #{thread_id}\n"
                notification_text += f"**Время ожидания**: {int(time_since_message)} мин\n\n"

                if duty_manager:
                    notification_text += f"**Ответственный**: @{duty_manager}\n\n"

                notification_text += "Пожалуйста, ответьте клиенту!"

                # Отправляем уведомление в тред
                try:
                    await client.send_message(
                        chat_id=SUPPORT_GROUP_ID,
                        text=notification_text,
                        reply_to_message_id=thread_id
                    )

                    # Обновляем статус уведомления
                    cursor.execute('''
                    UPDATE thread_status
                    SET is_notified = 1, last_notification = ?
                    WHERE thread_id = ?
                    ''', (current_time, thread_id))
                    db_connection.commit()

                    logger.info(f"Отправлено уведомление для треда {thread_id}")

                except Exception as e:
                    logger.error(f"Ошибка при отправке уведомления для треда {thread_id}: {e}")

    except Exception as e:
        logger.error(f"Ошибка в системе уведомлений: {e}")
