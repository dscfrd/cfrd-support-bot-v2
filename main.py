"""
CFRD Support Bot v2
Main entry point
"""

import asyncio
import logging
from pyrogram import Client

from config import API_ID, API_HASH, PHONE_NUMBER, SUPPORT_GROUP_ID, CHECK_INTERVAL
from bot.database import setup_database
from bot.handlers.client_messages import setup_client_handlers
from bot.handlers.manager_commands import setup_manager_handlers
from bot.services.notification_service import check_unanswered_messages
from bot.services.media_service import cleanup_media_groups

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Инициализация клиента
app = Client(
    "cfrd_support_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE_NUMBER
)


async def schedule_checks():
    """Планировщик проверок неотвеченных сообщений"""
    # Начальная задержка
    await asyncio.sleep(30)

    while True:
        try:
            logger.info("Запуск проверки неотвеченных сообщений...")
            await check_unanswered_messages(app)

            await asyncio.sleep(CHECK_INTERVAL * 60)
        except Exception as e:
            logger.error(f"Ошибка в планировщике проверок: {e}")
            await asyncio.sleep(60)


async def main():
    """Главная функция"""
    try:
        logger.info("Запуск CFRD Support Bot v2...")

        # Инициализация базы данных
        setup_database()
        logger.info("База данных инициализирована")

        # Регистрация обработчиков
        setup_client_handlers(app)
        setup_manager_handlers(app)
        logger.info("Обработчики зарегистрированы")

        # Запуск клиента
        await app.start()
        logger.info(f"Бот запущен. Группа поддержки: {SUPPORT_GROUP_ID}")

        # Запуск фоновых задач
        app.loop.create_task(schedule_checks())
        app.loop.create_task(cleanup_media_groups())
        logger.info("Фоновые задачи запущены")

        # Держим бота запущенным
        await asyncio.Event().wait()

    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        await app.stop()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    asyncio.run(main())
