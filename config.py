import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Константы для настройки интервалов (в минутах)
URGENT_WAIT_TIME = 10          # Сообщение становится срочным через 10 минут
FIRST_NOTIFICATION_DELAY = 0   # Уведомление отправляется сразу
NOTIFICATION_INTERVAL = 20     # Повторные уведомления каждые 20 минут
CHECK_INTERVAL = 3             # Проверка каждые 3 минуты

# Данные для авторизации Telegram
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

# Bot token для вспомогательных скриптов
BOT_TOKEN = os.getenv("BOT_TOKEN")

# ID группы поддержки и канала хранилища
SUPPORT_GROUP_ID = int(os.getenv("SUPPORT_GROUP_ID"))
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID"))

# Название базы данных
DATABASE_NAME = "clients_main_v2.db"
