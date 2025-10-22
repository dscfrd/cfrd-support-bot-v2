import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Константы для настройки интервалов
URGENT_WAIT_TIME = 1         # Сообщение становится срочным через 10 минут
FIRST_NOTIFICATION_DELAY = 0  # Уведомление отправляется сразу
NOTIFICATION_INTERVAL = 1    # Повторные уведомления каждые 20 минут
CHECK_INTERVAL = 0.5            # Проверка каждые 3 минуты

# Данные для авторизации
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")

# ID группы поддержки
SUPPORT_GROUP_ID = int(os.getenv("SUPPORT_GROUP_ID"))
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID"))