"""
Конфигурация бота поддержки CFRD
"""

import pyrogram

# === API настройки (ТЕСТОВЫЕ) ===
API_ID = 27337424
API_HASH = "4f5d8461e55fc3578c7659195a107def"
PHONE_NUMBER = "+79851730392"
SESSION_NAME = "business_account_test"

# === Группа поддержки (ТЕСТОВАЯ) ===
SUPPORT_GROUP_ID = -1003317645437

# === База данных ===
DATABASE_NAME = "clients_test.db"

# === Таймауты и интервалы (в минутах) ===
URGENT_WAIT_TIME = 10          # Сообщение становится срочным через N минут
FIRST_NOTIFICATION_DELAY = 0   # Уведомление отправляется сразу (0 = сразу после URGENT_WAIT_TIME)
NOTIFICATION_INTERVAL = 20     # Повторные уведомления каждые N минут
CHECK_INTERVAL = 3             # Проверка неотвеченных каждые N минут

# === Pyrogram настройки ===
PARSE_MODE = pyrogram.enums.ParseMode.MARKDOWN
WORKERS = 16
