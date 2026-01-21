"""
Конфигурация бота поддержки CFRD - БОЕВАЯ
"""

import pyrogram

# === API настройки ===
API_ID = 27337424
API_HASH = "4f5d8461e55fc3578c7659195a107def"
PHONE_NUMBER = "+79859482949"  # БОЕВОЙ НОМЕР
SESSION_NAME = "business_account"

# === Группа поддержки (БОЕВАЯ) ===
SUPPORT_GROUP_ID = -1002675883945

# === Канал-хранилище файлов ===
# TODO: Создать новый канал или использовать существующий
FILES_CHANNEL_ID = -1003563685324

# === Кастом эмодзи менеджеров ===
EMOJI_PACK_NAME = "cfrd_managers_emoji"

# === База данных ===
DATABASE_NAME = "clients.db"

# === Таймауты и интервалы (в минутах) ===
URGENT_WAIT_TIME = 10          # Сообщение становится срочным через N минут
FIRST_NOTIFICATION_DELAY = 0   # Уведомление отправляется сразу (0 = сразу после URGENT_WAIT_TIME)
NOTIFICATION_INTERVAL = 20     # Повторные уведомления каждые N минут
CHECK_INTERVAL = 3             # Проверка неотвеченных каждые N минут

# === Мониторинг ===
MONITORING_CHAT_ID = -1002675883945  # Группа поддержки для уведомлений мониторинга
HEARTBEAT_INTERVAL = 60              # Heartbeat каждые N минут (0 = отключить)
MONITORING_ENABLED = True            # Включить мониторинг

# === Pyrogram настройки ===
PARSE_MODE = pyrogram.enums.ParseMode.MARKDOWN
WORKERS = 16
