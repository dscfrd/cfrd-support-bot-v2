"""Helper functions"""

import asyncio
import logging
import random
import string

logger = logging.getLogger(__name__)


async def handle_flood_wait(func, *args, **kwargs):
    """Handle FloodWait errors with automatic retry"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if "FLOOD_WAIT_" in str(e):
                # Извлекаем время ожидания из ошибки
                wait_time = int(str(e).split("FLOOD_WAIT_")[1].split(":")[0])
                logger.warning(f"FloodWait: ждем {wait_time} секунд (попытка {attempt + 1}/{max_retries})")

                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time + 1)
                else:
                    logger.error(f"Не удалось выполнить операцию после {max_retries} попыток")
                    raise
            else:
                raise


def generate_client_id(db_connection, client_id, manager_id):
    """Generate unique client ID"""
    cursor = db_connection.cursor()

    # Получаем информацию о менеджере
    cursor.execute('SELECT name FROM managers WHERE manager_id = ?', (manager_id,))
    manager_data = cursor.fetchone()

    if manager_data and manager_data[0]:
        # Извлекаем инициалы менеджера
        manager_name = manager_data[0]
        name_parts = manager_name.split()

        if len(name_parts) >= 2:
            # Берем первые буквы имени и фамилии
            initials = (name_parts[0][0] + name_parts[1][0]).upper()
        elif len(name_parts) == 1:
            # Если только одно слово, берем первые две буквы
            initials = (name_parts[0][:2]).upper()
        else:
            # По умолчанию
            initials = "XX"
    else:
        # Если менеджер не найден, используем случайные буквы
        initials = ''.join(random.choices(string.ascii_uppercase, k=2))

    # Генерируем уникальный ID в формате: [AA][YYMM][NNN]
    # AA - инициалы менеджера
    # YYMM - год и месяц
    # NNN - уникальный номер (трехзначный)

    import datetime
    now = datetime.datetime.now()
    date_part = now.strftime("%y%m")  # YYMM

    # Ищем максимальный номер для данной комбинации initials+date_part
    prefix = f"{initials}{date_part}"
    cursor.execute('''
    SELECT custom_id FROM clients
    WHERE custom_id LIKE ?
    ORDER BY custom_id DESC
    LIMIT 1
    ''', (f"{prefix}%",))

    result = cursor.fetchone()

    if result:
        # Извлекаем номер из последнего ID
        last_id = result[0]
        try:
            last_number = int(last_id[len(prefix):])
            new_number = last_number + 1
        except (ValueError, IndexError):
            new_number = 1
    else:
        new_number = 1

    # Формируем новый ID
    new_id = f"{prefix}{new_number:03d}"

    # Сохраняем ID в базу данных
    cursor.execute('''
    UPDATE clients SET custom_id = ? WHERE user_id = ?
    ''', (new_id, client_id))
    db_connection.commit()

    return new_id
