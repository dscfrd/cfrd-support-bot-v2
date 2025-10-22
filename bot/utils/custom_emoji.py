"""Utilities for working with custom emoji in Telegram"""

import re
import logging
from typing import List, Tuple, Optional
from pyrogram import types
from bot.utils.emoji_mapper import get_custom_emoji_id, has_custom_emoji

logger = logging.getLogger(__name__)


def find_emoji_positions(text: str) -> List[Tuple[str, int, int]]:
    """
    Найти все эмодзи в тексте и их позиции

    Args:
        text: Текст для поиска

    Returns:
        Список кортежей (emoji, start_position, end_position)
    """
    # Регулярное выражение для поиска эмодзи
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )

    emoji_list = []
    for match in emoji_pattern.finditer(text):
        emoji = match.group()
        start = match.start()
        end = match.end()
        emoji_list.append((emoji, start, end))

    return emoji_list


def create_custom_emoji_entities(text: str) -> Tuple[str, Optional[List[types.MessageEntity]]]:
    """
    Создать entities для кастомных эмодзи в тексте
    """
    logger.info(f"🔍 create_custom_emoji_entities вызвана для текста: {text}")

    emoji_positions = find_emoji_positions(text)

    logger.info(f"🔍 Найдено эмодзи: {emoji_positions}")

    if not emoji_positions:
        logger.info("🔍 Эмодзи не найдены")
        return text, None

    entities = []

    for emoji, start, end in emoji_positions:
        custom_emoji_id = get_custom_emoji_id(emoji)

        logger.info(f"🔍 Эмодзи '{emoji}' -> custom_id: {custom_emoji_id}")

        if custom_emoji_id:
            entity = types.MessageEntity(
                type=types.enums.MessageEntityType.CUSTOM_EMOJI,
                offset=start,
                length=end - start,
                custom_emoji_id=custom_emoji_id
            )
            entities.append(entity)
            logger.info(f"✅ Создан entity для эмодзи '{emoji}' с ID {custom_emoji_id}")

    logger.info(f"🔍 Всего создано entities: {len(entities)}")
    return text, entities if entities else None


def format_signature_with_custom_emoji(emoji: str, name: str, position: str, extension: str) -> Tuple[str, Optional[List[types.MessageEntity]]]:
    """
    Форматировать подпись менеджера с кастомными эмодзи

    Args:
        emoji: Обычный эмодзи менеджера
        name: Имя менеджера
        position: Должность менеджера
        extension: Добавочный номер

    Returns:
        Кортеж (signature_text, entities):
        - signature_text: Текст подписи
        - entities: Список MessageEntity для кастомных эмодзи или None
    """
    signature = f"{emoji} {name}, {position}, доб. {extension}"
    return create_custom_emoji_entities(signature)


def format_card_with_custom_emoji(emoji: str, name: str, position: str, extension: str, username: Optional[str] = None) -> Tuple[str, Optional[List[types.MessageEntity]]]:
    """
    Форматировать карточку менеджера с кастомными эмодзи

    Args:
        emoji: Обычный эмодзи менеджера
        name: Имя менеджера
        position: Должность менеджера
        extension: Добавочный номер
        username: Username менеджера (опционально)

    Returns:
        Кортеж (card_text, entities):
        - card_text: Текст карточки
        - entities: Список MessageEntity для кастомных эмодзи или None
    """
    card_text = f"{emoji} **{name}**\n"
    card_text += f"_{position}_\n\n"
    card_text += f"📞 Добавочный: {extension}\n"

    if username:
        card_text += f"💬 Telegram: @{username}"

    return create_custom_emoji_entities(card_text)
