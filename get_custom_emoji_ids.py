#!/usr/bin/env python3
"""
Скрипт для получения ID кастомных эмодзи из стикерпака
Использует bot token для работы
"""

import asyncio
from pyrogram import Client
from config import API_ID, API_HASH, BOT_TOKEN

# Создаём бот-клиент
app = Client(
    "emoji_extractor",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)


async def get_custom_emojis_from_pack(sticker_set_name: str):
    """
    Получить все кастомные эмодзи из стикерпака

    Args:
        sticker_set_name: Короткое имя стикерпака (например "teamcfrd")
    """
    try:
        async with app:
            print(f"Получаем информацию о стикерпаке: {sticker_set_name}")

            # Получаем стикерпак
            sticker_set = await app.get_sticker_set(sticker_set_name)

            print(f"\nНазвание: {sticker_set.title}")
            print(f"Короткое имя: {sticker_set.short_name}")
            print(f"Количество стикеров: {len(sticker_set.stickers)}\n")

            # Выводим информацию о каждом стикере
            for i, sticker in enumerate(sticker_set.stickers, 1):
                print(f"Стикер {i}:")
                print(f"  File ID: {sticker.file_id}")
                print(f"  File Unique ID: {sticker.file_unique_id}")

                # Для кастомных эмодзи
                if hasattr(sticker, 'custom_emoji_id') and sticker.custom_emoji_id:
                    print(f"  Custom Emoji ID: {sticker.custom_emoji_id}")

                # Emoji ассоциированные со стикером
                if hasattr(sticker, 'emoji') and sticker.emoji:
                    print(f"  Emoji: {sticker.emoji}")

                print()

            # Формируем маппинг для копирования в код
            print("\n" + "="*60)
            print("МАППИНГ ДЛЯ emoji_mapper.py:")
            print("="*60)
            print("EMOJI_MAPPING = {")

            for sticker in sticker_set.stickers:
                if hasattr(sticker, 'emoji') and sticker.emoji:
                    # Используем file_id как custom_emoji_id для обычных стикеров
                    # Если это custom emoji стикерпак, используем custom_emoji_id
                    emoji_id = sticker.custom_emoji_id if hasattr(sticker, 'custom_emoji_id') and sticker.custom_emoji_id else sticker.file_id
                    print(f'    "{sticker.emoji}": "{emoji_id}",')

            print("}")

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # ЗАМЕНИТЕ на короткое имя вашего стикерпака
    # Короткое имя можно найти в URL: https://t.me/addstickers/SHORT_NAME
    sticker_set_name = "teamcfrd"

    asyncio.run(get_custom_emojis_from_pack(sticker_set_name))
