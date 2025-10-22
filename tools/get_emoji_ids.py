#!/usr/bin/env python3
"""
Скрипт для получения ID кастомных эмодзи из сообщений

Использование:
1. Запустите скрипт: python tools/get_emoji_ids.py
2. Отправьте боту сообщения с кастомными эмодзи из стикерпака teamcfrd
3. Бот выведет ID каждого эмодзи
4. Скопируйте ID и добавьте их в bot/utils/emoji_mapper.py
"""

import sys
import os

# Добавляем корневую директорию в путь для импорта config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyrogram import Client, filters
from config import API_ID, API_HASH, BOT_TOKEN

print("=" * 60)
print("Инструмент для получения ID кастомных эмодзи")
print("=" * 60)
print()
print("Отправьте боту сообщения с кастомными эмодзи из стикерпака")
print("Бот выведет ID каждого эмодзи")
print()
print("Для выхода нажмите Ctrl+C")
print("=" * 60)
print()

app = Client(
    "emoji_id_getter",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)


@app.on_message(filters.private & filters.text)
async def get_emoji_ids(client, message):
    """Обработчик для получения ID эмодзи"""
    print(f"\nПолучено сообщение от {message.from_user.first_name} (ID: {message.from_user.id})")
    print(f"Текст: {message.text}")

    if message.entities:
        found_custom_emoji = False

        for entity in message.entities:
            if entity.type.name == "CUSTOM_EMOJI":
                found_custom_emoji = True
                emoji_text = message.text[entity.offset:entity.offset + entity.length]
                emoji_id = entity.custom_emoji_id

                print(f"\n✅ Найден кастомный эмодзи:")
                print(f"   Эмодзи: {emoji_text}")
                print(f"   ID: {emoji_id}")
                print(f"   Позиция: {entity.offset}-{entity.offset + entity.length}")

                # Отправляем ответ пользователю
                await message.reply(
                    f"**Кастомный эмодзи найден:**\n\n"
                    f"Эмодзи: {emoji_text}\n"
                    f"ID: `{emoji_id}`\n\n"
                    f"Скопируйте ID и добавьте в bot/utils/emoji_mapper.py"
                )

        if not found_custom_emoji:
            print("❌ Кастомных эмодзи не найдено в сообщении")
            await message.reply(
                "В вашем сообщении нет кастомных эмодзи.\n"
                "Отправьте сообщение с кастомным эмодзи из стикерпака teamcfrd."
            )
    else:
        print("❌ В сообщении нет entities")
        await message.reply(
            "В вашем сообщении нет эмодзи.\n"
            "Отправьте сообщение с кастомным эмодзи из стикерпака teamcfrd."
        )

    print("-" * 60)


@app.on_message(filters.private & ~filters.text)
async def handle_other(client, message):
    """Обработчик для других типов сообщений"""
    await message.reply(
        "Пожалуйста, отправьте текстовое сообщение с кастомным эмодзи.\n"
        "Например: 🔧 или любой другой эмодзи из стикерпака teamcfrd"
    )


if __name__ == "__main__":
    try:
        print("Запуск бота...")
        app.run()
    except KeyboardInterrupt:
        print("\n\nБот остановлен")
        print("=" * 60)
