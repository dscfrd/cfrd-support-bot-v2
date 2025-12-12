#!/usr/bin/env python3
"""Получить ID группы по ссылке"""

from pyrogram import Client

business = Client(
    "business_account_test",
    api_id=27337424,
    api_hash="4f5d8461e55fc3578c7659195a107def",
    phone_number="+79851730392"
)

async def main():
    async with business:
        # Получаем информацию о группе по ссылке
        try:
            chat = await business.get_chat("https://t.me/+WB3490pq_NQ0NzVi")
            print(f"Название: {chat.title}")
            print(f"ID: {chat.id}")
            print(f"Тип: {chat.type}")
        except Exception as e:
            print(f"Ошибка: {e}")
            # Попробуем join
            try:
                chat = await business.join_chat("https://t.me/+WB3490pq_NQ0NzVi")
                print(f"Присоединились к группе:")
                print(f"Название: {chat.title}")
                print(f"ID: {chat.id}")
            except Exception as e2:
                print(f"Ошибка join: {e2}")

business.run(main())
