#!/usr/bin/env python3
"""Проверка группы"""

from pyrogram import Client

business = Client(
    "business_account_test",
    api_id=27337424,
    api_hash="4f5d8461e55fc3578c7659195a107def",
    phone_number="+79851730392"
)

async def main():
    async with business:
        chat = await business.get_chat(-1002369353918)
        print(f"Название: {chat.title}")
        print(f"ID: {chat.id}")
        print(f"Тип: {chat.type}")
        print(f"is_forum: {chat.is_forum}")

        # Попробуем получить топики
        print("\nТопики:")
        try:
            async for topic in business.get_forum_topics(-1002369353918):
                print(f"  - {topic.id}: {topic.title}")
        except Exception as e:
            print(f"  Ошибка: {e}")

business.run(main())
