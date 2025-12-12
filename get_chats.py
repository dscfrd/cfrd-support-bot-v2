#!/usr/bin/env python3
"""Скрипт для получения списка групп аккаунта"""

from pyrogram import Client

business = Client(
    "business_account_test",
    api_id=27337424,
    api_hash="4f5d8461e55fc3578c7659195a107def",
    phone_number="+79851730392"
)

async def main():
    async with business:
        print("Список групп и супергрупп:\n")
        async for dialog in business.get_dialogs():
            chat = dialog.chat
            if chat.type.name in ["GROUP", "SUPERGROUP"]:
                print(f"Название: {chat.title}")
                print(f"ID: {chat.id}")
                print(f"Тип: {chat.type.name}")
                print("-" * 40)

business.run(main())
