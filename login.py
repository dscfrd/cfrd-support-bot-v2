#!/usr/bin/env python3
"""Run this script interactively to login: python3 login.py"""
from pyrogram import Client
import pyrogram

app = Client(
    'business_account_test',
    api_id=27337424,
    api_hash="4f5d8461e55fc3578c7659195a107def",
    phone_number="+79851730392",
    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
)

with app:
    me = app.get_me()
    print(f"Logged in as: {me.first_name} (@{me.username})")
    print("Session saved! You can now run bot.py")
