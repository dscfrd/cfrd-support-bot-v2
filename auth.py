from pyrogram import Client
import pyrogram
import sys

code = sys.argv[1] if len(sys.argv) > 1 else input("Code: ")

app = Client(
    'business_account_test',
    api_id=27337424,
    api_hash="4f5d8461e55fc3578c7659195a107def",
    phone_number="+79851730392",
    parse_mode=pyrogram.enums.ParseMode.MARKDOWN
)

async def main():
    await app.connect()

    sent_code = await app.send_code(app.phone_number)

    try:
        signed_in = await app.sign_in(app.phone_number, sent_code.phone_code_hash, code)
        print(f"Signed in as: {signed_in.first_name}")
    except Exception as e:
        print(f"Error: {e}")
        return

    await app.disconnect()

import asyncio
asyncio.run(main())
