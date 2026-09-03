import os
import requests

TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

payload = {
    "chat_id": CHAT_ID,
    "text": "✅ Telegram test successful!"
}

response = requests.post(url, data=payload, timeout=20)

print("HTTP STATUS:", response.status_code)
print("TELEGRAM RESPONSE:", response.text)

response.raise_for_status()
