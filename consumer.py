import json
import os
import asyncio
from kafka import KafkaConsumer
from send_email import send_email
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

consumer = KafkaConsumer(
    "bulk_email_topic",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bulk_email_group"
)

result = {}

async def process_email(data):
    recipient = data["email"]
    subject = data["subject"]
    message = data["message"]
    success = await send_email(recipient, subject, message)
    result[recipient] = success
    with open("result_status.json", "w") as f:
        json.dump(result, f, indent=4)

async def main():
    print("Consumer started...")
    for msg in consumer:
        data = msg.value
        await process_email(data)

if __name__ == "__main__":
    asyncio.run(main())
