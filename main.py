from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from kafka import KafkaProducer
import json
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def load_email_list():
    df = pd.read_excel("email_list.xlsx")
    return df.to_dict(orient="records")  # list of dicts with 'Name' and 'Email'

@app.get("/", response_class=HTMLResponse)
async def form(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})

@app.post("/send", response_class=HTMLResponse)
async def send_bulk_email(request: Request, subject: str = Form(...), message: str = Form(...)):
    email_list = load_email_list()
    for person in email_list:
        personalized_message = f"Hi {person['Name']},\n\n{message}\n\n\nRegards,\nKiwi-Supreme"
        payload = {
            "email": person["Email"],
            "name": person["Name"],
            "subject": subject,
            "message": personalized_message
        }
        producer.send("bulk_email_topic", value=payload)
    producer.flush()
    return templates.TemplateResponse("success.html", {"request": request})
