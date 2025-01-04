import uuid
import json
import base64
import os
from enum import Enum
from datetime import datetime
from fastapi import FastAPI, HTTPException, Header, Depends, APIRouter
from confluent_kafka import Producer, Consumer, KafkaError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from database import get_db
from models import User, Request
from decouple import config

KAFKA_BROKER = config("KAFKA_BROKER")
REQUEST_TOPIC = config("REQUEST_TOPIC")
RESPONSE_TOPIC = config("RESPONSE_TOPIC")

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': f'pdf_response_group',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe([RESPONSE_TOPIC])

class RequestStatus(Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"

app = FastAPI()
router = APIRouter()

async def verify_api_key(api_key: str, db: AsyncSession):
    result = await db.execute(select(User).where(User.api_key == api_key))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return user

def send_pdf_request(tg_id, request_id):
    message = {
        "request_id": request_id,
        "tg_id": tg_id,
    }
    producer.produce(REQUEST_TOPIC, key=request_id, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print(f"Message sent to Kafka: {message}")

def save_pdf_from_base64(pdf_base64, filename):
    try:
        pdf_data = base64.b64decode(pdf_base64)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "wb") as f:
            f.write(pdf_data)
        print(f"PDF saved as {filename}")
    except Exception as e:
        print(f"Error saving PDF: {str(e)}")

async def update_request_with_pdf(db: AsyncSession, request_id: str, pdf_base64: str):
    result = await db.execute(select(Request).where(Request.request_id == request_id))
    request = result.scalars().first()
    if request:
        request.pdf_base64 = pdf_base64
        request.status = RequestStatus.completed.value
        await db.commit()
        await db.refresh(request)
        return request
    else:
        raise HTTPException(status_code=404, detail="Request not found")

@router.post("/generate_pdf/")
async def generate_pdf(
    tg_id: str,
    api_key: str = Header(...),
    db: AsyncSession = Depends(get_db),
):
    user = await verify_api_key(api_key, db)
    request_id = str(uuid.uuid4())

    new_request = Request(
        request_id=request_id,
        tg_id=tg_id,
        user_id=user.id,
        status=RequestStatus.pending.value,
        created_at=datetime.utcnow(),
    )
    db.add(new_request)
    await db.commit()
    await db.refresh(new_request)

    send_pdf_request(tg_id, request_id)

    try:
        while True:
            msg = consumer.poll(timeout=1)
            if msg is None:
                print("Waiting for messages...")
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                raise HTTPException(status_code=500, detail="Error in Kafka communication")

            response_data = json.loads(msg.value().decode('utf-8'))
            if response_data["request_id"] == request_id:
                print(f"Response received: {response_data}")

                pdf_base64 = response_data["pdf_base64"]
                filename = f"pdf/{tg_id}.pdf"
                save_pdf_from_base64(pdf_base64, filename)

                updated_request = await update_request_with_pdf(db, request_id, pdf_base64)

                return {
                    "tg_id": tg_id,
                    "status": updated_request.status,
                    "pdf_filename": filename,
                    "pdf_base64": pdf_base64,
                }

    except Exception as e:
        result = await db.execute(select(Request).where(Request.request_id == request_id))
        request = result.scalars().first()
        if request:
            request.status = RequestStatus.failed.value
            await db.commit()
        raise HTTPException(status_code=500, detail=f"Error during processing: {str(e)}")

@app.on_event("startup")
async def startup_event():
    print("Kafka Consumer started.")
    import asyncio
    loop = asyncio.get_event_loop()
    loop.create_task(consume_responses())

async def consume_responses():
    try:
        while True:
            msg = consumer.poll(timeout=1)
            if msg is None:
                print("Waiting for messages...")
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            response_data = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {response_data}")

    except Exception as e:
        print(f"Error during Kafka consumption: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down Kafka Consumer...")
    consumer.close()
    print("Kafka Consumer closed.")
