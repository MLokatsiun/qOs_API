import json
import uuid
import pandas
import pandas as pd
from fastapi import Query, File, HTTPException, Header, Depends, APIRouter, BackgroundTasks, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from database import get_db
from models import User, Request
from confluent_kafka import Producer
from datetime import datetime
from decouple import config
import numpy as np


KAFKA_BROKER = config("KAFKA_BROKER")
REQUEST_TOPIC = config("REQUEST_TOPIC")

router = APIRouter()

def send_request_to_kafka(request_id: str, tg_id: str, request_data: str, command: str, search_country: str):
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    kafka_request_data = {
        "request_id": request_id,
        "indicator": tg_id,
        "response_param": request_data,
        "command": command,
        "search_country": search_country
    }
    try:
        producer.produce(
            REQUEST_TOPIC,
            value=json.dumps(kafka_request_data).encode('utf-8'),
            callback=lambda err, msg: print(f"Kafka error: {err}") if err else None
        )
        producer.flush()
    except Exception as e:
        print(f"Error while sending message to Kafka: {str(e)}")


@router.post("/generate_pdf/")
async def generate_pdf(
        tg_id: str = Query(None),
        request_data: str = Query(None),
        command: str = Query(None),
        search_country: str = Query(None),
        file: UploadFile = File(None),
        api_key: str = Header(...),
        db: AsyncSession = Depends(get_db),
        background_tasks: BackgroundTasks = BackgroundTasks()
):
    result = await db.execute(select(User).where(User.api_key == api_key))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    request_ids = []
    batch_key = None

    if file:
        try:
            df = pandas.read_excel(file.file)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Invalid file format. {str(ve)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"An unexpected error occurred: {str(e)}")

        required_columns = ['id', 'ФИО', 'ИНН', 'НОМЕР ТЕЛЕФОНА', 'TG_ID', 'FACEBOOK_ID', 'VK_ID']

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")

        batch_key = str(uuid.uuid4())

        for _, row in df.iterrows():
            if pd.notna(row['ФИО']):
                request_param = str(row['ФИО'])
                command = 'FIO'
            elif pd.notna(row['ИНН']):
                request_param = str(row['ИНН'])
                command = 'INN'
            elif pd.notna(row['НОМЕР ТЕЛЕФОНА']):
                request_param = str(row['НОМЕР ТЕЛЕФОНА'])
                command = 'PHONE'
            elif pd.notna(row['TG_ID']):
                request_param = str(row['TG_ID'])
                command = '#'
            elif pd.notna(row['FACEBOOK_ID']):
                request_param = str(row['FACEBOOK_ID'])
                command = 'VK'
            elif pd.notna(row['VK_ID']):
                request_param = str(row['VK_ID'])
                command = 'FB'
            else:
                continue

            request_id = str(uuid.uuid4())
            new_request = Request(
                request_id=request_id,
                tg_id=tg_id,
                user_id=user.id,
                status="pending",
                created_at=datetime.utcnow(),
                indicator=tg_id,
                request_param=request_param,
                command=command,
                batch_key=batch_key
            )
            db.add(new_request)
            request_ids.append(request_id)

            background_tasks.add_task(send_request_to_kafka, request_id, tg_id, request_param, command,
                                      search_country)

    else:
        if not tg_id or not request_data or not command:
            raise HTTPException(status_code=400, detail="Missing required parameters.")

        request_id = str(uuid.uuid4())
        new_request = Request(
            request_id=request_id,
            tg_id=tg_id,
            user_id=user.id,
            status="pending",
            created_at=datetime.utcnow(),
            indicator=tg_id,
            request_param=request_data,
            command=command,
            batch_key=None
        )
        db.add(new_request)
        request_ids.append(request_id)

        background_tasks.add_task(send_request_to_kafka, request_id, tg_id, request_data, command, search_country)

    await db.commit()
    return {
        "message": "Request(s) processed successfully.",
        "request_ids": request_ids,
        "batch_key": batch_key if file else None
    }
