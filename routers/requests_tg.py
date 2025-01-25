import json
import uuid
import pandas
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

def send_request_to_kafka(request_id: str, tg_id: str, request_data: str, command: str):
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    kafka_request_data = {
        "request_id": request_id,
        "indicator": tg_id,
        "response_param": request_data,
        "command": command,
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

        if "request_param" not in df.columns:
            raise HTTPException(status_code=400, detail="Excel file must contain 'request_param' column.")

        batch_key = str(uuid.uuid4())

        for _, row in df.iterrows():
            request_param = row["request_param"]
            if not request_param:
                continue

            if isinstance(request_param, (np.int64, int, float)):
                request_param = str(request_param)
            elif isinstance(request_param, str):
                request_param = request_param.strip()
            else:
                request_param = str(request_param)

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

            background_tasks.add_task(send_request_to_kafka, request_id, tg_id, request_param, command)
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

        background_tasks.add_task(send_request_to_kafka, request_id, tg_id, request_data, command)

    await db.commit()
    return {
        "message": "Request(s) processed successfully.",
        "request_ids": request_ids,
        "batch_key": batch_key if file else None
    }

