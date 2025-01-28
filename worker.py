import asyncio

from sqlalchemy.testing import db

from celery_config import celery_app
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from database import get_db
from models import Request
import json
import uuid
import requests
import sqlite3
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
import io
from decouple import config
import logging
KAFKA_BROKER = config("KAFKA_BROKER")
RESPONSE_TOPIC = config("RESPONSE_TOPIC")
TELEGRAM_BOT_TOKEN = config("TELEGRAM_BOT_TOKEN")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import io
import json

try:
    pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
    pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
    print("Fonts registered successfully!")
except Exception as e:
    print(f"Error registering fonts: {e}")

def generate_pdf(request_id, response_param, command, indicator):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)

    c.setFont("DejaVuSans-Bold", 14)  

    y_position = 680
    c.setFont("DejaVuSans", 12)

    if not response_param or not isinstance(response_param, list):
        c.drawString(50, y_position, "No valid data available in response_param.")
    else:
        for idx, item in enumerate(response_param, start=1):
            if not isinstance(item, dict):
                c.drawString(50, y_position, f"Invalid item at index {idx}: {item}")
                y_position -= 20
                continue

            source = item.get("Источник", f"Item {idx}")
            c.setFont("DejaVuSans-Bold", 12)
            c.drawString(50, y_position, f"{idx}. Source: {source}")
            y_position -= 20

            c.setFont("DejaVuSans", 11)
            for key, value in item.items():
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False, indent=2)
                c.drawString(70, y_position, f"{key}: {value}")
                y_position -= 15

                if y_position < 50:
                    c.showPage()
                    c.setFont("DejaVuSans", 12)
                    y_position = 750

            y_position -= 10

    c.save()
    buffer.seek(0)
    return buffer.read()



@celery_app.task
def send_pdf_to_user(chat_id, pdf_content, filename):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    files = {
        'document': (filename, pdf_content, 'application/pdf')
    }
    params = {
        'chat_id': chat_id,
        'caption': 'Here is your response in PDF format.'
    }
    response = requests.post(url, params=params, files=files)
    return response.json()


@celery_app.task
def send_db_to_user(chat_id, db_filename):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(db_filename, 'rb') as file:
        files = {
            'document': file
        }
        params = {
            'chat_id': chat_id,
            'caption': f'Here is your SQLite database file: {db_filename}'
        }
        response = requests.post(url, params=params, files=files)
    return response.json()


@celery_app.task
def consume_responses():
    from database import SyncSessionLocal
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'response-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([RESPONSE_TOPIC])

    batch_requests = {}

    try:
        while True:
            msg = consumer.poll(5.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.warning(f"End of partition reached: {msg.error()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())
            else:
                logger.info(f"Received raw message from Kafka: {msg.value()}")

                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Decoded Kafka message: {message_data}")

                    request_id = message_data.get("requestId")
                    response_param = message_data.get("response_param")
                    command = message_data.get("command")
                    indicator = message_data.get("indicator")

                    logger.info(f"Extracted fields - request_id: {request_id}, response_param: {response_param}, "
                                f"command: {command}, indicator: {indicator}")

                    if not is_valid_uuid(request_id):
                        logger.warning(f"Invalid UUID format for request_id: {request_id}. Ignoring the message.")
                        continue

                    if not request_id or not response_param or not command or not indicator:
                        logger.warning(f"Invalid message format or missing required fields. Ignoring the message: {message_data}")
                        continue

                    try:
                        chat_id = int(indicator)
                    except ValueError:
                        logger.warning(f"Invalid chat_id (indicator): {indicator}. Ignoring the message.")
                        continue

                    with SyncSessionLocal() as session:

                        result = session.execute(select(Request).where(Request.request_id == request_id))
                        request_record = result.scalars().first()

                        if not request_record:
                            logger.warning(f"Request with id {request_id} not found in the database. Ignoring the message.")
                            continue

                        request_record.response_param = response_param
                        request_record.status = "completed"
                        session.commit()
                        logger.info(f"Request {request_id} updated in the database: response_param set and status changed to 'COMPLETE'")


                        if request_record.batch_key is None:
                            pdf_content = generate_pdf(request_id, response_param, command, indicator)
                            send_pdf_to_user(chat_id, pdf_content, f"response_{request_id}.pdf")
                            logger.info(f"PDF generated and sent to user {chat_id} for request_id: {request_id}")

                        else:
                            if request_record.batch_key not in batch_requests:
                                batch_requests[request_record.batch_key] = {}

                            if chat_id not in batch_requests[request_record.batch_key]:
                                batch_requests[request_record.batch_key][chat_id] = []

                            batch_requests[request_record.batch_key][chat_id].append(request_record)

                            result = session.execute(select(Request).where(Request.batch_key == request_record.batch_key))
                            all_requests = result.scalars().all()
                            completed_requests = [r for r in all_requests if r.status == 'completed']

                            if len(all_requests) == len(completed_requests):
                                for user_chat_id, user_requests in batch_requests[request_record.batch_key].items():
                                    db_filename = generate_sqlite_db_for_user(user_requests, user_chat_id,
                                                                              request_record.batch_key)
                                    send_db_to_user(user_chat_id, db_filename)
                                    logger.info(f"Batch {request_record.batch_key} processed and sent to user {user_chat_id}")
                                batch_requests.pop(request_record.batch_key)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode Kafka message: {msg.value()} - Error: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error while processing message: {msg.value()} - Error: {e}")

    finally:
        consumer.close()


def is_valid_uuid(value):
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False

@celery_app.task
def generate_sqlite_db_for_user(requests, user_chat_id, batch_key):
    db_filename = f"{batch_key}_user_{user_chat_id}.db"
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS requests (
        request_id TEXT PRIMARY KEY,
        response_param TEXT,  -- Store JSON as TEXT
        status TEXT,
        command TEXT,
        indicator TEXT
    )
    """)

    for request in requests:
        request_id = str(request.request_id)
        indicator = str(request.indicator)

        response_param_json = json.dumps(request.response_param)

        cursor.execute("""
        INSERT OR REPLACE INTO requests (request_id, response_param, status, command, indicator)
        VALUES (?, ?, ?, ?, ?)
        """, (request_id, response_param_json, request.status, request.command, indicator))

    conn.commit()
    conn.close()

    return db_filename
