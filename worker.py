from celery_config import celery_app
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.future import select

from database import get_db, SyncSessionLocal
from models import Request
import uuid
import requests
import sqlite3
from decouple import config
import logging
KAFKA_BROKER = config("KAFKA_BROKER")
RESPONSE_TOPIC = config("RESPONSE_TOPIC")
TELEGRAM_BOT_TOKEN = config("TELEGRAM_BOT_TOKEN")
from reportlab.lib import colors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

try:
    pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
    pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
    print("Fonts registered successfully!")
except Exception as e:
    print(f"Error registering fonts: {e}")

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
import io
import json
import logging

logger = logging.getLogger(__name__)

def generate_pdf(request_id, response_param, command, indicator):
    buffer = io.BytesIO()

    try:
        pdfmetrics.registerFont(TTFont("DejaVuSans", "DejaVuSans.ttf"))
        pdfmetrics.registerFont(TTFont("DejaVuSans-Bold", "DejaVuSans-Bold.ttf"))
    except Exception as e:
        logger.error(f"Error registering fonts: {e}")
        raise

    pdf = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()

    header_style = ParagraphStyle(
        name="HeaderStyle",
        parent=styles["Normal"],
        fontName="DejaVuSans-Bold",
        fontSize=14,
        spaceAfter=12,
        alignment=1,
        textColor=colors.black,
    )

    bold_style = ParagraphStyle(
        name="BoldStyle",
        parent=styles["Normal"],
        fontName="DejaVuSans-Bold",
        fontSize=12,
        textColor=colors.black,
    )

    normal_style = ParagraphStyle(
        name="NormalStyle",
        parent=styles["Normal"],
        fontName="DejaVuSans",
        fontSize=12,
        textColor=colors.black,
    )

    content = []

    with SyncSessionLocal() as session:
        result = session.execute(select(Request).where(Request.request_id == request_id))
        request_record = result.scalars().first()

        if not request_record:
            logger.warning(f"Request with id {request_id} not found in the database.")
            content.append(Paragraph(f"Request {request_id} not found in the database.", header_style))
            pdf.build(content)
            buffer.seek(0)
            return buffer.read()

        request_param = request_record.request_param

    if isinstance(response_param, list) and len(response_param) == 1 and "message" in response_param[0]:
        message = response_param[0]["message"]
        content.append(Paragraph(message, bold_style))
    else:
        content.append(Paragraph(f"Response for Request Param: {request_param}", header_style))
        content.append(Spacer(1, 12))

        if not response_param:
            content.append(Paragraph("No data available in response.", normal_style))
            pdf.build(content)
            buffer.seek(0)
            return buffer.read()

        if isinstance(response_param, list):
            for idx, item in enumerate(response_param, start=1):
                if not isinstance(item, dict):
                    continue

                if "message" in item and len(item) == 1:
                    continue

                source = item.get("Источник", f"Item {idx}")
                content.append(Paragraph(f"<b>Источник:</b> {source}", bold_style))
                content.append(Spacer(1, 6))

                for key, value in item.items():
                    if key == "message" or key == "Источник":
                        continue

                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, ensure_ascii=False, indent=2)

                    content.append(Paragraph(f"<b>{key}:</b> {value}", normal_style))
                    content.append(Spacer(1, 6))

    pdf.build(content)
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
                        logger.warning(
                            f"Invalid message format or missing required fields. Ignoring the message: {message_data}")
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
                            logger.warning(
                                f"Request with id {request_id} not found in the database. Ignoring the message.")
                            continue

                        request_record.response_param = response_param
                        request_record.status = "completed"
                        session.commit()
                        logger.info(
                            f"Request {request_id} updated in the database: response_param set and status changed to 'COMPLETE'")

                        if request_record.batch_key:
                            logger.info(f"Request {request_id} has batch_key, processing as part of a batch.")

                            if request_record.batch_key not in batch_requests:
                                batch_requests[request_record.batch_key] = {}

                            if chat_id not in batch_requests[request_record.batch_key]:
                                batch_requests[request_record.batch_key][chat_id] = []

                            batch_requests[request_record.batch_key][chat_id].append(request_record)

                            result = session.execute(
                                select(Request).where(Request.batch_key == request_record.batch_key))
                            all_requests = result.scalars().all()
                            completed_requests = [r for r in all_requests if r.status == 'completed']

                            if len(all_requests) == len(completed_requests):
                                for user_chat_id, user_requests in batch_requests[request_record.batch_key].items():
                                    db_filename = generate_sqlite_db_for_user(user_requests, user_chat_id,
                                                                              request_record.batch_key)
                                    send_db_to_user(user_chat_id, db_filename)
                                    logger.info(
                                        f"Batch {request_record.batch_key} processed and sent to user {user_chat_id}")
                                batch_requests.pop(request_record.batch_key)

                        else:
                            if isinstance(response_param, list) and len(response_param) == 1 and "message" in \
                                    response_param[0]:
                                send_message_to_user(chat_id, response_param[0]["message"])
                                logger.info(
                                    f"Message sent to user {chat_id} for request_id: {request_id}. No PDF generated.")
                                continue

                            if not response_param or (isinstance(response_param, list) and len(response_param) == 0):
                                send_message_to_user(chat_id,
                                                     f"Не знайдено даних за вашим запитом (request_id: {request_id}).")
                                logger.info(
                                    f"No data found for request_id: {request_id}. Sent message to user {chat_id}.")
                                continue
                            pdf_content = generate_pdf(request_id, response_param, command, indicator)
                            send_pdf_to_user(chat_id, pdf_content, f"response_{request_id}.pdf")
                            logger.info(f"PDF generated and sent to user {chat_id} for request_id: {request_id}")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode Kafka message: {msg.value()} - Error: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error while processing message: {msg.value()} - Error: {e}")

    finally:
        consumer.close()


def send_message_to_user(chat_id, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {
        'chat_id': chat_id,
        'text': message
    }
    response = requests.post(url, params=params)
    return response.json()


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
        id TEXT PRIMARY KEY
        request_id TEXT,
        response_param TEXT,
        status TEXT,
        command TEXT,
        indicator TEXT,
        request_param TEXT  
    )
    """)

    for request in requests:
        request_id = str(request.request_id)
        indicator = str(request.indicator)

        with SyncSessionLocal() as session:
            result = session.execute(select(Request).where(Request.request_id == request_id))
            request_record = result.scalars().first()

            if request_record:
                request_param = request_record.request_param
            else:
                request_param = None

        response_param_json = json.dumps(request.response_param)

        cursor.execute("""
        INSERT OR REPLACE INTO requests (request_id, response_param, status, command, indicator, request_param)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (request_id, response_param_json, request.status, request.command, indicator, request_param))

    conn.commit()
    conn.close()

    return db_filename