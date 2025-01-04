import base64
import uuid
import json
from confluent_kafka import Consumer, Producer
from fpdf import FPDF
from decouple import config
import time

KAFKA_BROKER = config("KAFKA_BROKER")
REQUEST_TOPIC = config("REQUEST_TOPIC")
RESPONSE_TOPIC = config("RESPONSE_TOPIC")

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': f'pdf_response_group_{uuid.uuid4()}',
    'auto.offset.reset': 'earliest',
})
consumer.subscribe([REQUEST_TOPIC])

def generate_test_pdf():
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="texxxxxxxxxxxxxxxxxxxxxxxxxxxxtttttttttttttttttttttttttt.", ln=True)

    pdf_output = pdf.output(dest='S').encode('latin1')  # Сохраняем в память

    pdf_base64 = base64.b64encode(pdf_output).decode('utf-8')
    return pdf_base64

def send_pdf_response(request_id, tg_id, pdf_base64):
    response_message = {
        "request_id": request_id,
        "tg_id": tg_id,
        "pdf_base64": pdf_base64
    }
    producer.produce(RESPONSE_TOPIC, key=request_id, value=json.dumps(response_message).encode('utf-8'))
    producer.flush()
    print(f"Response sent to Kafka: {response_message}")

def consume_requests():
    try:
        while True:
            msg = consumer.poll(timeout=60)
            if msg is None:
                print("Waiting for messages...")
                time.sleep(1)
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            request_data = json.loads(msg.value().decode('utf-8'))
            request_id = request_data["request_id"]
            tg_id = request_data["tg_id"]

            pdf_base64 = generate_test_pdf()

            send_pdf_response(request_id, tg_id, pdf_base64)

    except KeyboardInterrupt:
        print("Service stopped.")
    finally:
        print("Closing Kafka Consumer...")
        consumer.close()
        print("Kafka Consumer closed.")

if __name__ == "__main__":
    consume_requests()
