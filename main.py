import asyncio
import json
from celery_config import celery_app
from fastapi import FastAPI

from database import SyncSessionLocal, get_db
from init_data import create_client
from routers.registration import router as registration_router
from routers.requests_tg import router as tg_request_router
from routers.shodan_tg import router as shd_router
from fastapi.middleware.cors import CORSMiddleware
from worker import consume_responses, consume_shodan_responses

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(registration_router, prefix="/registration", tags=["Registration"])
app.include_router(tg_request_router, prefix="/tg_request", tags=["TG Request"])
app.include_router(shd_router, prefix="/request", tags=["SHD"])

consumer_started = False


@app.on_event("startup")
async def startup_event():
    async for db in get_db():
        await create_client(db)

@app.on_event("startup")
async def start_consumer_on_startup():
    global consumer_started
    if not consumer_started:
        asyncio.create_task(start_celery_consumer())
        consumer_started = True



async def start_celery_consumer():
    """Функція для запуску Celery воркера лише один раз."""
    consume_shodan_responses.delay()
    consume_responses.delay()
    print("Celery consumer запущений")