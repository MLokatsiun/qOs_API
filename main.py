from fastapi import FastAPI
from routers.registration import router as registration_router
from routers.requests_tg import router as tg_request_router
from fastapi.middleware.cors import CORSMiddleware
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
