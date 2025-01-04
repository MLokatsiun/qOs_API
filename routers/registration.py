from fastapi import FastAPI, HTTPException, Depends, APIRouter
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from models import User
from database import get_db, AsyncSession
from schemas import UserCreateRequest

router = APIRouter()
@router.post("/users/")
async def create_user(user_data: UserCreateRequest, db: AsyncSession = Depends(get_db)):
    phone_number = user_data.phone_number

    result = await db.execute(select(User).where(User.phone_number == phone_number))
    existing_user = result.scalars().first()
    if existing_user:
        raise HTTPException(status_code=400, detail="User with this phone number already exists")

    new_user = User(phone_number=phone_number)
    new_user.api_key = new_user.generate_api_key()  # Генерація API-ключа

    db.add(new_user)
    try:
        await db.commit()
    except IntegrityError:
        raise HTTPException(status_code=400, detail="API key or phone number must be unique")

    return {"id": new_user.id, "phone_number": new_user.phone_number, "api_key": new_user.api_key}