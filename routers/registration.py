from fastapi import FastAPI, HTTPException, Depends, APIRouter
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from models import User
from database import get_db, AsyncSession
from schemas import UserCreateRequest

router = APIRouter()

@router.post("/users/")
async def create_user(user_data: UserCreateRequest, db: AsyncSession = Depends(get_db)):
    """
    Створює нового користувача або повертає API-ключ, якщо користувач уже існує.

    Параметри:
    - **user_data** (UserCreateRequest): Об'єкт, що містить дані користувача, такі як номер телефону.
    - **db** (AsyncSession): Сесія для взаємодії з базою даних.

    Процес:
    1. Перевіряє, чи є користувач із таким номером телефону в базі даних.
    2. Якщо користувач існує, повертає його ID, номер телефону та API-ключ.
    3. Якщо користувача немає, створює нового користувача, генерує API-ключ та додає його в базу даних.

    Повертає:
    - **id** (int): Ідентифікатор користувача в базі даних.
    - **phone_number** (str): Номер телефону користувача.
    - **api_key** (str): API-ключ користувача.
    """
    phone_number = user_data.phone_number

    result = await db.execute(select(User).where(User.phone_number == phone_number))
    existing_user = result.scalars().first()
    if existing_user:

        return {
            "id": existing_user.id,
            "phone_number": existing_user.phone_number,
            "api_key": existing_user.api_key
        }

    new_user = User(phone_number=phone_number)
    new_user.api_key = new_user.generate_api_key()
    db.add(new_user)
    try:
        await db.commit()
        await db.refresh(new_user)
    except IntegrityError:
        raise HTTPException(status_code=400, detail="API key or phone number must be unique")

    return {
        "id": new_user.id,
        "phone_number": new_user.phone_number,
        "api_key": new_user.api_key
    }
