from fastapi import FastAPI, HTTPException, Depends, APIRouter, Header
from passlib.context import CryptContext
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from models import User
from database import get_db, AsyncSession
from schemas import UserCreateRequest
from models import ClientCredentialsModel
router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

@router.post("/users/")
async def create_user(
    user_data: UserCreateRequest,
    client_name: str = Header(...),
    client_password: str = Header(...),
    db: AsyncSession = Depends(get_db)
):
    credentials_query = select(ClientCredentialsModel).where(
        ClientCredentialsModel.client_name == client_name
    )
    result = await db.execute(credentials_query)
    credentials = result.scalars().first()

    if not credentials or not pwd_context.verify(client_password, credentials.client_password):
        raise HTTPException(status_code=401, detail="Invalid client credentials")

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
