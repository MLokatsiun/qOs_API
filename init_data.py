from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from passlib.context import CryptContext
from models import ClientCredentialsModel
from decouple import config
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

CLIENT_PASSWORD = config("CLIENT_PASSWORD")
CLIENT_NAME = config("CLIENT_NAME")

async def create_client(db: AsyncSession):

    result = await db.execute(
        select(ClientCredentialsModel).filter(ClientCredentialsModel.client_name == CLIENT_NAME)
    )
    existing_credentials = result.scalars().first()

    if not existing_credentials:
        password = pwd_context.hash(CLIENT_PASSWORD)
        credentials = ClientCredentialsModel(
            client_name=CLIENT_NAME,
            client_password=password
        )
        db.add(credentials)
        await db.commit()