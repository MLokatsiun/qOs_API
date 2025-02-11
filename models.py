import enum
import hashlib
import secrets
from sqlalchemy import Column, Integer, String, Text, ForeignKey, Enum, TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID, ENUM, JSONB
import uuid
from datetime import datetime
from database import Base


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    phone_number = Column(String(15), unique=True, index=True, nullable=False)
    api_key = Column(String(64), unique=True, nullable=False)
    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)

    requests = relationship("Request", back_populates="user")
    shd_requests = relationship("Shd_Request", back_populates="user")

    def generate_api_key(self):
        unique_string = self.phone_number + secrets.token_hex(16)
        return hashlib.sha256(unique_string.encode()).hexdigest()

class RequestStatus(str, enum.Enum):

    pending = "pending"
    completed = "completed"
    failed = "failed"



class Request(Base):
    __tablename__ = 'requests'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(UUID, default=uuid.uuid4, unique=True, nullable=False)
    tg_id = Column(String(20), index=True, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    pdf_base64 = Column(String)
    status = Column(Enum(RequestStatus), nullable=False)
    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    indicator = Column(String, nullable=False)
    request_param = Column(Text)
    response_param = Column(JSONB)
    command = Column(String, nullable=False)
    batch_key = Column(String, nullable=True)
    user = relationship("User", back_populates="requests")

class Shd_Request(Base):
    __tablename__ = 'shd_requests'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(UUID, default=uuid.uuid4, unique=True, nullable=False)
    indicator = Column(String, nullable=False)
    status = Column(Enum(RequestStatus), nullable=False)
    created_at = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    request_param = Column(Text)
    response_param = Column(JSONB)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    command = Column(String, nullable=False)
    batch_key = Column(String, nullable=True)
    user = relationship("User", back_populates="shd_requests")


class ClientCredentialsModel(Base):
    __tablename__ = "client_credentials"
    id = Column(Integer, primary_key=True, index=True)
    client_name = Column(String, nullable=False)
    client_password = Column(String, nullable=False)
