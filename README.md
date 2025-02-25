# Telegram Bot with FastAPI and Celery

## Description
This project is a Telegram bot that uses FastAPI and Celery for user registration and PDF search.

## Technology Stack
- **FastAPI** – for creating the web service.
- **Celery** – for background task processing.
- **PostgreSQL** – for data storage.
- **SQLAlchemy & Alembic** – for database management.
- **Docker & Docker Compose** – for containerization and simplified deployment.

## Installation and Setup
### 1. Clone the Repository
```bash
git clone https://github.com/MLokatsiun/qOs_API.git
cd your-repo
```

### 2. Create a Virtual Environment and Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # For Linux/macOS
venv\Scripts\activate    # For Windows
pip install -r requirements.txt
```

### 3. Database Setup
Apply Alembic migrations:
```bash
alembic upgrade head
```

### 4. Run with Docker
```bash
docker-compose up --build
```

### 5. Run Manually
Start FastAPI:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Start Celery:
```bash
celery -A worker worker --loglevel=info
```

## Project Structure
```
/
├── alembic/              # Database migrations
├── routers/              # API routes
│   ├── registration.py   # User registration
│   ├── requests_tg.py    # Telegram request processing
│   ├── shodan_tg.py      # Interaction with Shodan API
├── database.py           # Database connection
├── models.py             # Database models
├── schemas.py            # Pydantic schemas
├── worker.py             # Celery task processing
├── init_data.py          # Data initialization
├── main.py               # Main application file
├── requirements.txt      # Dependencies
├── docker-compose.yml    # Docker configuration
├── Dockerfile.api        # Dockerfile for API
├── Dockerfile.celery     # Dockerfile for Celery
└── .gitignore            # Git ignored files
```

## API Endpoints
| Method | Endpoint           | Description              |
|--------|-------------------|--------------------------|
| POST   | /registration     | User registration        |
| POST   | /tg_request       | Telegram request         |
| POST   | /request          | Shodan request           |

## Authors
- **MLokatsiun** – [GitHub](https://github.com/MLokatsiun)

## License
This project is distributed under the MIT License.

