import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine


# Ambil path folder ml-service
ROOT_DIR = Path(__file__).resolve().parent

# Check if env file is overridden by ML_ENV_FILE (e.g. in Airflow)
env_file = os.getenv("ML_ENV_FILE")
if env_file:
    env_path = Path(env_file)
    if not env_path.is_absolute():
        env_path = ROOT_DIR / env_file
    load_dotenv(env_path, override=True)
else:
    load_dotenv(ROOT_DIR / ".env", override=True)


def get_engine():
    """
    Membuat koneksi SQLAlchemy ke MySQL untuk ML service.
    Credential dibaca dari file .env di folder ml-service.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "tiktok_oltp")
    db_user = os.getenv("DB_USER", "ml_service_user")
    db_password_raw = os.getenv("DB_PASSWORD", "")

    if not db_password_raw:
        print("[DB WARNING] DB_PASSWORD is empty. Please check ml-service/.env")

    print(
        f"[DB] Connecting to MySQL: "
        f"user={db_user}, host={db_host}, port={db_port}, database={db_name}"
    )

    # Encode password agar aman kalau ada karakter seperti @, #, !, atau %
    db_password = quote_plus(db_password_raw)

    database_url = (
        f"mysql+pymysql://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    )

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=3600,
    )