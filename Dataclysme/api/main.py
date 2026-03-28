import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


API_TITLE = "Dataclysme Datamart API"
API_VERSION = "1.0.0"

JWT_SECRET = os.getenv("API_JWT_SECRET", "change-me-please")
JWT_ALGORITHM = os.getenv("API_JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("API_TOKEN_EXPIRE_MINUTES", "60"))

API_USER = os.getenv("API_USER", "admin")
API_PASSWORD = os.getenv("API_PASSWORD", "admin123")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://root:my-secret-pw@mysql:3306/dataclysme",
)

ALLOWED_DATAMARTS: Dict[str, str] = {
    "risks": "dm_risks",
    "tourism": "dm_tourism",
    "agriculture": "dm_agriculture",
}

security = HTTPBearer()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title=API_TITLE, version=API_VERSION)


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_minutes: int


class PaginationMeta(BaseModel):
    page: int
    page_size: int
    total_rows: int
    total_pages: int


class DatamartResponse(BaseModel):
    datamart: str
    pagination: PaginationMeta
    data: List[Dict[str, Any]]


def _create_access_token(subject: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=JWT_EXPIRE_MINUTES)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
            )
        return username
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from exc


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": API_TITLE}


@app.post("/auth/login", response_model=TokenResponse)
def login(payload: LoginRequest) -> TokenResponse:
    if payload.username != API_USER or payload.password != API_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bad credentials",
        )

    token = _create_access_token(payload.username)
    return TokenResponse(
        access_token=token,
        expires_in_minutes=JWT_EXPIRE_MINUTES,
    )


@app.get("/api/v1/datamarts", response_model=List[str])
def list_datamarts(_: str = Depends(_verify_token)) -> List[str]:
    return sorted(ALLOWED_DATAMARTS.keys())


@app.get("/api/v1/datamarts/{datamart_name}", response_model=DatamartResponse)
def get_datamart(
    datamart_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    _: str = Depends(_verify_token),
) -> DatamartResponse:
    if datamart_name not in ALLOWED_DATAMARTS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Unknown datamart",
        )

    table_name = ALLOWED_DATAMARTS[datamart_name]
    offset = (page - 1) * page_size

    try:
        with engine.connect() as conn:
            count_stmt = text(f"SELECT COUNT(*) AS total_rows FROM {table_name}")
            total_rows = int(conn.execute(count_stmt).scalar_one())

            if offset >= total_rows and total_rows > 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Requested page exceeds available data",
                )

            query_stmt = text(
                f"SELECT * FROM {table_name} LIMIT :limit_value OFFSET :offset_value"
            )
            rows = conn.execute(
                query_stmt,
                {"limit_value": page_size, "offset_value": offset},
            ).mappings().all()

    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error while reading datamart",
        ) from exc

    total_pages = (total_rows + page_size - 1) // page_size if total_rows > 0 else 0

    return DatamartResponse(
        datamart=datamart_name,
        pagination=PaginationMeta(
            page=page,
            page_size=page_size,
            total_rows=total_rows,
            total_pages=total_pages,
        ),
        data=[dict(row) for row in rows],
    )
