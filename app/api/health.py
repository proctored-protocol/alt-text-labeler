from fastapi import APIRouter
from sqlalchemy import text

from app.db import SessionLocal

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    with SessionLocal() as session:
        session.execute(text("SELECT 1"))
    return {"status": "ok"}


@router.get("/version")
def version() -> dict[str, str]:
    return {"version": "0.1.0"}