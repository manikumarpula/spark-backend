# app.py - FastAPI server
import uuid
import json
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
import db
import models
import schemas
from datetime import datetime
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError

app = FastAPI(title="Loan Pipeline - DB-backed API")

# Dependency
def get_db():
    db_sess = db.SessionLocal()
    try:
        yield db_sess
    finally:
        db_sess.close()

@app.post("/apply")
def apply(payload: schemas.ApplicationIn, db_sess: Session = Depends(get_db)):
    application_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    raw_payload = payload.dict()
    raw_payload.update({"submitted_at": now})

    # Insert into applications_raw
    try:
        stmt = insert(models.ApplicationsRaw).values(
            application_id=application_id,
            payload=raw_payload,
            source="api"
        )
        db_sess.execute(stmt)
        db_sess.commit()
    except IntegrityError:
        db_sess.rollback()
        raise HTTPException(status_code=500, detail="Failed to insert raw application")

    return {"application_id": application_id, "status": "SUBMITTED"}

@app.get("/status/{application_id}")
def status(application_id: str, db_sess: Session = Depends(get_db)):
    # Look up processed record in applications
    row = db_sess.execute(
        models.Applications.__table__.select().where(models.Applications.application_id == application_id)
    ).first()
    if row:
        # convert Row to dict
        record = dict(row._mapping)
        return {"status": "COMPLETED", "result": {k: str(v) for k, v in record.items()}}
    # else check raw existence
    raw = db_sess.execute(
        models.ApplicationsRaw.__table__.select().where(models.ApplicationsRaw.application_id == application_id)
    ).first()
    if raw:
        return {"status": "PENDING"}
    raise HTTPException(status_code=404, detail="application_id not found")

@app.get("/health")
def health():
    return {"ok": True}
