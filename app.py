# app.py - FastAPI server
import uuid
import json
import logging
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
import db
import models
import schemas
from datetime import datetime
from sqlalchemy import insert as sa_insert, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
import processor

app = FastAPI(title="Loan Pipeline - DB-backed API")

# logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-backend")

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

    logger.info("/apply called; application_id=%s; keys=%s", application_id, list(raw_payload.keys()))

    # Insert into applications_raw
    try:
        stmt = sa_insert(models.ApplicationsRaw).values(
            application_id=application_id,
            payload=raw_payload,
            source="api"
        )
        db_sess.execute(stmt)
        db_sess.commit()
        logger.info("Inserted raw application %s into applications_raw", application_id)
    except IntegrityError:
        db_sess.rollback()
        logger.exception("IntegrityError inserting raw application %s", application_id)
        raise HTTPException(status_code=500, detail="Failed to insert raw application")

    # Normalize and compute features immediately, then upsert into `applications`.
    try:
        logger.debug("Computing features for %s", application_id)
        features = processor.compute_features_and_decision(raw_payload)
        logger.debug("Features computed for %s: %s", application_id, features)

        insert_values = {
            "application_id": application_id,
            "full_name": raw_payload.get("full_name"),
            "dob": raw_payload.get("dob"),
            "email": raw_payload.get("email"),
            "phone": raw_payload.get("phone"),
            "annual_income": raw_payload.get("annual_income"),
            "employment_status": raw_payload.get("employment_status"),
            "months_in_current_job": raw_payload.get("months_in_current_job"),
            "requested_amount": raw_payload.get("requested_amount"),
            "loan_term_months": raw_payload.get("loan_term_months"),
            "existing_monthly_obligations": raw_payload.get("existing_monthly_obligations"),
            "num_current_loans": raw_payload.get("num_current_loans"),
            "credit_score": raw_payload.get("credit_score"),
            "bank_account_age_months": raw_payload.get("bank_account_age_months"),
            "submitted_at": raw_payload.get("submitted_at"),
            "monthly_income": features.get("monthly_income"),
            "debt_to_income": features.get("debt_to_income"),
            "age_years": features.get("age_years"),
            "employment_stability": features.get("employment_stability"),
            "decision": features.get("decision"),
            "explanation": features.get("explanation"),
            "score": raw_payload.get("score"),
            "processed_at": datetime.utcnow(),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        stmt_upsert = pg_insert(models.Applications).values(**insert_values).on_conflict_do_update(
            index_elements=["application_id"],
            set_={k: insert_values[k] for k in [
                "full_name", "dob", "email", "phone", "annual_income", "credit_score",
                "monthly_income", "debt_to_income", "decision", "explanation", "processed_at", "updated_at"
            ]}
        )
        db_sess.execute(stmt_upsert)
        logger.info("Upserted normalized application %s into applications", application_id)

        # mark the raw record as processed so processor won't re-process
        stmt_mark = update(models.ApplicationsRaw).where(models.ApplicationsRaw.application_id == application_id).values(processed=True, processed_at=datetime.utcnow())
        db_sess.execute(stmt_mark)
        db_sess.commit()
        logger.info("Marked raw application %s as processed", application_id)
    except Exception as e:
        logger.exception("Error in normalization/upsert for %s", application_id)
        # if normalization/upsert fails, rollback but keep raw stored for later processing
        db_sess.rollback()

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
