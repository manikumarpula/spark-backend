# app.py - FastAPI server
import uuid
import json
import logging
import threading
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
import db
import models
import schemas
from datetime import datetime
from sqlalchemy import insert as sa_insert
from sqlalchemy.exc import IntegrityError
from apscheduler.schedulers.background import BackgroundScheduler
from spark_pipeline import SparkDataPipeline

app = FastAPI(title="Loan Pipeline - Spark-backed API")

# logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-backend")

# Initialize scheduler for Spark pipeline (every 1 minute)
scheduler = BackgroundScheduler()

def run_spark_pipeline():
    """Run Spark pipeline in background thread."""
    try:
        logger.info("[SCHEDULER] Triggering Spark pipeline...")
        pipeline = SparkDataPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"[SCHEDULER] Spark pipeline error: {e}", exc_info=True)

@app.on_event("startup")
def startup_event():
    """Start the scheduler when FastAPI starts."""
    scheduler.add_job(run_spark_pipeline, 'interval', minutes=1, id='spark_pipeline_job')
    scheduler.start()
    logger.info("[STARTUP] Spark pipeline scheduler started (runs every 1 minute)")

@app.on_event("shutdown")
def shutdown_event():
    """Shutdown scheduler when FastAPI stops."""
    if scheduler.running:
        scheduler.shutdown()
    logger.info("[SHUTDOWN] Spark pipeline scheduler stopped")

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

    # Insert into applications_raw only (Spark pipeline will process it)
    try:
        stmt = sa_insert(models.ApplicationsRaw).values(
            application_id=application_id,
            payload=raw_payload,
            source="api",
            processed=False
        )
        db_sess.execute(stmt)
        db_sess.commit()
        logger.info("Inserted raw application %s into applications_raw (awaiting Spark pipeline)", application_id)
    except IntegrityError:
        db_sess.rollback()
        logger.exception("IntegrityError inserting raw application %s", application_id)
        raise HTTPException(status_code=500, detail="Failed to insert raw application")
    except Exception as e:
        db_sess.rollback()
        logger.exception("Error inserting raw application %s", application_id)
        raise HTTPException(status_code=500, detail="Error inserting application")

    return {"application_id": application_id, "status": "SUBMITTED", "message": "Application queued for Spark processing"}

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
