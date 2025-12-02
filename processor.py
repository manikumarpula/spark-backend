# processor.py - simple processor that reads unprocessed raw rows, computes features/decision, upserts to applications
import os
from db import SessionLocal, engine
from models import ApplicationsRaw, Applications, Base
from sqlalchemy import select, update, insert
from datetime import datetime, date
from decimal import Decimal
import math
import json

# Ensure models are known
Base.metadata.create_all(bind=engine)

def compute_features_and_decision(payload: dict):
    # Basic transforms / defaults
    annual_income = Decimal(payload.get("annual_income") or 0)
    existing_monthly_obligations = Decimal(payload.get("existing_monthly_obligations") or 0)
    credit_score = payload.get("credit_score")
    dob = payload.get("dob")
    months_in_current_job = payload.get("months_in_current_job") or 0

    monthly_income = (annual_income / Decimal(12)) if annual_income else Decimal(0)
    dti = (existing_monthly_obligations / (monthly_income + Decimal("1e-9"))) if monthly_income else Decimal(999)
    # age in years
    try:
        age_years = (date.today() - date.fromisoformat(dob)).days // 365
    except Exception:
        age_years = None

    employment_stability = months_in_current_job >= 12

    # Decision rules (same as earlier)
    decision = "REJECT"
    explanation = "Low score or high DTI"
    try:
        if credit_score is not None:
            if credit_score >= 700 and float(dti) < 0.35:
                decision = "APPROVE"
                explanation = "High credit score and healthy DTI"
            elif credit_score >= 620 and float(dti) < 0.45:
                decision = "REVIEW"
                explanation = "Moderate score; check documents"
            else:
                decision = "REJECT"
                explanation = "Low score or high DTI"
        else:
            # no credit score — conservative
            if float(dti) < 0.30 and monthly_income > 0:
                decision = "REVIEW"
                explanation = "No credit score; low DTI"
            else:
                decision = "REJECT"
                explanation = "Missing credit score and/or high DTI"
    except Exception:
        decision = "REJECT"
        explanation = "Error occurred during scoring"

    features = {
        "monthly_income": float(monthly_income),
        "debt_to_income": float(dti),
        "age_years": age_years,
        "employment_stability": employment_stability,
        "decision": decision,
        "explanation": explanation
    }
    return features

def process_once():
    sess = SessionLocal()
    try:
        # fetch unprocessed raw rows (limit to small batch)
        stmt = select(ApplicationsRaw).where(ApplicationsRaw.processed == False).limit(20)
        rows = sess.execute(stmt).scalars().all()
        if not rows:
            print("No unprocessed rows.")
            return

        for raw in rows:
            payload = raw.payload
            app_id = str(raw.application_id)
            features = compute_features_and_decision(payload)

            # prepare upsert into applications
            insert_values = {
                "application_id": app_id,
                "full_name": payload.get("full_name"),
                "dob": payload.get("dob"),
                "email": payload.get("email"),
                "phone": payload.get("phone"),
                "annual_income": payload.get("annual_income"),
                "employment_status": payload.get("employment_status"),
                "months_in_current_job": payload.get("months_in_current_job"),
                "requested_amount": payload.get("requested_amount"),
                "loan_term_months": payload.get("loan_term_months"),
                "existing_monthly_obligations": payload.get("existing_monthly_obligations"),
                "num_current_loans": payload.get("num_current_loans"),
                "credit_score": payload.get("credit_score"),
                "bank_account_age_months": payload.get("bank_account_age_months"),
                "submitted_at": payload.get("submitted_at"),
                "monthly_income": features["monthly_income"],
                "debt_to_income": features["debt_to_income"],
                "age_years": features["age_years"],
                "employment_stability": features["employment_stability"],
                "decision": features["decision"],
                "explanation": features["explanation"],
                "processed_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }

            # Upsert using INSERT ... ON CONFLICT
            stmt_upsert = insert(Applications).values(**insert_values).on_conflict_do_update(
                index_elements=["application_id"],
                set_={
                    "full_name": insert_values["full_name"],
                    "dob": insert_values["dob"],
                    "email": insert_values["email"],
                    "phone": insert_values["phone"],
                    "annual_income": insert_values["annual_income"],
                    "credit_score": insert_values["credit_score"],
                    "monthly_income": insert_values["monthly_income"],
                    "debt_to_income": insert_values["debt_to_income"],
                    "decision": insert_values["decision"],
                    "explanation": insert_values["explanation"],
                    "processed_at": insert_values["processed_at"],
                    "updated_at": insert_values["updated_at"]
                }
            )
            sess.execute(stmt_upsert)

            # mark raw as processed
            stmt_marker = update(ApplicationsRaw).where(ApplicationsRaw.id == raw.id).values(processed=True, processed_at=datetime.utcnow())
            sess.execute(stmt_marker)

        sess.commit()
        print(f"Processed {len(rows)} rows.")
    except Exception as e:
        sess.rollback()
        print("Error in processing:", str(e))
    finally:
        sess.close()

if __name__ == "__main__":
    process_once()
