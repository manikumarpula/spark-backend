# models.py - SQLAlchemy models mirroring your migrations
from sqlalchemy import Column, Integer, String, Date, Text, Numeric, Boolean, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
import datetime
import uuid

Base = declarative_base()

class ApplicationsRaw(Base):
    __tablename__ = "applications_raw"
    id = Column(Integer, primary_key=True)
    application_id = Column(UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    payload = Column(JSONB, nullable=False)
    received_at = Column(TIMESTAMP(timezone=True), nullable=False, default=datetime.datetime.utcnow)
    source = Column(String(100), default="api")
    processed = Column(Boolean, nullable=False, default=False)
    processed_at = Column(TIMESTAMP(timezone=True), nullable=True)

class Applications(Base):
    __tablename__ = "applications"
    id = Column(Integer, primary_key=True)
    application_id = Column(UUID(as_uuid=True), unique=True, nullable=False)
    full_name = Column(Text)
    dob = Column(Date)
    email = Column(Text)
    phone = Column(Text)
    annual_income = Column(Numeric(12,2))
    employment_status = Column(Text)
    months_in_current_job = Column(Integer)
    requested_amount = Column(Numeric(12,2))
    loan_term_months = Column(Integer)
    existing_monthly_obligations = Column(Numeric(12,2))
    num_current_loans = Column(Integer)
    credit_score = Column(Integer)
    bank_account_age_months = Column(Integer)
    submitted_at = Column(TIMESTAMP(timezone=True))
    monthly_income = Column(Numeric(12,2))
    debt_to_income = Column(Numeric(8,6))
    age_years = Column(Integer)
    employment_stability = Column(Boolean)
    decision = Column(Text)
    explanation = Column(Text)
    score = Column(Numeric(6,4))
    processed_at = Column(TIMESTAMP(timezone=True), default=datetime.datetime.utcnow)
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.datetime.utcnow)
    updated_at = Column(TIMESTAMP(timezone=True), default=datetime.datetime.utcnow)
