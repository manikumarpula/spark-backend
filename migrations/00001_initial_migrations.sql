-- 001_create_tables.sql

-- Raw applications (raw JSON payload + metadata)
CREATE TABLE IF NOT EXISTS applications_raw (
  id SERIAL PRIMARY KEY,
  application_id UUID NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source VARCHAR(100) DEFAULT 'api',
  processed BOOLEAN NOT NULL DEFAULT FALSE,
  processed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_applications_raw_received_at ON applications_raw (received_at);
CREATE INDEX IF NOT EXISTS idx_applications_raw_processed ON applications_raw (processed);


-- Processed applications (result / canonical)
CREATE TABLE IF NOT EXISTS applications (
  id SERIAL PRIMARY KEY,
  application_id UUID NOT NULL UNIQUE,
  full_name TEXT,
  dob DATE,
  email TEXT,
  phone TEXT,
  annual_income NUMERIC(12,2),
  employment_status TEXT,
  months_in_current_job INT,
  requested_amount NUMERIC(12,2),
  loan_term_months INT,
  existing_monthly_obligations NUMERIC(12,2),
  num_current_loans INT,
  credit_score INT,
  bank_account_age_months INT,
  submitted_at TIMESTAMPTZ,
  monthly_income NUMERIC(12,2),
  debt_to_income NUMERIC(8,6),
  age_years INT,
  employment_stability BOOLEAN,
  decision TEXT,            -- APPROVE / REVIEW / REJECT
  explanation TEXT,
  score NUMERIC(6,4),      -- optional probability/score if needed later
  processed_at TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_applications_application_id ON applications (application_id);
CREATE INDEX IF NOT EXISTS idx_applications_decision ON applications (decision);


-- Application audit timeline
CREATE TABLE IF NOT EXISTS application_events (
  id SERIAL PRIMARY KEY,
  application_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  event_payload JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_application_events_appid ON application_events (application_id);
