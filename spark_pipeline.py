# spark_pipeline.py - PySpark-based distributed data processing pipeline
import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from db import SessionLocal
from models import ApplicationsRaw, Applications
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Configure PySpark to use the correct Python executable on Windows
python_executable = sys.executable
os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-pipeline")


class SparkDataPipeline:
    """PySpark-based data processing pipeline for loan applications."""

    def __init__(self):
        """Initialize Spark session with proper Windows configuration."""
        logger.info(f"Initializing Spark with Python: {python_executable}")
        
        self.spark = SparkSession.builder \
            .appName("LoanApplicationPipeline") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")

    def read_raw_applications(self):
        """Read unprocessed raw applications from PostgreSQL into Spark DataFrame."""
        logger.info("Reading unprocessed applications from applications_raw...")
        
        sess = SessionLocal()
        try:
            # Query raw applications via SQLAlchemy
            stmt = select(ApplicationsRaw).where(ApplicationsRaw.processed == False).limit(100)
            rows = sess.execute(stmt).scalars().all()
            
            if not rows:
                logger.info("No unprocessed records found")
                return None
            
            logger.info(f"Read {len(rows)} unprocessed records")
            
            # Convert to list of dicts
            data = []
            for row in rows:
                payload = row.payload
                data.append({
                    "id": row.id,
                    "application_id": str(row.application_id),
                    "full_name": payload.get("full_name"),
                    "dob": payload.get("dob"),
                    "email": payload.get("email"),
                    "phone": payload.get("phone"),
                    "annual_income": float(payload.get("annual_income") or 0),
                    "employment_status": payload.get("employment_status"),
                    "months_in_current_job": int(payload.get("months_in_current_job") or 0),
                    "requested_amount": float(payload.get("requested_amount") or 0),
                    "loan_term_months": int(payload.get("loan_term_months") or 0),
                    "existing_monthly_obligations": float(payload.get("existing_monthly_obligations") or 0),
                    "num_current_loans": int(payload.get("num_current_loans") or 0),
                    "credit_score": int(payload.get("credit_score") or 0) if payload.get("credit_score") else None,
                    "bank_account_age_months": int(payload.get("bank_account_age_months") or 0),
                    "submitted_at": payload.get("submitted_at"),
                })
            
            # Create Spark DataFrame
            schema = StructType([
                StructField("id", IntegerType()),
                StructField("application_id", StringType()),
                StructField("full_name", StringType()),
                StructField("dob", StringType()),
                StructField("email", StringType()),
                StructField("phone", StringType()),
                StructField("annual_income", DoubleType()),
                StructField("employment_status", StringType()),
                StructField("months_in_current_job", IntegerType()),
                StructField("requested_amount", DoubleType()),
                StructField("loan_term_months", IntegerType()),
                StructField("existing_monthly_obligations", DoubleType()),
                StructField("num_current_loans", IntegerType()),
                StructField("credit_score", IntegerType()),
                StructField("bank_account_age_months", IntegerType()),
                StructField("submitted_at", StringType()),
            ])
            
            df = self.spark.createDataFrame(data, schema=schema)
            return df
        finally:
            sess.close()

    def compute_features(self, df):
        """Compute derived features using Spark SQL operations."""
        logger.info("Computing features using Spark...")
        
        # Compute monthly income
        df = df.withColumn(
            "monthly_income",
            F.when(F.col("annual_income") > 0, F.col("annual_income") / 12.0).otherwise(0.0)
        )
        
        # Compute debt-to-income ratio
        df = df.withColumn(
            "debt_to_income",
            F.when(
                F.col("monthly_income") > 0,
                F.col("existing_monthly_obligations") / F.col("monthly_income")
            ).otherwise(999.0)
        )
        
        # Compute age in years from DOB
        df = df.withColumn(
            "age_years",
            F.when(
                F.col("dob").isNotNull(),
                F.floor(F.datediff(F.current_date(), F.to_date(F.col("dob"))) / 365)
            ).otherwise(F.lit(None))
        )
        
        # Employment stability
        df = df.withColumn(
            "employment_stability",
            F.when(F.col("months_in_current_job") >= 12, True).otherwise(False)
        )
        
        # Credit decision logic
        df = df.withColumn(
            "decision",
            F.when(
                F.col("credit_score").isNotNull() & (F.col("credit_score") > 0),
                F.when(
                    (F.col("credit_score") >= 700) & (F.col("debt_to_income") < 0.35),
                    F.lit("APPROVE")
                ).when(
                    (F.col("credit_score") >= 620) & (F.col("debt_to_income") < 0.45),
                    F.lit("REVIEW")
                ).otherwise(F.lit("REJECT"))
            ).otherwise(
                F.when(
                    (F.col("debt_to_income") < 0.30) & (F.col("monthly_income") > 0),
                    F.lit("REVIEW")
                ).otherwise(F.lit("REJECT"))
            )
        )
        
        # Explanation
        df = df.withColumn(
            "explanation",
            F.when(
                F.col("decision") == "APPROVE",
                F.lit("High credit score and healthy DTI")
            ).when(
                F.col("decision") == "REVIEW",
                F.lit("Moderate score or no credit score; check documents")
            ).otherwise(F.lit("Low score or high DTI"))
        )
        
        return df

    def add_metadata(self, df):
        """Add processing timestamps."""
        logger.info("Adding processing metadata...")
        now = F.current_timestamp()
        df = df \
            .withColumn("processed_at", now) \
            .withColumn("created_at", now) \
            .withColumn("updated_at", now) \
            .withColumn("score", F.lit(None).cast(DoubleType()))
        return df

    def write_to_applications(self, df):
        """Write processed records to applications table."""
        logger.info("Writing processed records to applications table...")
        
        # Collect data and write via SQLAlchemy
        records = df.select([
            "id", "application_id", "full_name", "dob", "email", "phone",
            "annual_income", "employment_status", "months_in_current_job",
            "requested_amount", "loan_term_months", "existing_monthly_obligations",
            "num_current_loans", "credit_score", "bank_account_age_months",
            "submitted_at", "monthly_income", "debt_to_income", "age_years",
            "employment_stability", "decision", "explanation", "score",
            "processed_at", "created_at", "updated_at"
        ]).collect()
        
        sess = SessionLocal()
        try:
            processed_ids = []
            count = 0
            
            for row in records:
                insert_values = {
                    "application_id": row["application_id"],
                    "full_name": row["full_name"],
                    "dob": row["dob"],
                    "email": row["email"],
                    "phone": row["phone"],
                    "annual_income": row["annual_income"],
                    "employment_status": row["employment_status"],
                    "months_in_current_job": row["months_in_current_job"],
                    "requested_amount": row["requested_amount"],
                    "loan_term_months": row["loan_term_months"],
                    "existing_monthly_obligations": row["existing_monthly_obligations"],
                    "num_current_loans": row["num_current_loans"],
                    "credit_score": row["credit_score"],
                    "bank_account_age_months": row["bank_account_age_months"],
                    "submitted_at": row["submitted_at"],
                    "monthly_income": row["monthly_income"],
                    "debt_to_income": row["debt_to_income"],
                    "age_years": row["age_years"],
                    "employment_stability": row["employment_stability"],
                    "decision": row["decision"],
                    "explanation": row["explanation"],
                    "score": row["score"],
                    "processed_at": row["processed_at"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
                
                stmt = pg_insert(Applications).values(**insert_values).on_conflict_do_update(
                    index_elements=["application_id"],
                    set_={k: insert_values[k] for k in [
                        "full_name", "dob", "email", "phone", "annual_income", "credit_score",
                        "monthly_income", "debt_to_income", "age_years", "employment_stability",
                        "decision", "explanation", "processed_at", "updated_at"
                    ]}
                )
                sess.execute(stmt)
                processed_ids.append(row["id"])
                count += 1
                
                logger.info(f"  {row['application_id']}: {row['decision']} (DTI: {row['debt_to_income']:.2f})")
            
            # Mark raw records as processed
            if processed_ids:
                stmt_mark = update(ApplicationsRaw).where(
                    ApplicationsRaw.id.in_(processed_ids)
                ).values(processed=True, processed_at=datetime.utcnow())
                sess.execute(stmt_mark)
            
            sess.commit()
            logger.info(f"Successfully wrote {count} records to applications table")
        except Exception as e:
            sess.rollback()
            logger.error(f"Error writing to applications: {e}", exc_info=True)
        finally:
            sess.close()

    def run(self):
        """Execute the full pipeline."""
        try:
            logger.info("=" * 70)
            logger.info("Starting PySpark data processing pipeline")
            logger.info("=" * 70)
            
            # Read raw applications
            df_raw = self.read_raw_applications()
            if df_raw is None:
                logger.info("No records to process. Exiting.")
                return
            
            # Show raw data
            logger.info("Raw data sample:")
            df_raw.select("application_id", "full_name", "annual_income", "credit_score").show(5)
            
            # Compute features
            df_features = self.compute_features(df_raw)
            
            # Add metadata
            df_final = self.add_metadata(df_features)
            
            # Show processed data
            logger.info("Processed data sample:")
            df_final.select("application_id", "full_name", "decision", "debt_to_income", "monthly_income").show(5)
            
            # Write to database
            self.write_to_applications(df_final)
            
            logger.info("=" * 70)
            logger.info("Pipeline completed successfully")
            logger.info("=" * 70)
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
        finally:
            self.spark.stop()
            logger.info("Spark session closed")


if __name__ == "__main__":
    pipeline = SparkDataPipeline()
    pipeline.run()
