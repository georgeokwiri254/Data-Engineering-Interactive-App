
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random

# Extracted from app.py - init_module3_database
def init_module3_database():
    """Initialize Module 3 SQLite database for ETL/ELT pipelines and staging data"""
    conn = sqlite3.connect('module3_etl_pipelines.db', check_same_thread=False)
    cursor = conn.cursor()
    
    # Set SQLite optimizations for ETL workloads
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA cache_size = -64000")  # 64MB cache
    cursor.execute("PRAGMA temp_store = memory")
    
    # Create staging tables for each company (Module 3 - Cleansed data)
    staging_tables = [
        """
        CREATE TABLE IF NOT EXISTS staging_uber_rides (
            ride_id TEXT PRIMARY KEY,
            driver_id TEXT,
            rider_id TEXT,
            pickup_ts TEXT,
            dropoff_ts TEXT,
            pickup_coord TEXT,  -- JSON: {"lat": float, "lng": float}
            dropoff_coord TEXT, -- JSON: {"lat": float, "lng": float}
            distance_km REAL,
            fare_aed REAL,
            fare_base REAL,
            fare_taxes REAL,
            status TEXT,
            ingest_latency_ms INTEGER,
            etl_batch_id TEXT,
            processed_ts TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS staging_netflix_events (
            event_id TEXT PRIMARY KEY,
            user_id TEXT,
            content_id TEXT,
            genre TEXT,
            device TEXT,
            event_ts TEXT,
            playback_sec INTEGER,
            country TEXT,
            session_id TEXT,
            video_quality TEXT,
            etl_batch_id TEXT,
            processed_ts TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS staging_amazon_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_ts TEXT,
            items_count INTEGER,
            subtotal_aed REAL,
            shipping_aed REAL,
            tax_aed REAL,
            total_aed REAL,
            fulfillment_center TEXT,
            order_channel TEXT,
            etl_batch_id TEXT,
            processed_ts TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS staging_airbnb_reservations (
            booking_id TEXT PRIMARY KEY,
            host_id TEXT,
            guest_id TEXT,
            property_id TEXT,
            checkin_date TEXT,
            checkout_date TEXT,
            nights INTEGER,
            price_aed REAL,
            status TEXT,
            property_type TEXT,
            city TEXT,
            etl_batch_id TEXT,
            processed_ts TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS staging_nyse_trades (
            tick_id TEXT PRIMARY KEY,
            ticker TEXT,
            timestamp_ms TEXT,
            price REAL,
            size INTEGER,
            venue TEXT,
            is_auction INTEGER,  -- 0/1 boolean
            trade_type TEXT,
            etl_batch_id TEXT,
            processed_ts TEXT
        )
        """
    ]
    
    # Create ETL processing jobs metadata table (Module 6 - Job tracking)
    processing_jobs_table = """
        CREATE TABLE IF NOT EXISTS processing_jobs (
            job_id TEXT PRIMARY KEY,
            company TEXT,
            job_name TEXT,
            job_type TEXT,  -- batch/stream/micro-batch
            engine TEXT,    -- spark/flink/airflow/dbt
            input_path TEXT,
            output_path TEXT,
            records_in INTEGER,
            records_out INTEGER,
            start_ts TEXT,
            end_ts TEXT,
            duration_ms INTEGER,
            status TEXT,    -- running/completed/failed/cancelled
            error_msg TEXT,
            resource_cpu_cores INTEGER,
            resource_memory_gb INTEGER,
            data_quality_score REAL,
            batch_id TEXT
        )
    """
    
    # Create ETL manifests table for dataset lineage
    manifests_table = """
        CREATE TABLE IF NOT EXISTS etl_manifests (
            manifest_id TEXT PRIMARY KEY,
            dataset_name TEXT,
            schema_version TEXT,
            row_count INTEGER,
            size_bytes INTEGER,
            created_by TEXT,
            created_ts TEXT,
            source_dataset TEXT,
            transformation_logic TEXT,
            data_quality_checks TEXT,  -- JSON
            partition_info TEXT        -- JSON
        )
    """
    
    # Execute table creation
    tables = staging_tables + [processing_jobs_table, manifests_table]
    for table_sql in tables:
        cursor.execute(table_sql)
    
    # Create indexes for ETL performance
    indexes = [
        # Staging table indexes
        "CREATE INDEX IF NOT EXISTS idx_uber_rides_processed_ts ON staging_uber_rides(processed_ts)",
        "CREATE INDEX IF NOT EXISTS idx_uber_rides_etl_batch ON staging_uber_rides(etl_batch_id)",
        "CREATE INDEX IF NOT EXISTS idx_netflix_events_processed_ts ON staging_netflix_events(processed_ts)",
        "CREATE INDEX IF NOT EXISTS idx_netflix_events_user ON staging_netflix_events(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_amazon_orders_processed_ts ON staging_amazon_orders(processed_ts)",
        "CREATE INDEX IF NOT EXISTS idx_amazon_orders_customer ON staging_amazon_orders(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_airbnb_reservations_processed_ts ON staging_airbnb_reservations(processed_ts)",
        "CREATE INDEX IF NOT EXISTS idx_airbnb_reservations_city ON staging_airbnb_reservations(city)",
        "CREATE INDEX IF NOT EXISTS idx_nyse_trades_processed_ts ON staging_nyse_trades(processed_ts)",
        "CREATE INDEX IF NOT EXISTS idx_nyse_trades_ticker ON staging_nyse_trades(ticker)",
        
        # Processing jobs indexes
        "CREATE INDEX IF NOT EXISTS idx_jobs_company ON processing_jobs(company)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_start_ts ON processing_jobs(start_ts)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_status ON processing_jobs(status)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_engine ON processing_jobs(engine)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_batch_id ON processing_jobs(batch_id)",
        
        # Manifests indexes
        "CREATE INDEX IF NOT EXISTS idx_manifests_dataset ON etl_manifests(dataset_name)",
        "CREATE INDEX IF NOT EXISTS idx_manifests_created_ts ON etl_manifests(created_ts)"
    ]
    
    for index in indexes:
        cursor.execute(index)
    
    conn.commit()
    return conn

# Extracted from app.py - generate_uber_etl_jobs
def generate_uber_etl_jobs(n_jobs=200):
    """Generate Uber ETL job execution data"""
    np.random.seed(43)
    
    data = []
    job_types = ['batch', 'stream', 'micro-batch']
    engines = ['spark', 'flink', 'airflow', 'kafka-streams']
    statuses = ['completed', 'failed', 'running', 'cancelled']
    job_names = [
        'rides-raw-to-staging', 'driver-location-stream', 'fare-calculation-batch',
        'surge-pricing-realtime', 'trip-analytics-daily', 'payment-reconciliation',
        'driver-performance-etl', 'rider-churn-prediction', 'fraud-detection-stream',
        'geo-analytics-batch', 'demand-forecasting', 'earnings-summary-etl'
    ]
    
    for i in range(n_jobs):
        start_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        # Realistic duration based on job type and engine
        if np.random.choice(['spark', 'flink']) in engines:
            duration_ms = int(np.random.lognormal(mean=8, sigma=1.2) * 1000)  # 3min to 2hrs typical
        else:
            duration_ms = int(np.random.lognormal(mean=6, sigma=1.5) * 1000)  # 30sec to 45min typical
        
        end_time = start_time + timedelta(milliseconds=duration_ms)
        
        records_in = int(np.random.lognormal(mean=10, sigma=2))  # 10K to 10M records
        # Simulate data quality issues and processing efficiency
        efficiency = np.random.beta(a=8, b=2)  # Most jobs are efficient
        records_out = int(records_in * efficiency)
        
        status = np.random.choice(statuses, p=[0.85, 0.10, 0.03, 0.02])
        
        data.append({
            'job_id': f"uber_job_{i:06d}_{start_time.strftime('%Y%m%d_%H%M%S')}",
            'company': 'Uber',
            'job_name': np.random.choice(job_names),
            'job_type': np.random.choice(job_types, p=[0.6, 0.3, 0.1]),
            'engine': np.random.choice(engines, p=[0.4, 0.25, 0.25, 0.1]),
            'input_path': f"s3://uber-data-lake/raw/rides/{start_time.strftime('%Y/%m/%d')}",
            'output_path': f"s3://uber-data-lake/staging/rides/{start_time.strftime('%Y/%m/%d')}",
            'records_in': records_in,
            'records_out': records_out if status == 'completed' else 0,
            'start_ts': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_ts': end_time.strftime('%Y-%m-%d %H:%M:%S') if status in ['completed', 'failed'] else None,
            'duration_ms': duration_ms if status in ['completed', 'failed'] else None,
            'status': status,
            'error_msg': 'OutOfMemoryError: Java heap space' if status == 'failed' else None,
            'resource_cpu_cores': np.random.choice([4, 8, 16, 32]),
            'resource_memory_gb': np.random.choice([16, 32, 64, 128]),
            'data_quality_score': np.random.beta(a=9, b=1) * 100 if status == 'completed' else None,
            'batch_id': f"batch_{start_time.strftime('%Y%m%d_%H')}"
        })
    
    return pd.DataFrame(data)

# Extracted from app.py - generate_uber_staging_data
def generate_uber_staging_data(n_records=8000):
    """Generate Uber staging (cleansed) ride data"""
    np.random.seed(43)
    
    data = []
    statuses = ['completed', 'cancelled', 'ongoing']
    cities = ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Fujairah']
    
    for i in range(n_records):
        pickup_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        trip_duration = np.random.gamma(shape=2, scale=15)  # Average 30min trip
        dropoff_time = pickup_time + timedelta(minutes=trip_duration)
        
        # Dubai coordinates (realistic)
        pickup_lat = 25.1972 + np.random.normal(0, 0.1)
        pickup_lng = 55.2744 + np.random.normal(0, 0.1)
        dropoff_lat = pickup_lat + np.random.normal(0, 0.05)
        dropoff_lng = pickup_lng + np.random.normal(0, 0.05)
        
        distance = np.random.lognormal(mean=2, sigma=1)  # 3-50km typical
        base_fare = max(10, distance * np.random.uniform(2, 4))  # AED 2-4 per km
        taxes = base_fare * 0.05  # 5% tax
        total_fare = base_fare + taxes
        
        data.append({
            'ride_id': f"uber_ride_{i:08d}",
            'driver_id': f"driver_{np.random.randint(1000, 9999)}",
            'rider_id': f"rider_{np.random.randint(10000, 99999)}",
            'pickup_ts': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
            'dropoff_ts': dropoff_time.strftime('%Y-%m-%d %H:%M:%S'),
            'pickup_coord': json.dumps({"lat": round(pickup_lat, 6), "lng": round(pickup_lng, 6)}),
            'dropoff_coord': json.dumps({"lat": round(dropoff_lat, 6), "lng": round(dropoff_lng, 6)}),
            'distance_km': round(distance, 2),
            'fare_aed': round(total_fare, 2),
            'fare_base': round(base_fare, 2),
            'fare_taxes': round(taxes, 2),
            'status': np.random.choice(statuses, p=[0.85, 0.12, 0.03]),
            'ingest_latency_ms': np.random.randint(100, 5000),
            'etl_batch_id': f"etl_batch_{pickup_time.strftime('%Y%m%d_%H')}",
            'processed_ts': (pickup_time + timedelta(minutes=np.random.randint(5, 30))).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return pd.DataFrame(data)

# Extracted from app.py - generate_uber_etl_manifests
def generate_uber_etl_manifests(n_manifests=15):
    """Generate Uber ETL manifest data for dataset lineage"""
    np.random.seed(43)
    
    data = []
    datasets = ['raw_rides', 'staging_rides', 'aggregated_rides', 'driver_metrics', 'surge_data']
    
    for i in range(n_manifests):
        created_time = datetime.now() - timedelta(days=np.random.randint(0, 30))
        
        data.append({
            'manifest_id': f"uber_manifest_{i:04d}",
            'dataset_name': np.random.choice(datasets),
            'schema_version': f"v{np.random.randint(1, 5)}.{np.random.randint(0, 10)}",
            'row_count': int(np.random.lognormal(mean=12, sigma=1.5)),
            'size_bytes': int(np.random.lognormal(mean=20, sigma=2)),
            'created_by': np.random.choice(['etl_service', 'data_engineer', 'airflow_dag']),
            'created_ts': created_time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_dataset': 'raw_uber_events',
            'transformation_logic': 'Clean nulls, standardize timestamps, calculate fares',
            'data_quality_checks': json.dumps({"null_check": "passed", "schema_validation": "passed", "row_count_validation": "passed"}),
            'partition_info': json.dumps({"partition_cols": ["date", "city"], "partition_count": np.random.randint(10, 100)})
        })
    
    return pd.DataFrame(data)

# Extracted from app.py - populate_module3_data
def populate_module3_data(conn, company_name):
    """Populate Module 3 database with synthetic ETL pipeline and staging data"""
    cursor = conn.cursor()
    
    try:
        # Check if data already exists for this company
        cursor.execute("SELECT COUNT(*) FROM processing_jobs WHERE company = ?", (company_name,))
        job_count = cursor.fetchone()[0]
        
        # Also check staging table
        staging_table_map = {
            'Uber': 'staging_uber_rides',
            'Netflix': 'staging_netflix_events',
            'Amazon': 'staging_amazon_orders', 
            'Airbnb': 'staging_airbnb_reservations',
            'NYSE': 'staging_nyse_trades'
        }
        staging_table = staging_table_map[company_name]
        cursor.execute(f"SELECT COUNT(*) FROM {staging_table}")
        staging_count = cursor.fetchone()[0]
        
        if job_count > 0 and staging_count > 0:
            print(f"Data already exists for {company_name}. Skipping population.")
            return  # Data already exists for this company
        
        # Generate ETL job data
        if company_name == "Uber":
            jobs_data = generate_uber_etl_jobs(200)
            staging_data = generate_uber_staging_data(8000)
            manifests_data = generate_uber_etl_manifests(15)
        elif company_name == "Netflix":
            jobs_data = generate_netflix_etl_jobs(150)
            staging_data = generate_netflix_staging_data(12000)
            manifests_data = generate_netflix_etl_manifests(12)
        elif company_name == "Amazon":
            jobs_data = generate_amazon_etl_jobs(300)
            staging_data = generate_amazon_staging_data(15000)
            manifests_data = generate_amazon_etl_manifests(20)
        elif company_name == "Airbnb":
            jobs_data = generate_airbnb_etl_jobs(100)
            staging_data = generate_airbnb_staging_data(6000)
            manifests_data = generate_airbnb_etl_manifests(10)
        elif company_name == "NYSE":
            jobs_data = generate_nyse_etl_jobs(500)
            staging_data = generate_nyse_staging_data(50000)
            manifests_data = generate_nyse_etl_manifests(25)
        else:
            print(f"No data generation function for company: {company_name}")
            return
        
        # Insert ETL jobs data in chunks to avoid SQLite variable limit
        chunk_size = 1000  # SQLite default limit is ~999 variables per query
        for i in range(0, len(jobs_data), chunk_size):        
            chunk = jobs_data.iloc[i:i+chunk_size]
            chunk.to_sql('processing_jobs', conn, if_exists='append', index=False)
        
        # Insert staging data
        # Insert staging data in chunks to avoid SQLite variable limit
        chunk_size = 1000  # SQLite default limit is ~999 variables per query
        for i in range(0, len(staging_data), chunk_size):
            chunk = staging_data.iloc[i:i+chunk_size]
            chunk.to_sql(staging_table_map[company_name], conn, if_exists='append', index=False)
        
        # Insert manifests data (small dataset, no chunking needed)
        manifests_data.to_sql('etl_manifests', conn, if_exists='append', index=False)
        
        conn.commit()
        
        # Debug: Show what was inserted
        cursor.execute(f"SELECT COUNT(*) FROM {staging_table}")
        final_staging_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM processing_jobs WHERE company = ?", (company_name,))
        final_job_count = cursor.fetchone()[0]
        
        print(f"Populated {company_name} data: {final_job_count} jobs, {final_staging_count} staging records")
        
    except Exception as e:
        print(f"Error populating Module 3 data for {company_name}: {str(e)}")
        try:
            conn.rollback()
        except:
            pass  # Ignore rollback errors if no transaction is active
        raise e

# Netflix data generation functions
def generate_netflix_etl_jobs(n_jobs=150):
    """Generate Netflix ETL job execution data"""
    np.random.seed(44)
    
    data = []
    job_types = ['batch', 'stream', 'micro-batch']
    engines = ['spark', 'flink', 'airflow', 'kafka-streams']
    statuses = ['completed', 'failed', 'running', 'cancelled']
    job_names = [
        'content-encoding-batch', 'user-behavior-stream', 'recommendation-training',
        'content-quality-analysis', 'viewing-pattern-etl', 'subscription-analytics',
        'content-popularity-batch', 'user-engagement-stream', 'a-b-test-analysis',
        'content-metadata-etl', 'viewing-time-analytics', 'churn-prediction-batch'
    ]
    
    for i in range(n_jobs):
        start_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        if np.random.choice(['spark', 'flink']) in engines:
            duration_ms = int(np.random.lognormal(mean=8.5, sigma=1.1) * 1000)
        else:
            duration_ms = int(np.random.lognormal(mean=6.5, sigma=1.4) * 1000)
        
        end_time = start_time + timedelta(milliseconds=duration_ms)
        
        records_in = int(np.random.lognormal(mean=11, sigma=2.2))
        efficiency = np.random.beta(a=9, b=2)
        records_out = int(records_in * efficiency)
        
        status = np.random.choice(statuses, p=[0.88, 0.08, 0.03, 0.01])
        
        data.append({
            'job_id': f"netflix_job_{i:06d}_{start_time.strftime('%Y%m%d_%H%M%S')}",
            'company': 'Netflix',
            'job_name': np.random.choice(job_names),
            'job_type': np.random.choice(job_types, p=[0.5, 0.4, 0.1]),
            'engine': np.random.choice(engines, p=[0.45, 0.3, 0.2, 0.05]),
            'input_path': f"s3://netflix-data-lake/raw/content/{start_time.strftime('%Y/%m/%d')}",
            'output_path': f"s3://netflix-data-lake/staging/content/{start_time.strftime('%Y/%m/%d')}",
            'records_in': records_in,
            'records_out': records_out if status == 'completed' else 0,
            'start_ts': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_ts': end_time.strftime('%Y-%m-%d %H:%M:%S') if status in ['completed', 'failed'] else None,
            'duration_ms': duration_ms if status in ['completed', 'failed'] else None,
            'status': status,
            'error_msg': 'ResourceExhaustedException: Memory limit exceeded' if status == 'failed' else None,
            'resource_cpu_cores': np.random.choice([8, 16, 32, 64]),
            'resource_memory_gb': np.random.choice([32, 64, 128, 256]),
            'data_quality_score': np.random.beta(a=8, b=1) * 100 if status == 'completed' else None,
            'batch_id': f"batch_{start_time.strftime('%Y%m%d_%H')}"
        })
    
    return pd.DataFrame(data)

def generate_netflix_staging_data(n_records=12000):
    """Generate Netflix staging (cleansed) content and user data"""
    np.random.seed(44)
    
    data = []
    genres = ['Drama', 'Comedy', 'Action', 'Horror', 'Documentary', 'Romance', 'Thriller', 'Sci-Fi']
    devices = ['SmartTV', 'Mobile', 'Desktop', 'Tablet', 'Console']
    countries = ['US', 'UK', 'Canada', 'Brazil', 'India', 'Australia', 'Germany', 'France']
    video_qualities = ['4K', 'HD', 'SD', 'Auto']
    
    for i in range(n_records):
        event_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        playback_duration = np.random.gamma(shape=2, scale=25)  # Average 50min viewing
        
        data.append({
            'event_id': f"netflix_event_{i:08d}",
            'user_id': f"user_{np.random.randint(100000, 999999)}",
            'content_id': f"content_{np.random.randint(10000, 99999)}",
            'genre': np.random.choice(genres),
            'device': np.random.choice(devices),
            'event_ts': event_time.strftime('%Y-%m-%d %H:%M:%S'),
            'playback_sec': int(playback_duration * 60),
            'country': np.random.choice(countries),
            'session_id': f"session_{np.random.randint(1000000, 9999999)}",
            'video_quality': np.random.choice(video_qualities),
            'etl_batch_id': f"etl_batch_{event_time.strftime('%Y%m%d_%H')}",
            'processed_ts': (event_time + timedelta(minutes=np.random.randint(5, 30))).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return pd.DataFrame(data)

def generate_netflix_etl_manifests(n_manifests=12):
    """Generate Netflix ETL manifest data for dataset lineage"""
    np.random.seed(44)
    
    data = []
    datasets = ['raw_viewing_data', 'staging_content', 'user_profiles', 'content_metadata', 'recommendation_features']
    
    for i in range(n_manifests):
        created_time = datetime.now() - timedelta(days=np.random.randint(0, 30))
        
        data.append({
            'manifest_id': f"netflix_manifest_{i:04d}",
            'dataset_name': np.random.choice(datasets),
            'schema_version': f"v{np.random.randint(1, 4)}.{np.random.randint(0, 8)}",
            'row_count': int(np.random.lognormal(mean=13, sigma=1.3)),
            'size_bytes': int(np.random.lognormal(mean=22, sigma=1.8)),
            'created_by': np.random.choice(['content_pipeline', 'analytics_team', 'recommendation_service']),
            'created_ts': created_time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_dataset': 'raw_netflix_events',
            'transformation_logic': 'Content metadata enrichment, user behavior aggregation',
            'data_quality_checks': json.dumps({"content_validation": "passed", "user_privacy_check": "passed", "duplicate_removal": "passed"}),
            'partition_info': json.dumps({"partition_cols": ["date", "country"], "partition_count": np.random.randint(20, 150)})
        })
    
    return pd.DataFrame(data)

# Amazon data generation functions
def generate_amazon_etl_jobs(n_jobs=300):
    """Generate Amazon ETL job execution data"""
    np.random.seed(45)
    
    data = []
    job_types = ['batch', 'stream', 'micro-batch']
    engines = ['spark', 'flink', 'airflow', 'glue', 'dbt']
    statuses = ['completed', 'failed', 'running', 'cancelled']
    job_names = [
        'inventory-sync-batch', 'order-processing-stream', 'price-optimization',
        'fraud-detection-realtime', 'recommendation-training', 'supply-chain-etl',
        'customer-analytics-batch', 'product-catalog-sync', 'review-sentiment-analysis',
        'warehouse-optimization', 'demand-forecasting', 'seller-performance-etl'
    ]
    
    for i in range(n_jobs):
        start_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        if 'glue' in np.random.choice(engines):
            duration_ms = int(np.random.lognormal(mean=7.8, sigma=1.3) * 1000)
        else:
            duration_ms = int(np.random.lognormal(mean=8.2, sigma=1.2) * 1000)
        
        end_time = start_time + timedelta(milliseconds=duration_ms)
        
        records_in = int(np.random.lognormal(mean=12, sigma=2.5))
        efficiency = np.random.beta(a=9, b=1.5)
        records_out = int(records_in * efficiency)
        
        status = np.random.choice(statuses, p=[0.86, 0.10, 0.03, 0.01])
        
        data.append({
            'job_id': f"amazon_job_{i:06d}_{start_time.strftime('%Y%m%d_%H%M%S')}",
            'company': 'Amazon',
            'job_name': np.random.choice(job_names),
            'job_type': np.random.choice(job_types, p=[0.7, 0.25, 0.05]),
            'engine': np.random.choice(engines, p=[0.35, 0.25, 0.2, 0.15, 0.05]),
            'input_path': f"s3://amazon-data-warehouse/raw/orders/{start_time.strftime('%Y/%m/%d')}",
            'output_path': f"s3://amazon-data-warehouse/staging/orders/{start_time.strftime('%Y/%m/%d')}",
            'records_in': records_in,
            'records_out': records_out if status == 'completed' else 0,
            'start_ts': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_ts': end_time.strftime('%Y-%m-%d %H:%M:%S') if status in ['completed', 'failed'] else None,
            'duration_ms': duration_ms if status in ['completed', 'failed'] else None,
            'status': status,
            'error_msg': 'S3Exception: Access denied to data lake' if status == 'failed' else None,
            'resource_cpu_cores': np.random.choice([16, 32, 64, 128]),
            'resource_memory_gb': np.random.choice([64, 128, 256, 512]),
            'data_quality_score': np.random.beta(a=8.5, b=1.2) * 100 if status == 'completed' else None,
            'batch_id': f"batch_{start_time.strftime('%Y%m%d_%H')}"
        })
    
    return pd.DataFrame(data)

def generate_amazon_staging_data(n_records=15000):
    """Generate Amazon staging (cleansed) order and inventory data"""
    np.random.seed(45)
    
    data = []
    channels = ['website', 'mobile_app', 'alexa', 'marketplace']
    fulfillment_centers = ['DXB1', 'AUH2', 'SHJ3', 'RAK1', 'ALN1']
    
    for i in range(n_records):
        order_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        items_count = np.random.poisson(lam=2.5) + 1  # Average 3 items per order
        
        subtotal = np.random.lognormal(mean=4, sigma=1.2)  # AED 30-500 typical
        shipping = subtotal * 0.1 if subtotal > 100 else 15  # Free shipping over AED 100
        tax = subtotal * 0.05  # 5% VAT
        total = subtotal + shipping + tax
        
        data.append({
            'order_id': f"amazon_order_{i:08d}",
            'customer_id': f"customer_{np.random.randint(1000000, 9999999)}",
            'order_ts': order_time.strftime('%Y-%m-%d %H:%M:%S'),
            'items_count': items_count,
            'subtotal_aed': round(subtotal, 2),
            'shipping_aed': round(shipping, 2),
            'tax_aed': round(tax, 2),
            'total_aed': round(total, 2),
            'fulfillment_center': np.random.choice(fulfillment_centers),
            'order_channel': np.random.choice(channels),
            'etl_batch_id': f"etl_batch_{order_time.strftime('%Y%m%d_%H')}",
            'processed_ts': (order_time + timedelta(minutes=np.random.randint(5, 30))).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return pd.DataFrame(data)

def generate_amazon_etl_manifests(n_manifests=20):
    """Generate Amazon ETL manifest data for dataset lineage"""
    np.random.seed(45)
    
    data = []
    datasets = ['raw_orders', 'staging_inventory', 'customer_profiles', 'product_catalog', 'fulfillment_data', 'pricing_data']
    
    for i in range(n_manifests):
        created_time = datetime.now() - timedelta(days=np.random.randint(0, 30))
        
        data.append({
            'manifest_id': f"amazon_manifest_{i:04d}",
            'dataset_name': np.random.choice(datasets),
            'schema_version': f"v{np.random.randint(1, 6)}.{np.random.randint(0, 12)}",
            'row_count': int(np.random.lognormal(mean=14, sigma=1.5)),
            'size_bytes': int(np.random.lognormal(mean=23, sigma=2)),
            'created_by': np.random.choice(['order_pipeline', 'inventory_service', 'pricing_engine', 'customer_analytics']),
            'created_ts': created_time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_dataset': 'raw_amazon_events',
            'transformation_logic': 'Order normalization, inventory tracking, customer segmentation',
            'data_quality_checks': json.dumps({"financial_validation": "passed", "inventory_consistency": "passed", "customer_privacy": "passed"}),
            'partition_info': json.dumps({"partition_cols": ["date", "fulfillment_center"], "partition_count": np.random.randint(50, 200)})
        })
    
    return pd.DataFrame(data)

# Airbnb data generation functions
def generate_airbnb_etl_jobs(n_jobs=100):
    """Generate Airbnb ETL job execution data"""
    np.random.seed(46)
    
    data = []
    job_types = ['batch', 'stream', 'micro-batch']
    engines = ['spark', 'flink', 'airflow', 'kafka-streams']
    statuses = ['completed', 'failed', 'running', 'cancelled']
    job_names = [
        'pricing-model-batch', 'host-quality-score', 'booking-prediction',
        'search-ranking-etl', 'trust-safety-analysis', 'property-analytics',
        'guest-behavior-stream', 'revenue-optimization', 'availability-sync',
        'review-processing', 'market-analysis', 'fraud-detection'
    ]
    
    for i in range(n_jobs):
        start_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        duration_ms = int(np.random.lognormal(mean=7.5, sigma=1.4) * 1000)
        end_time = start_time + timedelta(milliseconds=duration_ms)
        
        records_in = int(np.random.lognormal(mean=9.5, sigma=2))
        efficiency = np.random.beta(a=8.5, b=2)
        records_out = int(records_in * efficiency)
        
        status = np.random.choice(statuses, p=[0.87, 0.09, 0.03, 0.01])
        
        data.append({
            'job_id': f"airbnb_job_{i:06d}_{start_time.strftime('%Y%m%d_%H%M%S')}",
            'company': 'Airbnb',
            'job_name': np.random.choice(job_names),
            'job_type': np.random.choice(job_types, p=[0.65, 0.3, 0.05]),
            'engine': np.random.choice(engines, p=[0.4, 0.25, 0.3, 0.05]),
            'input_path': f"s3://airbnb-data-lake/raw/bookings/{start_time.strftime('%Y/%m/%d')}",
            'output_path': f"s3://airbnb-data-lake/staging/bookings/{start_time.strftime('%Y/%m/%d')}",
            'records_in': records_in,
            'records_out': records_out if status == 'completed' else 0,
            'start_ts': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_ts': end_time.strftime('%Y-%m-%d %H:%M:%S') if status in ['completed', 'failed'] else None,
            'duration_ms': duration_ms if status in ['completed', 'failed'] else None,
            'status': status,
            'error_msg': 'TimeoutException: Spark job timed out' if status == 'failed' else None,
            'resource_cpu_cores': np.random.choice([4, 8, 16, 32]),
            'resource_memory_gb': np.random.choice([16, 32, 64, 128]),
            'data_quality_score': np.random.beta(a=8.2, b=1.3) * 100 if status == 'completed' else None,
            'batch_id': f"batch_{start_time.strftime('%Y%m%d_%H')}"
        })
    
    return pd.DataFrame(data)

def generate_airbnb_staging_data(n_records=6000):
    """Generate Airbnb staging (cleansed) booking and property data"""
    np.random.seed(46)
    
    data = []
    cities = ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Ras Al Khaimah']
    property_types = ['Apartment', 'Villa', 'Studio', 'Penthouse', 'Townhouse']
    statuses = ['confirmed', 'cancelled', 'pending']
    
    for i in range(n_records):
        checkin_date = datetime.now().date() + timedelta(days=np.random.randint(-30, 90))
        nights = np.random.poisson(lam=3) + 1  # Average 4 nights
        checkout_date = checkin_date + timedelta(days=nights)
        
        price_per_night = np.random.lognormal(mean=5, sigma=0.8)  # AED 100-800 per night
        total_price = price_per_night * nights
        
        data.append({
            'booking_id': f"airbnb_booking_{i:08d}",
            'host_id': f"host_{np.random.randint(10000, 99999)}",
            'guest_id': f"guest_{np.random.randint(100000, 999999)}",
            'property_id': f"property_{np.random.randint(100000, 999999)}",
            'checkin_date': checkin_date.strftime('%Y-%m-%d'),
            'checkout_date': checkout_date.strftime('%Y-%m-%d'),
            'nights': nights,
            'price_aed': round(total_price, 2),
            'status': np.random.choice(statuses, p=[0.85, 0.12, 0.03]),
            'property_type': np.random.choice(property_types),
            'city': np.random.choice(cities),
            'etl_batch_id': f"etl_batch_{datetime.now().strftime('%Y%m%d_%H')}",
            'processed_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    return pd.DataFrame(data)

def generate_airbnb_etl_manifests(n_manifests=10):
    """Generate Airbnb ETL manifest data for dataset lineage"""
    np.random.seed(46)
    
    data = []
    datasets = ['raw_bookings', 'staging_properties', 'host_profiles', 'guest_reviews', 'pricing_data']
    
    for i in range(n_manifests):
        created_time = datetime.now() - timedelta(days=np.random.randint(0, 30))
        
        data.append({
            'manifest_id': f"airbnb_manifest_{i:04d}",
            'dataset_name': np.random.choice(datasets),
            'schema_version': f"v{np.random.randint(1, 4)}.{np.random.randint(0, 6)}",
            'row_count': int(np.random.lognormal(mean=11, sigma=1.4)),
            'size_bytes': int(np.random.lognormal(mean=19, sigma=1.7)),
            'created_by': np.random.choice(['booking_service', 'host_platform', 'search_ranking']),
            'created_ts': created_time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_dataset': 'raw_airbnb_events',
            'transformation_logic': 'Booking validation, property enrichment, pricing optimization',
            'data_quality_checks': json.dumps({"booking_validation": "passed", "property_verification": "passed", "pricing_consistency": "passed"}),
            'partition_info': json.dumps({"partition_cols": ["date", "city"], "partition_count": np.random.randint(15, 80)})
        })
    
    return pd.DataFrame(data)

# NYSE data generation functions
def generate_nyse_etl_jobs(n_jobs=500):
    """Generate NYSE ETL job execution data"""
    np.random.seed(47)
    
    data = []
    job_types = ['stream', 'micro-batch', 'batch']  # Mostly streaming for financial data
    engines = ['flink', 'kafka-streams', 'spark', 'storm']
    statuses = ['completed', 'failed', 'running', 'cancelled']
    job_names = [
        'market-data-processing', 'trade-settlement-stream', 'risk-calculation',
        'order-matching-realtime', 'price-discovery-batch', 'compliance-monitoring',
        'market-surveillance', 'volatility-calculation', 'index-computation',
        'clearing-settlement', 'regulatory-reporting', 'liquidity-analysis'
    ]
    
    for i in range(n_jobs):
        start_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        # Financial systems need very fast processing
        duration_ms = int(np.random.lognormal(mean=5.5, sigma=1.5) * 1000)
        end_time = start_time + timedelta(milliseconds=duration_ms)
        
        records_in = int(np.random.lognormal(mean=13, sigma=2))  # High volume
        efficiency = np.random.beta(a=9.5, b=1)  # Very high efficiency needed
        records_out = int(records_in * efficiency)
        
        status = np.random.choice(statuses, p=[0.92, 0.05, 0.02, 0.01])  # High success rate
        
        data.append({
            'job_id': f"nyse_job_{i:06d}_{start_time.strftime('%Y%m%d_%H%M%S')}",
            'company': 'NYSE',
            'job_name': np.random.choice(job_names),
            'job_type': np.random.choice(job_types, p=[0.6, 0.3, 0.1]),  # Mostly streaming
            'engine': np.random.choice(engines, p=[0.4, 0.3, 0.25, 0.05]),
            'input_path': f"kafka://nyse-market-data/trades/{start_time.strftime('%Y/%m/%d')}",
            'output_path': f"s3://nyse-processed-data/settlements/{start_time.strftime('%Y/%m/%d')}",
            'records_in': records_in,
            'records_out': records_out if status == 'completed' else 0,
            'start_ts': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_ts': end_time.strftime('%Y-%m-%d %H:%M:%S') if status in ['completed', 'failed'] else None,
            'duration_ms': duration_ms if status in ['completed', 'failed'] else None,
            'status': status,
            'error_msg': 'LatencyException: Processing latency exceeded SLA' if status == 'failed' else None,
            'resource_cpu_cores': np.random.choice([32, 64, 128, 256]),
            'resource_memory_gb': np.random.choice([128, 256, 512, 1024]),
            'data_quality_score': np.random.beta(a=9.8, b=0.5) * 100 if status == 'completed' else None,
            'batch_id': f"batch_{start_time.strftime('%Y%m%d_%H')}"
        })
    
    return pd.DataFrame(data)

def generate_nyse_staging_data(n_records=50000):
    """Generate NYSE staging (cleansed) trade and settlement data"""
    np.random.seed(47)
    
    data = []
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK.B', 'JNJ', 'JPM']
    venues = ['NYSE', 'NASDAQ', 'BATS', 'EDGE', 'ARCA']
    trade_types = ['buy', 'sell']
    
    for i in range(n_records):
        trade_time = datetime.now() - timedelta(milliseconds=np.random.randint(0, 604800000))  # Past week
        
        # NYSE typically closed auction vs continuous trading
        is_auction = 1 if np.random.random() < 0.1 else 0
        
        ticker = np.random.choice(tickers)
        # Simulate realistic stock prices
        base_prices = {'AAPL': 150, 'MSFT': 300, 'GOOGL': 2500, 'AMZN': 3200, 'TSLA': 800, 
                      'META': 200, 'NVDA': 400, 'BRK.B': 300, 'JNJ': 160, 'JPM': 140}
        
        base_price = base_prices.get(ticker, 100)
        price = base_price * np.random.normal(1, 0.02)  # 2% volatility
        
        size = int(np.random.lognormal(mean=4, sigma=1.5))  # 100-10,000 shares typical
        
        data.append({
            'tick_id': f"nyse_tick_{i:08d}",
            'ticker': ticker,
            'timestamp_ms': str(int(trade_time.timestamp() * 1000)),
            'price': round(price, 2),
            'size': size,
            'venue': np.random.choice(venues),
            'is_auction': is_auction,
            'trade_type': np.random.choice(trade_types),
            'etl_batch_id': f"etl_batch_{trade_time.strftime('%Y%m%d_%H')}",
            'processed_ts': (trade_time + timedelta(milliseconds=np.random.randint(1, 100))).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        })
    
    return pd.DataFrame(data)

def generate_nyse_etl_manifests(n_manifests=25):
    """Generate NYSE ETL manifest data for dataset lineage"""
    np.random.seed(47)
    
    data = []
    datasets = ['raw_trades', 'staging_settlements', 'market_data', 'risk_metrics', 'compliance_reports', 'index_calculations']
    
    for i in range(n_manifests):
        created_time = datetime.now() - timedelta(days=np.random.randint(0, 30))
        
        data.append({
            'manifest_id': f"nyse_manifest_{i:04d}",
            'dataset_name': np.random.choice(datasets),
            'schema_version': f"v{np.random.randint(1, 8)}.{np.random.randint(0, 15)}",
            'row_count': int(np.random.lognormal(mean=15, sigma=1.8)),
            'size_bytes': int(np.random.lognormal(mean=24, sigma=2.2)),
            'created_by': np.random.choice(['trading_engine', 'settlement_service', 'risk_management', 'compliance_system']),
            'created_ts': created_time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_dataset': 'raw_nyse_market_data',
            'transformation_logic': 'Trade validation, settlement processing, risk calculation',
            'data_quality_checks': json.dumps({"trade_validation": "passed", "settlement_reconciliation": "passed", "regulatory_compliance": "passed"}),
            'partition_info': json.dumps({"partition_cols": ["date", "venue"], "partition_count": np.random.randint(100, 500)})
        })
    
    return pd.DataFrame(data)

# Main execution for Processing module (all companies)
if __name__ == "__main__":
    print("Initializing Module 3 database and populating Processing data for all companies...")
    conn = init_module3_database()
    
    companies = ["Uber", "Netflix", "Amazon", "Airbnb", "NYSE"]
    
    for company in companies:
        print(f"Populating data for {company}...")
        populate_module3_data(conn, company)
    
    conn.close()
    print("Processing module data population complete for all companies.")
