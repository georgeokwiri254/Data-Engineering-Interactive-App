import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import sqlite3
from random import choice, randint
import logging
import os

st.set_page_config(
    page_title="Data Architecture & Engineering Learning Hub",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize SQLite database for logging
def init_logging_db():
    conn = sqlite3.connect('app_logs.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            module TEXT NOT NULL,
            message TEXT NOT NULL,
            user_session TEXT,
            ip_address TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Function to log activities
def log_activity(level, module, message, user_session=None):
    conn = sqlite3.connect('app_logs.db')
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        INSERT INTO app_logs (timestamp, level, module, message, user_session, ip_address)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (timestamp, level, module, message, user_session, st.session_state.get('client_ip', 'unknown')))
    conn.commit()
    conn.close()

# Initialize logging database
init_logging_db()

# Add caching to improve performance
@st.cache_data
def generate_sample_data():
    """Generate sample data with caching to improve performance"""
    np.random.seed(42)
    n_records = 5000
    return pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=n_records, freq='15min'),
        'user_id': np.random.randint(1000, 9999, n_records),
        'event_type': np.random.choice(['purchase', 'view', 'click', 'login', 'logout'], n_records, p=[0.1, 0.4, 0.3, 0.1, 0.1]),
        'value': np.random.lognormal(mean=3, sigma=1, size=n_records).round(2),
        'source': np.random.choice(['mobile', 'web', 'api', 'batch'], n_records, p=[0.4, 0.3, 0.2, 0.1]),
        'region': np.random.choice(['US', 'EU', 'ASIA', 'LATAM'], n_records, p=[0.4, 0.3, 0.2, 0.1]),
        'processing_time_ms': np.random.exponential(scale=50, size=n_records).round(1),
        'success': np.random.choice([True, False], n_records, p=[0.95, 0.05])
    })

@st.cache_data
def create_company_database():
    """Create SQLite database with company synthetic datasets"""
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    
    # Netflix Data
    netflix_data = generate_netflix_data()
    netflix_data.to_sql('netflix_viewership', conn, if_exists='replace', index=False)
    
    # Amazon Data  
    amazon_data = generate_amazon_data()
    amazon_data.to_sql('amazon_sales', conn, if_exists='replace', index=False)
    
    # Uber Data
    uber_data = generate_uber_data()
    uber_data.to_sql('uber_rides', conn, if_exists='replace', index=False)
    
    # NYSE Data
    nyse_data = generate_nyse_data()
    nyse_data.to_sql('nyse_trades', conn, if_exists='replace', index=False)
    
    return conn

# ============================================================================
# MODULE 1: SQLite DATABASE INTEGRATION
# ============================================================================

@st.cache_resource
def init_module1_database():
    """Initialize Module 1 SQLite database with proper schema and optimization"""
    conn = sqlite3.connect('module1_ingestion.db', check_same_thread=False)
    cursor = conn.cursor()
    
    # Apply SQLite optimizations per Module 1 specifications
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL") 
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA temp_store = MEMORY")
    cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB
    
    # Create Module 1 ingestion tables per schema specifications
    
    # Uber ingestion events table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingest_uber_events (
            event_id TEXT PRIMARY KEY,
            ride_id TEXT,
            driver_id TEXT,
            rider_id TEXT,
            event_type TEXT,
            pickup_ts TEXT,
            dropoff_ts TEXT,
            pickup_lat REAL,
            pickup_lng REAL,
            dropoff_lat REAL,
            dropoff_lng REAL,
            distance_km REAL,
            price_aed REAL,
            payment_method TEXT,
            status TEXT,
            ingestion_ts TEXT
        )
    ''')
    
    # Netflix ingestion events table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingest_netflix_events (
            event_id TEXT PRIMARY KEY,
            user_id TEXT,
            device_type TEXT,
            content_id TEXT,
            content_title TEXT,
            event_type TEXT,
            timestamp TEXT,
            duration_sec INTEGER,
            bitrate_kbps INTEGER,
            country TEXT,
            subscription_tier TEXT
        )
    ''')
    
    # Amazon order events table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingest_amazon_orders (
            event_id TEXT PRIMARY KEY,
            order_id TEXT,
            customer_id TEXT,
            product_id TEXT,
            event_type TEXT,
            quantity INTEGER,
            unit_price_aed REAL,
            total_price_aed REAL,
            timestamp TEXT,
            channel TEXT,
            product_category TEXT
        )
    ''')
    
    # Airbnb booking events table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingest_airbnb_bookings (
            event_id TEXT PRIMARY KEY,
            booking_id TEXT,
            host_id TEXT,
            guest_id TEXT,
            property_id TEXT,
            event_type TEXT,
            checkin TEXT,
            checkout TEXT,
            price_per_night_aed REAL,
            total_price_aed REAL,
            nights INTEGER,
            timestamp TEXT,
            city TEXT,
            property_type TEXT
        )
    ''')
    
    # NYSE tick events table (high-frequency)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ingest_nyse_ticks (
            tick_id TEXT PRIMARY KEY,
            ticker TEXT,
            trade_ts TEXT,
            price REAL,
            size INTEGER,
            trade_type TEXT,
            exchange TEXT,
            order_id TEXT
        )
    ''')
    
    # Create indexes for high-cardinality columns as per specifications
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_uber_pickup_ts ON ingest_uber_events(pickup_ts)",
        "CREATE INDEX IF NOT EXISTS idx_uber_event_type ON ingest_uber_events(event_type)",
        "CREATE INDEX IF NOT EXISTS idx_uber_driver_id ON ingest_uber_events(driver_id)",
        
        "CREATE INDEX IF NOT EXISTS idx_netflix_timestamp ON ingest_netflix_events(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_netflix_user_id ON ingest_netflix_events(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_netflix_event_type ON ingest_netflix_events(event_type)",
        
        "CREATE INDEX IF NOT EXISTS idx_amazon_timestamp ON ingest_amazon_orders(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_amazon_customer_id ON ingest_amazon_orders(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_amazon_event_type ON ingest_amazon_orders(event_type)",
        
        "CREATE INDEX IF NOT EXISTS idx_airbnb_timestamp ON ingest_airbnb_bookings(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_airbnb_event_type ON ingest_airbnb_bookings(event_type)",
        "CREATE INDEX IF NOT EXISTS idx_airbnb_city ON ingest_airbnb_bookings(city)",
        
        "CREATE INDEX IF NOT EXISTS idx_nyse_trade_ts ON ingest_nyse_ticks(trade_ts)",
        "CREATE INDEX IF NOT EXISTS idx_nyse_ticker ON ingest_nyse_ticks(ticker)",
        "CREATE INDEX IF NOT EXISTS idx_nyse_exchange ON ingest_nyse_ticks(exchange)"
    ]
    
    for index in indexes:
        cursor.execute(index)
    
    conn.commit()
    return conn

def populate_module1_data(conn, company_name):
    """Populate Module 1 database with synthetic data using batch transactions"""
    cursor = conn.cursor()
    
    # Check if data already exists
    table_map = {
        'Uber': 'ingest_uber_events',
        'Netflix': 'ingest_netflix_events', 
        'Amazon': 'ingest_amazon_orders',
        'Airbnb': 'ingest_airbnb_bookings',
        'NYSE': 'ingest_nyse_ticks'
    }
    
    table_name = table_map[company_name]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    
    if count > 0:
        return  # Data already exists
    
    # Generate and insert data using transactions for speed
    try:
        # Start transaction
        cursor.execute("BEGIN")
        
        if company_name == 'Uber':
            data = generate_uber_ingest_events(5000)
            data.to_sql('ingest_uber_events', conn, if_exists='append', index=False)
        elif company_name == 'Netflix':
            data = generate_netflix_ingest_events(5000)
            data.to_sql('ingest_netflix_events', conn, if_exists='append', index=False)
        elif company_name == 'Amazon':
            data = generate_amazon_ingest_events(5000)
            data.to_sql('ingest_amazon_orders', conn, if_exists='append', index=False)
        elif company_name == 'Airbnb':
            data = generate_airbnb_ingest_events(5000)
            data.to_sql('ingest_airbnb_bookings', conn, if_exists='append', index=False)
        elif company_name == 'NYSE':
            data = generate_nyse_ingest_ticks(10000)
            data.to_sql('ingest_nyse_ticks', conn, if_exists='append', index=False)
        
        # Commit transaction
        conn.commit()
        
    except Exception as e:
        # Only rollback if transaction is active
        try:
            conn.rollback()
        except:
            pass  # Ignore rollback errors if no transaction is active
        raise e

def load_module1_data_from_db(conn, company_name, limit=None):
    """Load Module 1 data from SQLite database with optional filtering"""
    table_map = {
        'Uber': 'ingest_uber_events',
        'Netflix': 'ingest_netflix_events',
        'Amazon': 'ingest_amazon_orders', 
        'Airbnb': 'ingest_airbnb_bookings',
        'NYSE': 'ingest_nyse_ticks'
    }
    
    table_name = table_map[company_name]
    query = f"SELECT * FROM {table_name}"
    
    if limit:
        query += f" LIMIT {limit}"
        
    return pd.read_sql_query(query, conn)

def execute_module1_sql_query(conn, query):
    """Execute custom SQL queries on Module 1 database"""
    return pd.read_sql_query(query, conn)

# ============================================================================
# MODULE 2: RAW LANDING - SQLITE DATABASE INTEGRATION
# ============================================================================

@st.cache_resource
def init_module2_database():
    """Initialize Module 2 SQLite database for raw landing storage"""
    conn = sqlite3.connect('module2_raw_landing.db', check_same_thread=False)
    cursor = conn.cursor()
    
    # Apply SQLite optimizations per Module 2 specifications
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL") 
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA temp_store = MEMORY")
    cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB
    
    # Create Module 2 raw landing table per schema specifications
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_landing (
            raw_id TEXT PRIMARY KEY,
            company TEXT NOT NULL,
            source_system TEXT NOT NULL,
            raw_payload TEXT NOT NULL,  -- JSON payload
            file_name TEXT,
            arrival_ts TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            payload_size_bytes INTEGER,
            schema_version TEXT,
            source_ip TEXT,
            processing_status TEXT DEFAULT 'pending'
        )
    ''')
    
    # Create indexes for high-cardinality columns
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_company_arrival ON raw_landing(company, arrival_ts)",
        "CREATE INDEX IF NOT EXISTS idx_partition_key ON raw_landing(partition_key)",
        "CREATE INDEX IF NOT EXISTS idx_source_system ON raw_landing(source_system)",
        "CREATE INDEX IF NOT EXISTS idx_arrival_ts ON raw_landing(arrival_ts)",
        "CREATE INDEX IF NOT EXISTS idx_processing_status ON raw_landing(processing_status)"
    ]
    
    for index in indexes:
        cursor.execute(index)
    
    conn.commit()
    return conn

def populate_module2_data(conn, company_name):
    """Populate Module 2 database with synthetic raw landing data"""
    cursor = conn.cursor()
    
    # Check if data already exists
    cursor.execute("SELECT COUNT(*) FROM raw_landing WHERE company = ?", (company_name,))
    count = cursor.fetchone()[0]
    
    if count > 0:
        return  # Data already exists
    
    # Generate and insert raw landing data using batch transactions
    try:
        cursor.execute("BEGIN")
        
        if company_name == 'Uber':
            data = generate_uber_raw_landing(3000)
        elif company_name == 'Netflix':
            data = generate_netflix_raw_landing(3000)
        elif company_name == 'Amazon':
            data = generate_amazon_raw_landing(3000)
        elif company_name == 'Airbnb':
            data = generate_airbnb_raw_landing(3000)
        elif company_name == 'NYSE':
            data = generate_nyse_raw_landing(5000)
        
        # Insert data
        data.to_sql('raw_landing', conn, if_exists='append', index=False)
        conn.commit()
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        raise e

def load_module2_data_from_db(conn, company_name, limit=None):
    """Load Module 2 data from SQLite database"""
    query = f"SELECT * FROM raw_landing WHERE company = '{company_name}'"
    if limit:
        query += f" LIMIT {limit}"
    return pd.read_sql_query(query, conn)

def execute_module2_sql_query(conn, query):
    """Execute custom SQL queries on Module 2 database"""
    return pd.read_sql_query(query, conn)

# ============================================================================
# MODULE 2: RAW LANDING - SYNTHETIC DATA GENERATORS
# ============================================================================

@st.cache_data
def generate_uber_raw_landing(n_records=3000):
    """Generate Uber raw landing data - unstructured JSON payloads"""
    np.random.seed(42)
    
    data = []
    source_systems = ['mobile-app-ios', 'mobile-app-android', 'driver-app', 'web-portal', 'api-gateway']
    processing_statuses = ['pending', 'processed', 'failed', 'archived']
    
    for i in range(n_records):
        arrival_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))  # Last 7 days
        
        # Create realistic raw payload JSON
        raw_payload = {
            "metadata": {
                "event_version": "2.1",
                "source": np.random.choice(source_systems),
                "timestamp": arrival_time.isoformat(),
                "correlation_id": f"corr_{i:08d}",
                "session_id": f"sess_{np.random.randint(100000, 999999)}"
            },
            "data": {
                "ride_id": f"ride_{i:06d}",
                "driver_id": f"drv_{np.random.randint(1000, 9999):04d}",
                "rider_id": f"usr_{np.random.randint(10000, 99999):05d}",
                "event_type": np.random.choice(['request', 'accept', 'start', 'end', 'cancel']),
                "location": {
                    "pickup": {
                        "lat": 25.2048 + np.random.normal(0, 0.05),
                        "lng": 55.2708 + np.random.normal(0, 0.05),
                        "address": "Dubai Mall Area"
                    },
                    "dropoff": {
                        "lat": 25.2048 + np.random.normal(0, 0.08),
                        "lng": 55.2708 + np.random.normal(0, 0.08),
                        "address": "Downtown Dubai"
                    }
                },
                "pricing": {
                    "base_fare": round(np.random.exponential(scale=30) + 5, 2),
                    "distance_km": round(np.random.exponential(scale=8) + 1, 2),
                    "surge_multiplier": np.random.choice([1.0, 1.2, 1.5, 2.0], p=[0.7, 0.15, 0.1, 0.05]),
                    "currency": "AED"
                },
                "device_info": {
                    "os": np.random.choice(['iOS', 'Android']),
                    "app_version": f"{np.random.randint(8, 12)}.{np.random.randint(0, 9)}.{np.random.randint(0, 9)}",
                    "device_id": f"device_{np.random.randint(1000000, 9999999)}"
                }
            }
        }
        
        payload_json = json.dumps(raw_payload)
        
        data.append({
            'raw_id': f"uber_raw_{i:08d}",
            'company': 'Uber',
            'source_system': raw_payload['metadata']['source'],
            'raw_payload': payload_json,
            'file_name': f"uber_events_{arrival_time.strftime('%Y%m%d_%H')}.json",
            'arrival_ts': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'partition_key': f"company=uber/date={arrival_time.strftime('%Y-%m-%d')}/hour={arrival_time.hour:02d}",
            'payload_size_bytes': len(payload_json.encode('utf-8')),
            'schema_version': raw_payload['metadata']['event_version'],
            'source_ip': f"192.168.{np.random.randint(1, 255)}.{np.random.randint(1, 255)}",
            'processing_status': np.random.choice(processing_statuses, p=[0.3, 0.6, 0.08, 0.02])
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_netflix_raw_landing(n_records=3000):
    """Generate Netflix raw landing data - streaming event payloads"""
    np.random.seed(42)
    
    data = []
    source_systems = ['smart-tv', 'mobile-ios', 'mobile-android', 'web-browser', 'gaming-console']
    content_titles = ['Stranger Things', 'The Crown', 'Squid Game', 'Wednesday', 'Ozark', 'Dark']
    
    for i in range(n_records):
        arrival_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        raw_payload = {
            "event_metadata": {
                "schema_version": "3.2",
                "event_id": f"nf_raw_{i:08d}",
                "ingestion_time": arrival_time.isoformat(),
                "source_platform": np.random.choice(source_systems),
                "geo_location": {
                    "country": np.random.choice(['UAE', 'USA', 'UK', 'CA', 'AU']),
                    "region": "EMEA" if np.random.choice(['UAE', 'UK']) else "AMERICAS",
                    "isp": f"ISP_{np.random.randint(1, 50)}"
                }
            },
            "user_session": {
                "user_id": f"nf_usr_{np.random.randint(100000, 999999):06d}",
                "session_id": f"sess_{np.random.randint(1000000, 9999999):07d}",
                "device_info": {
                    "platform": np.random.choice(source_systems),
                    "os_version": f"{np.random.randint(10, 15)}.{np.random.randint(0, 9)}",
                    "app_version": f"Netflix {np.random.randint(8, 12)}.{np.random.randint(0, 20)}.0",
                    "screen_resolution": np.random.choice(["1920x1080", "3840x2160", "1366x768", "1280x720"])
                }
            },
            "playback_event": {
                "content_id": f"cnt_{np.random.randint(100, 999):03d}",
                "content_title": np.random.choice(content_titles),
                "event_type": np.random.choice(['play', 'pause', 'seek', 'resume', 'stop']),
                "playback_position_sec": np.random.randint(0, 7200),
                "video_quality": np.random.choice(['720p', '1080p', '4K', 'Auto']),
                "audio_language": np.random.choice(['en-US', 'ar-AE', 'es-ES', 'fr-FR']),
                "subtitle_language": np.random.choice(['None', 'en', 'ar', 'es'])
            }
        }
        
        payload_json = json.dumps(raw_payload)
        
        data.append({
            'raw_id': f"netflix_raw_{i:08d}",
            'company': 'Netflix',
            'source_system': raw_payload['event_metadata']['source_platform'],
            'raw_payload': payload_json,
            'file_name': f"netflix_events_{arrival_time.strftime('%Y%m%d_%H')}.json",
            'arrival_ts': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'partition_key': f"company=netflix/date={arrival_time.strftime('%Y-%m-%d')}/hour={arrival_time.hour:02d}",
            'payload_size_bytes': len(payload_json.encode('utf-8')),
            'schema_version': raw_payload['event_metadata']['schema_version'],
            'source_ip': f"10.{np.random.randint(0, 255)}.{np.random.randint(0, 255)}.{np.random.randint(1, 255)}",
            'processing_status': np.random.choice(['pending', 'processed', 'failed'], p=[0.2, 0.75, 0.05])
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_amazon_raw_landing(n_records=3000):
    """Generate Amazon raw landing data - e-commerce event payloads"""
    np.random.seed(42)
    
    data = []
    source_systems = ['web-frontend', 'mobile-app', 'alexa-service', 'api-gateway', 'warehouse-system']
    
    for i in range(n_records):
        arrival_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        raw_payload = {
            "request_info": {
                "request_id": f"amz_req_{i:010d}",
                "api_version": "v2.0",
                "timestamp": arrival_time.isoformat(),
                "source": np.random.choice(source_systems),
                "region": "me-south-1",
                "trace_id": f"trace_{np.random.randint(100000000, 999999999)}"
            },
            "customer_context": {
                "customer_id": f"cust_{np.random.randint(100000, 999999):06d}",
                "session_id": f"session_{np.random.randint(1000000, 9999999)}",
                "marketplace": "amazon.ae",
                "currency": "AED",
                "language": "en-AE"
            },
            "order_data": {
                "order_id": f"order_{i:010d}",
                "event_type": np.random.choice(['created', 'paid', 'shipped', 'delivered', 'returned']),
                "items": [
                    {
                        "product_id": f"prod_{np.random.randint(100000, 999999):06d}",
                        "quantity": np.random.randint(1, 5),
                        "unit_price_aed": round(np.random.exponential(scale=100) + 10, 2),
                        "category": np.random.choice(['electronics', 'books', 'clothing', 'home'])
                    }
                ],
                "shipping_address": {
                    "city": np.random.choice(['Dubai', 'Abu Dhabi', 'Sharjah']),
                    "emirate": "Dubai",
                    "postal_code": f"{np.random.randint(10000, 99999)}"
                },
                "payment_info": {
                    "method": np.random.choice(['credit_card', 'debit_card', 'cash_on_delivery']),
                    "currency": "AED"
                }
            }
        }
        
        payload_json = json.dumps(raw_payload)
        
        data.append({
            'raw_id': f"amazon_raw_{i:08d}",
            'company': 'Amazon',
            'source_system': raw_payload['request_info']['source'],
            'raw_payload': payload_json,
            'file_name': f"amazon_orders_{arrival_time.strftime('%Y%m%d_%H')}.json",
            'arrival_ts': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'partition_key': f"company=amazon/date={arrival_time.strftime('%Y-%m-%d')}/hour={arrival_time.hour:02d}",
            'payload_size_bytes': len(payload_json.encode('utf-8')),
            'schema_version': raw_payload['request_info']['api_version'],
            'source_ip': f"172.16.{np.random.randint(0, 255)}.{np.random.randint(1, 255)}",
            'processing_status': np.random.choice(['pending', 'processed', 'failed'], p=[0.25, 0.7, 0.05])
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_airbnb_raw_landing(n_records=3000):
    """Generate Airbnb raw landing data - booking platform payloads"""
    np.random.seed(42)
    
    data = []
    source_systems = ['web-app', 'mobile-ios', 'mobile-android', 'host-dashboard', 'channel-manager']
    
    for i in range(n_records):
        arrival_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        
        raw_payload = {
            "event_header": {
                "event_id": f"bnb_raw_{i:08d}",
                "version": "1.3",
                "timestamp": arrival_time.isoformat(),
                "source_application": np.random.choice(source_systems),
                "environment": "production",
                "correlation_id": f"corr_{np.random.randint(1000000, 9999999)}"
            },
            "booking_event": {
                "booking_id": f"booking_{i:08d}",
                "host_id": f"host_{np.random.randint(10000, 99999):05d}",
                "guest_id": f"guest_{np.random.randint(100000, 999999):06d}",
                "property_id": f"prop_{np.random.randint(10000, 99999):05d}",
                "event_type": np.random.choice(['search', 'inquiry', 'booking', 'cancellation', 'review']),
                "dates": {
                    "checkin": (arrival_time + timedelta(days=np.random.randint(1, 90))).strftime('%Y-%m-%d'),
                    "checkout": (arrival_time + timedelta(days=np.random.randint(2, 95))).strftime('%Y-%m-%d'),
                    "nights": np.random.randint(1, 14)
                },
                "property_details": {
                    "city": np.random.choice(['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman']),
                    "property_type": np.random.choice(['apartment', 'villa', 'hotel_room']),
                    "guests": np.random.randint(1, 8),
                    "bedrooms": np.random.randint(1, 4)
                },
                "pricing": {
                    "currency": "AED",
                    "base_price_per_night": round(np.random.exponential(scale=400) + 150, 2),
                    "cleaning_fee": round(np.random.uniform(50, 150), 2),
                    "service_fee": round(np.random.uniform(20, 80), 2)
                }
            },
            "user_context": {
                "user_agent": "Mozilla/5.0 (compatible browser info)",
                "ip_geolocation": {
                    "country": np.random.choice(['AE', 'US', 'UK', 'DE', 'FR']),
                    "city": np.random.choice(['Dubai', 'London', 'New York', 'Berlin'])
                },
                "language_preference": np.random.choice(['en-US', 'ar-AE', 'fr-FR', 'de-DE'])
            }
        }
        
        payload_json = json.dumps(raw_payload)
        
        data.append({
            'raw_id': f"airbnb_raw_{i:08d}",
            'company': 'Airbnb',
            'source_system': raw_payload['event_header']['source_application'],
            'raw_payload': payload_json,
            'file_name': f"airbnb_events_{arrival_time.strftime('%Y%m%d_%H')}.json",
            'arrival_ts': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'partition_key': f"company=airbnb/date={arrival_time.strftime('%Y-%m-%d')}/hour={arrival_time.hour:02d}",
            'payload_size_bytes': len(payload_json.encode('utf-8')),
            'schema_version': raw_payload['event_header']['version'],
            'source_ip': f"203.{np.random.randint(0, 255)}.{np.random.randint(0, 255)}.{np.random.randint(1, 255)}",
            'processing_status': np.random.choice(['pending', 'processed', 'failed'], p=[0.3, 0.65, 0.05])
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_nyse_raw_landing(n_records=5000):
    """Generate NYSE raw landing data - high-frequency trading payloads"""
    np.random.seed(42)
    
    data = []
    source_systems = ['market-data-feed', 'order-gateway', 'matching-engine', 'surveillance-system']
    
    for i in range(n_records):
        arrival_time = datetime.now() - timedelta(hours=np.random.randint(0, 48))  # Last 2 days (trading days)
        
        raw_payload = {
            "message_header": {
                "sequence_number": i,
                "message_type": "TRADE_EXECUTION",
                "timestamp_ns": int(arrival_time.timestamp() * 1000000000) + np.random.randint(0, 999999999),
                "source_system": np.random.choice(source_systems),
                "market_session": np.random.choice(['PRE_MARKET', 'REGULAR', 'AFTER_HOURS']),
                "venue": np.random.choice(['NYSE', 'NASDAQ', 'BATS', 'IEX'])
            },
            "trade_data": {
                "symbol": np.random.choice(['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA']),
                "trade_id": f"trade_{i:012d}",
                "price": round(175 + np.random.normal(0, 5), 2),
                "quantity": np.random.randint(100, 50000),
                "side": np.random.choice(['BUY', 'SELL']),
                "order_type": np.random.choice(['MARKET', 'LIMIT', 'STOP']),
                "execution_quality": {
                    "latency_microseconds": np.random.randint(50, 500),
                    "price_improvement": round(np.random.uniform(-0.05, 0.05), 4) if np.random.random() > 0.7 else 0
                }
            },
            "regulatory_info": {
                "sip_timestamp": arrival_time.isoformat(),
                "trade_conditions": np.random.choice(['@', 'F', 'I', 'T', 'Z']),  # Trade condition codes
                "settlement_date": (arrival_time + timedelta(days=2)).strftime('%Y-%m-%d'),
                "reporting_party": np.random.choice(['FIRM_A', 'FIRM_B', 'MM_1', 'MM_2'])
            }
        }
        
        payload_json = json.dumps(raw_payload)
        
        data.append({
            'raw_id': f"nyse_raw_{i:08d}",
            'company': 'NYSE',
            'source_system': raw_payload['message_header']['source_system'],
            'raw_payload': payload_json,
            'file_name': f"nyse_trades_{arrival_time.strftime('%Y%m%d_%H%M')}.json",
            'arrival_ts': arrival_time.strftime('%Y-%m-%d %H:%M:%S'),
            'partition_key': f"company=nyse/date={arrival_time.strftime('%Y-%m-%d')}/hour={arrival_time.hour:02d}",
            'payload_size_bytes': len(payload_json.encode('utf-8')),
            'schema_version': raw_payload['message_header']['message_type'],
            'source_ip': f"10.0.{np.random.randint(1, 255)}.{np.random.randint(1, 255)}",
            'processing_status': np.random.choice(['pending', 'processed', 'failed'], p=[0.1, 0.88, 0.02])
        })
    
    return pd.DataFrame(data)

# ============================================================================
# MODULE 1: INGESTION - CHART HELPER FUNCTIONS
# ============================================================================

def create_overview_dashboard(data, company_name):
    """Create overview dashboard for company data"""
    st.markdown(f"### 📊 {company_name} Dataset Overview")
    
    if company_name == "Uber":
        col1, col2 = st.columns(2)
        with col1:
            # Event type distribution
            event_counts = data['event_type'].value_counts()
            fig = px.pie(values=event_counts.values, names=event_counts.index,
                        title="Ride Event Types Distribution")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Price distribution
            fig = px.histogram(data, x='price_aed', nbins=50,
                             title="Ride Price Distribution (AED)")
            st.plotly_chart(fig, use_container_width=True)
            
        # Payment methods
        payment_counts = data['payment_method'].value_counts()
        fig = px.bar(x=payment_counts.index, y=payment_counts.values,
                    title="Payment Methods Usage")
        st.plotly_chart(fig, use_container_width=True)
        
    elif company_name == "Netflix":
        col1, col2 = st.columns(2)
        with col1:
            # Device type distribution
            device_counts = data['device_type'].value_counts()
            fig = px.pie(values=device_counts.values, names=device_counts.index,
                        title="Device Usage Distribution")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Event type distribution
            event_counts = data['event_type'].value_counts()
            fig = px.bar(x=event_counts.index, y=event_counts.values,
                        title="Streaming Events")
            st.plotly_chart(fig, use_container_width=True)
            
        # Country distribution
        country_counts = data['country'].value_counts().head(10)
        fig = px.bar(x=country_counts.values, y=country_counts.index,
                    orientation='h', title="Top 10 Countries by Views")
        st.plotly_chart(fig, use_container_width=True)
        
    elif company_name == "Amazon":
        col1, col2 = st.columns(2)
        with col1:
            # Channel distribution
            channel_counts = data['channel'].value_counts()
            fig = px.pie(values=channel_counts.values, names=channel_counts.index,
                        title="Order Channels Distribution")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Price distribution
            fig = px.histogram(data, x='total_price_aed', nbins=50,
                             title="Order Value Distribution (AED)")
            st.plotly_chart(fig, use_container_width=True)
            
        # Event type distribution
        event_counts = data['event_type'].value_counts()
        fig = px.bar(x=event_counts.index, y=event_counts.values,
                    title="Order Event Types")
        st.plotly_chart(fig, use_container_width=True)
        
    elif company_name == "Airbnb":
        col1, col2 = st.columns(2)
        with col1:
            # Property type distribution
            prop_counts = data['property_type'].value_counts()
            fig = px.pie(values=prop_counts.values, names=prop_counts.index,
                        title="Property Types Distribution")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Price per night distribution
            fig = px.histogram(data, x='price_per_night_aed', nbins=50,
                             title="Nightly Rates Distribution (AED)")
            st.plotly_chart(fig, use_container_width=True)
            
        # City distribution
        city_counts = data['city'].value_counts()
        fig = px.bar(x=city_counts.index, y=city_counts.values,
                    title="Bookings by UAE City")
        st.plotly_chart(fig, use_container_width=True)
        
    else:  # NYSE
        col1, col2 = st.columns(2)
        with col1:
            # Ticker distribution
            ticker_counts = data['ticker'].value_counts()
            fig = px.bar(x=ticker_counts.index, y=ticker_counts.values,
                        title="Trading Volume by Ticker")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Price distribution
            fig = px.histogram(data, x='price', nbins=50,
                             title="Stock Price Distribution ($)")
            st.plotly_chart(fig, use_container_width=True)
            
        # Exchange distribution
        exchange_counts = data['exchange'].value_counts()
        fig = px.pie(values=exchange_counts.values, names=exchange_counts.index,
                    title="Trading by Exchange")
        st.plotly_chart(fig, use_container_width=True)

def create_time_series_charts(data, company_name):
    """Create time series analysis charts"""
    st.markdown(f"### 📈 {company_name} Time Series Analysis")
    
    # Find timestamp column
    time_cols = [col for col in data.columns if 'timestamp' in col or col.endswith('_ts') or 'time' in col]
    
    if time_cols:
        time_col = time_cols[0]
        data[time_col] = pd.to_datetime(data[time_col])
        
        # Hourly aggregation
        data['hour'] = data[time_col].dt.hour
        hourly_counts = data.groupby('hour').size()
        
        fig = px.line(x=hourly_counts.index, y=hourly_counts.values,
                     title=f"{company_name} Activity by Hour of Day",
                     labels={'x': 'Hour', 'y': 'Event Count'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Daily aggregation
        daily_counts = data.groupby(data[time_col].dt.date).size()
        fig = px.line(x=daily_counts.index, y=daily_counts.values,
                     title=f"{company_name} Daily Activity Trend",
                     labels={'x': 'Date', 'y': 'Event Count'})
        st.plotly_chart(fig, use_container_width=True)
        
def create_distribution_charts(data, company_name):
    """Create distribution analysis charts"""
    st.markdown(f"### 🥧 {company_name} Distribution Analysis")
    
    # Find numerical columns
    numeric_cols = data.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 0:
        selected_col = st.selectbox("Choose column for distribution:", numeric_cols)
        
        col1, col2 = st.columns(2)
        with col1:
            # Histogram
            fig = px.histogram(data, x=selected_col, nbins=50,
                             title=f"Distribution of {selected_col}")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Box plot
            fig = px.box(data, y=selected_col,
                        title=f"Box Plot of {selected_col}")
            st.plotly_chart(fig, use_container_width=True)

def create_event_analysis_charts(data, company_name):
    """Create event-specific analysis charts"""
    st.markdown(f"### 📊 {company_name} Event Analysis")
    
    if 'event_type' in data.columns:
        event_counts = data['event_type'].value_counts()
        
        col1, col2 = st.columns(2)
        with col1:
            # Event type pie chart
            fig = px.pie(values=event_counts.values, names=event_counts.index,
                        title="Event Type Distribution")
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Event type bar chart
            fig = px.bar(x=event_counts.index, y=event_counts.values,
                        title="Event Frequency")
            st.plotly_chart(fig, use_container_width=True)

def create_heatmap_charts(data, company_name):
    """Create heatmap analysis charts"""
    st.markdown(f"### 🔥 {company_name} Heatmap Analysis")
    
    # Find timestamp and categorical columns
    time_cols = [col for col in data.columns if 'timestamp' in col or col.endswith('_ts')]
    
    if time_cols:
        time_col = time_cols[0]
        data[time_col] = pd.to_datetime(data[time_col])
        data['hour'] = data[time_col].dt.hour
        data['day_of_week'] = data[time_col].dt.day_name()
        
        # Create hour vs day of week heatmap
        pivot_data = data.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)
        
        fig = px.imshow(pivot_data, 
                       title=f"{company_name} Activity Heatmap (Day vs Hour)",
                       labels={'x': 'Hour of Day', 'y': 'Day of Week', 'color': 'Event Count'},
                       aspect='auto')
        st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# MODULE 1: INGESTION - REALISTIC SYNTHETIC DATA GENERATORS
# ============================================================================

@st.cache_data
def generate_uber_ingest_events(n_records=5000):
    """Generate Uber ingestion events per Module 1 specifications"""
    np.random.seed(42)
    
    # Generate timestamps over last 90 days with exponential inter-arrival for streaming
    end_time = datetime.now()
    start_time = end_time - timedelta(days=90)
    
    # Exponential inter-arrival times for streaming simulation
    inter_arrivals = np.random.exponential(scale=30, size=n_records)  # 30 second average
    timestamps = []
    current_time = start_time
    for i in range(n_records):
        timestamps.append(current_time)
        current_time += timedelta(seconds=inter_arrivals[i])
    
    # Dubai coordinates for realistic pickup/dropoff
    dubai_center_lat, dubai_center_lng = 25.2048, 55.2708
    
    data = []
    for i, ts in enumerate(timestamps):
        ride_id = f"ride_{i:06d}"
        event_data = {
            'event_id': f"evt_{i:08d}",
            'ride_id': ride_id,
            'driver_id': f"drv_{np.random.randint(1000, 9999):04d}",
            'rider_id': f"usr_{np.random.randint(10000, 99999):05d}",
            'event_type': np.random.choice(['request', 'accept', 'start', 'end', 'cancel'], 
                                         p=[0.3, 0.25, 0.25, 0.15, 0.05]),
            'pickup_ts': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'dropoff_ts': (ts + timedelta(minutes=np.random.randint(5, 45))).strftime('%Y-%m-%d %H:%M:%S'),
            'pickup_lat': dubai_center_lat + np.random.normal(0, 0.05),
            'pickup_lng': dubai_center_lng + np.random.normal(0, 0.05),
            'dropoff_lat': dubai_center_lat + np.random.normal(0, 0.08),
            'dropoff_lng': dubai_center_lng + np.random.normal(0, 0.08),
            'distance_km': np.random.exponential(scale=8) + 1,  # 1-50km realistic range
            'price_aed': 0,  # Will be calculated
            'payment_method': np.random.choice(['credit_card', 'cash', 'wallet', 'corporate'], 
                                             p=[0.4, 0.3, 0.2, 0.1]),
            'status': np.random.choice(['completed', 'cancelled', 'pending'], p=[0.85, 0.1, 0.05]),
            'ingestion_ts': (ts + timedelta(seconds=np.random.uniform(0, 5))).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Calculate realistic price in AED (5-300 AED range as specified)
        base_fare = 5  # AED
        per_km_rate = 2.5  # AED per km
        surge_multiplier = np.random.choice([1.0, 1.2, 1.5, 2.0], p=[0.7, 0.15, 0.1, 0.05])
        event_data['price_aed'] = round((base_fare + event_data['distance_km'] * per_km_rate) * surge_multiplier, 2)
        
        data.append(event_data)
    
    return pd.DataFrame(data)

@st.cache_data  
def generate_netflix_ingest_events(n_records=5000):
    """Generate Netflix ingestion events per Module 1 specifications"""
    np.random.seed(42)
    
    # Time range
    end_time = datetime.now()
    start_time = end_time - timedelta(days=90)
    timestamps = pd.date_range(start_time, end_time, periods=n_records)
    
    # Content catalog
    content_catalog = [
        'Stranger Things', 'The Crown', 'Ozark', 'Bridgerton', 'Money Heist', 'Squid Game',
        'The Witcher', 'Orange Is the New Black', 'House of Cards', 'Breaking Bad', 'Narcos',
        'The Queen\'s Gambit', 'Lupin', 'Dark', 'Elite', 'Sex Education', 'Mindhunter',
        'Black Mirror', 'Peaky Blinders', 'Better Call Saul'
    ]
    
    device_types = ['smart_tv', 'mobile', 'tablet', 'laptop', 'desktop', 'game_console']
    countries = ['UAE', 'USA', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Spain', 'India', 'Brazil']
    subscription_tiers = ['basic', 'standard', 'premium']
    
    data = []
    for i, ts in enumerate(timestamps):
        data.append({
            'event_id': f"nf_evt_{i:08d}",
            'user_id': f"nf_usr_{np.random.randint(100000, 999999):06d}",
            'device_type': np.random.choice(device_types, p=[0.35, 0.25, 0.15, 0.1, 0.1, 0.05]),
            'content_id': f"cnt_{np.random.choice(range(len(content_catalog))):03d}",
            'content_title': np.random.choice(content_catalog),
            'event_type': np.random.choice(['play', 'pause', 'stop', 'seek', 'resume'], 
                                         p=[0.4, 0.2, 0.15, 0.15, 0.1]),
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_sec': np.random.randint(1, 7200),  # Up to 2 hours
            'bitrate_kbps': np.random.choice([720, 1080, 1440, 2160, 4320], p=[0.3, 0.4, 0.2, 0.08, 0.02]),
            'country': np.random.choice(countries, p=[0.15, 0.25, 0.1, 0.08, 0.07, 0.1, 0.08, 0.07, 0.05, 0.05]),
            'subscription_tier': np.random.choice(subscription_tiers, p=[0.2, 0.5, 0.3])
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_amazon_ingest_events(n_records=5000):
    """Generate Amazon order ingestion events per Module 1 specifications"""
    np.random.seed(42)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=90)
    timestamps = pd.date_range(start_time, end_time, periods=n_records)
    
    channels = ['web', 'mobile_app', 'alexa', 'api', 'marketplace']
    product_categories = ['electronics', 'books', 'clothing', 'home_garden', 'sports', 'beauty', 'toys', 'automotive']
    
    data = []
    for i, ts in enumerate(timestamps):
        order_id = f"amz_order_{i:08d}"
        product_id = f"prod_{np.random.randint(100000, 999999):06d}"
        
        # Realistic pricing in AED (10-5000 AED range as specified)
        unit_price = np.random.exponential(scale=100) + 10
        unit_price = min(unit_price, 5000)  # Cap at 5000 AED
        quantity = np.random.choice([1, 2, 3, 4, 5], p=[0.6, 0.2, 0.1, 0.07, 0.03])
        
        data.append({
            'event_id': f"amz_evt_{i:08d}",
            'order_id': order_id,
            'customer_id': f"cust_{np.random.randint(100000, 999999):06d}",
            'product_id': product_id,
            'event_type': np.random.choice(['created', 'paid', 'shipped', 'delivered', 'returned'], 
                                         p=[0.25, 0.23, 0.22, 0.25, 0.05]),
            'quantity': quantity,
            'unit_price_aed': round(unit_price, 2),
            'total_price_aed': round(unit_price * quantity, 2),
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'channel': np.random.choice(channels, p=[0.35, 0.3, 0.1, 0.15, 0.1]),
            'product_category': np.random.choice(product_categories)
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_airbnb_ingest_events(n_records=5000):
    """Generate Airbnb booking ingestion events per Module 1 specifications"""
    np.random.seed(42)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=90)
    timestamps = pd.date_range(start_time, end_time, periods=n_records)
    
    cities = ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Ras Al Khaimah', 'Fujairah', 'Umm Al Quwain']
    property_types = ['apartment', 'villa', 'hotel_room', 'entire_home', 'shared_room']
    
    data = []
    for i, ts in enumerate(timestamps):
        booking_id = f"bnb_book_{i:08d}"
        
        # Realistic pricing (150-2500 AED per night as specified)
        base_price = np.random.exponential(scale=400) + 150
        price_per_night = min(base_price, 2500)
        nights = np.random.randint(1, 15)
        
        checkin = ts.date()
        checkout = checkin + timedelta(days=nights)
        
        data.append({
            'event_id': f"bnb_evt_{i:08d}",
            'booking_id': booking_id,
            'host_id': f"host_{np.random.randint(10000, 99999):05d}",
            'guest_id': f"guest_{np.random.randint(100000, 999999):06d}",
            'property_id': f"prop_{np.random.randint(10000, 99999):05d}",
            'event_type': np.random.choice(['requested', 'confirmed', 'cancelled', 'checked_in', 'checked_out'], 
                                         p=[0.3, 0.25, 0.1, 0.2, 0.15]),
            'checkin': checkin.strftime('%Y-%m-%d'),
            'checkout': checkout.strftime('%Y-%m-%d'),
            'price_per_night_aed': round(price_per_night, 2),
            'total_price_aed': round(price_per_night * nights, 2),
            'nights': nights,
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'city': np.random.choice(cities),
            'property_type': np.random.choice(property_types, p=[0.4, 0.25, 0.15, 0.15, 0.05])
        })
    
    return pd.DataFrame(data)

@st.cache_data
def generate_nyse_ingest_ticks(n_records=10000):
    """Generate NYSE tick ingestion events per Module 1 specifications (high-frequency)"""
    np.random.seed(42)
    
    # High frequency - millisecond precision
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=8)  # Trading day
    
    # Generate microsecond timestamps
    total_ms = int((end_time - start_time).total_seconds() * 1000)
    timestamps_ms = sorted(np.random.choice(total_ms, n_records, replace=False))
    
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JPM', 'JNJ', 'V']
    exchanges = ['NYSE', 'NASDAQ', 'BATS', 'IEX']
    trade_types = ['buy', 'sell']
    
    # Base prices for each ticker (realistic ranges)
    base_prices = {
        'AAPL': 175, 'MSFT': 380, 'GOOGL': 2800, 'AMZN': 3200, 'TSLA': 800,
        'NVDA': 450, 'META': 320, 'JPM': 150, 'JNJ': 160, 'V': 240
    }
    
    data = []
    for i, ts_ms in enumerate(timestamps_ms):
        ticker = np.random.choice(tickers)
        base_price = base_prices[ticker]
        
        # Add realistic price movement
        price_change = np.random.normal(0, base_price * 0.001)  # 0.1% volatility
        current_price = base_price + price_change
        
        trade_time = start_time + timedelta(milliseconds=int(ts_ms))
        
        data.append({
            'tick_id': f"tick_{i:010d}",
            'ticker': ticker,
            'trade_ts': trade_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # Millisecond precision
            'price': round(current_price, 2),
            'size': np.random.randint(100, 50000),  # Share volume
            'trade_type': np.random.choice(trade_types),
            'exchange': np.random.choice(exchanges, p=[0.4, 0.35, 0.15, 0.1]),
            'order_id': f"ord_{i:010d}"
        })
    
    return pd.DataFrame(data)

# ============================================================================
# SINGLE EVENT GENERATORS FOR STREAMING SIMULATION
# ============================================================================

def generate_single_uber_event(event_id):
    """Generate a single Uber event for streaming simulation"""
    dubai_center_lat, dubai_center_lng = 25.2048, 55.2708
    
    event_data = {
        'event_id': f"evt_{event_id:08d}",
        'ride_id': f"ride_{np.random.randint(100000, 999999):06d}",
        'driver_id': f"drv_{np.random.randint(1000, 9999):04d}",
        'rider_id': f"usr_{np.random.randint(10000, 99999):05d}",
        'event_type': np.random.choice(['request', 'accept', 'start', 'end', 'cancel'], 
                                     p=[0.3, 0.25, 0.25, 0.15, 0.05]),
        'pickup_lat': dubai_center_lat + np.random.normal(0, 0.05),
        'pickup_lng': dubai_center_lng + np.random.normal(0, 0.05),
        'price_aed': round(np.random.exponential(scale=50) + 10, 2),
        'payment_method': np.random.choice(['credit_card', 'cash', 'wallet']),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    }
    return event_data

def generate_single_netflix_event(event_id):
    """Generate a single Netflix event for streaming simulation"""
    content_titles = ['Stranger Things', 'The Crown', 'Squid Game', 'Wednesday', 'Ozark']
    
    return {
        'event_id': f"nf_evt_{event_id:08d}",
        'user_id': f"nf_usr_{np.random.randint(100000, 999999):06d}",
        'device_type': np.random.choice(['smart_tv', 'mobile', 'tablet', 'laptop']),
        'content_title': np.random.choice(content_titles),
        'event_type': np.random.choice(['play', 'pause', 'stop', 'seek', 'resume']),
        'duration_sec': np.random.randint(1, 7200),
        'country': np.random.choice(['UAE', 'USA', 'UK', 'Canada']),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    }

def generate_single_amazon_event(event_id):
    """Generate a single Amazon event for streaming simulation"""
    return {
        'event_id': f"amz_evt_{event_id:08d}",
        'order_id': f"amz_order_{np.random.randint(10000000, 99999999):08d}",
        'customer_id': f"cust_{np.random.randint(100000, 999999):06d}",
        'product_id': f"prod_{np.random.randint(100000, 999999):06d}",
        'event_type': np.random.choice(['created', 'paid', 'shipped', 'delivered']),
        'total_price_aed': round(np.random.exponential(scale=100) + 10, 2),
        'channel': np.random.choice(['web', 'mobile_app', 'api']),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    }

def generate_single_airbnb_event(event_id):
    """Generate a single Airbnb event for streaming simulation"""
    return {
        'event_id': f"bnb_evt_{event_id:08d}",
        'booking_id': f"bnb_book_{np.random.randint(10000000, 99999999):08d}",
        'host_id': f"host_{np.random.randint(10000, 99999):05d}",
        'guest_id': f"guest_{np.random.randint(100000, 999999):06d}",
        'event_type': np.random.choice(['requested', 'confirmed', 'cancelled']),
        'price_per_night_aed': round(np.random.exponential(scale=400) + 150, 2),
        'city': np.random.choice(['Dubai', 'Abu Dhabi', 'Sharjah']),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    }

def generate_single_nyse_event(event_id):
    """Generate a single NYSE tick for streaming simulation"""
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    ticker = np.random.choice(tickers)
    base_prices = {'AAPL': 175, 'MSFT': 380, 'GOOGL': 2800, 'AMZN': 3200, 'TSLA': 800}
    
    return {
        'tick_id': f"tick_{event_id:010d}",
        'ticker': ticker,
        'price': round(base_prices[ticker] + np.random.normal(0, base_prices[ticker] * 0.001), 2),
        'size': np.random.randint(100, 10000),
        'trade_type': np.random.choice(['buy', 'sell']),
        'exchange': np.random.choice(['NYSE', 'NASDAQ']),
        'trade_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    }

@st.cache_data
def generate_netflix_data():
    """Generate realistic Netflix viewership data"""
    np.random.seed(42)
    n_records = 10000
    
    titles = ['Stranger Things', 'The Crown', 'Ozark', 'Bridgerton', 'Money Heist', 'Squid Game',
              'The Witcher', 'Orange Is the New Black', 'House of Cards', 'Breaking Bad']
    
    genres = ['Drama', 'Comedy', 'Action', 'Documentary', 'Horror', 'Romance', 'Thriller']
    devices = ['Smart TV', 'Mobile', 'Laptop', 'Tablet', 'Gaming Console']
    regions = ['US', 'EU', 'APAC', 'LATAM', 'Canada']
    
    return pd.DataFrame({
        'view_id': range(1, n_records + 1),
        'user_id': np.random.randint(100000, 999999, n_records),
        'title': np.random.choice(titles, n_records),
        'genre': np.random.choice(genres, n_records),
        'watch_duration_min': np.random.lognormal(3.5, 0.8, n_records).astype(int),
        'completion_rate': np.random.beta(2, 2, n_records).round(2),
        'device_type': np.random.choice(devices, n_records),
        'region': np.random.choice(regions, n_records, p=[0.35, 0.25, 0.2, 0.15, 0.05]),
        'timestamp': pd.date_range('2024-01-01', periods=n_records, freq='5min'),
        'rating': np.random.choice([1, 2, 3, 4, 5], n_records, p=[0.05, 0.1, 0.15, 0.35, 0.35]),
        'subscription_type': np.random.choice(['Basic', 'Standard', 'Premium'], n_records, p=[0.3, 0.4, 0.3])
    })

@st.cache_data  
def generate_amazon_data():
    """Generate realistic Amazon sales data"""
    np.random.seed(43)
    n_records = 10000
    
    categories = ['Electronics', 'Books', 'Clothing', 'Home & Kitchen', 'Sports', 'Beauty', 'Toys']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Amazon Pay', 'Gift Card']
    shipping_speeds = ['Standard', 'Prime', 'Next Day', '2-Day']
    
    return pd.DataFrame({
        'order_id': range(1, n_records + 1),
        'customer_id': np.random.randint(10000, 99999, n_records),
        'product_category': np.random.choice(categories, n_records),
        'order_value': np.random.lognormal(4, 0.8, n_records).round(2),
        'quantity': np.random.poisson(2, n_records) + 1,
        'payment_method': np.random.choice(payment_methods, n_records),
        'shipping_speed': np.random.choice(shipping_speeds, n_records, p=[0.3, 0.4, 0.15, 0.15]),
        'prime_member': np.random.choice([True, False], n_records, p=[0.6, 0.4]),
        'order_date': pd.date_range('2024-01-01', periods=n_records, freq='3min'),
        'delivery_days': np.random.choice([1, 2, 3, 5, 7], n_records, p=[0.15, 0.25, 0.25, 0.25, 0.1]),
        'customer_satisfaction': np.random.choice([1, 2, 3, 4, 5], n_records, p=[0.05, 0.1, 0.15, 0.4, 0.3]),
        'region': np.random.choice(['North America', 'Europe', 'Asia', 'Other'], n_records, p=[0.5, 0.25, 0.2, 0.05])
    })

@st.cache_data
def generate_uber_data():
    """Generate realistic Uber ride data"""
    np.random.seed(44)
    n_records = 10000
    
    ride_types = ['UberX', 'UberXL', 'UberPool', 'UberBlack', 'UberEats']
    cities = ['New York', 'Los Angeles', 'Chicago', 'San Francisco', 'Boston', 'Seattle']
    payment_methods = ['Credit Card', 'PayPal', 'Cash', 'Uber Cash']
    
    return pd.DataFrame({
        'ride_id': range(1, n_records + 1),
        'driver_id': np.random.randint(1000, 9999, n_records),
        'rider_id': np.random.randint(10000, 99999, n_records),
        'ride_type': np.random.choice(ride_types, n_records, p=[0.4, 0.15, 0.2, 0.1, 0.15]),
        'city': np.random.choice(cities, n_records),
        'distance_miles': np.random.exponential(5, n_records).round(1),
        'duration_minutes': np.random.exponential(15, n_records).astype(int) + 5,
        'fare_amount': np.random.lognormal(2.5, 0.6, n_records).round(2),
        'tip_amount': np.random.exponential(2, n_records).round(2),
        'payment_method': np.random.choice(payment_methods, n_records),
        'rider_rating': np.random.choice([3, 4, 5], n_records, p=[0.1, 0.3, 0.6]),
        'driver_rating': np.random.choice([3, 4, 5], n_records, p=[0.15, 0.35, 0.5]),
        'pickup_time': pd.date_range('2024-01-01', periods=n_records, freq='2min'),
        'surge_multiplier': np.random.choice([1.0, 1.2, 1.5, 2.0, 2.5], n_records, p=[0.6, 0.2, 0.1, 0.08, 0.02])
    })

@st.cache_data
def generate_nyse_data():
    """Generate realistic NYSE trading data"""
    np.random.seed(45)
    n_records = 10000
    
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'DIS', 'JPM']
    sectors = ['Technology', 'Healthcare', 'Financial', 'Consumer', 'Industrial', 'Energy']
    
    base_prices = {
        'AAPL': 180, 'GOOGL': 140, 'MSFT': 380, 'AMZN': 150, 'TSLA': 250,
        'META': 350, 'NVDA': 800, 'NFLX': 400, 'DIS': 100, 'JPM': 150
    }
    
    data = []
    for i in range(n_records):
        symbol = np.random.choice(symbols)
        base_price = base_prices[symbol]
        price_change = np.random.normal(0, base_price * 0.02)
        
        data.append({
            'trade_id': i + 1,
            'symbol': symbol,
            'sector': np.random.choice(sectors),
            'price': round(base_price + price_change, 2),
            'volume': np.random.poisson(1000) * 100,
            'trade_type': np.random.choice(['Buy', 'Sell'], p=[0.52, 0.48]),
            'timestamp': pd.date_range('2024-01-01 09:30:00', periods=n_records, freq='10s')[i],
            'market_cap_billion': np.random.exponential(500, 1)[0].round(1),
            'pe_ratio': np.random.gamma(2, 10, 1)[0].round(1),
            'dividend_yield': np.random.exponential(2, 1)[0].round(2),
            'day_change_pct': np.random.normal(0, 2, 1)[0].round(2)
        })
    
    return pd.DataFrame(data)

def main():
    st.title("🏗️ Data Architecture & Engineering Learning Hub")
    st.markdown("---")
    
    # Sidebar navigation
    st.sidebar.title("📚 Learning Modules")
    
    modules = [
        "🏠 Home",
        "📥 Data Ingestion", 
        "💾 Data Storage",
        "🔄 ETL/ELT Pipelines",
        "⚡ Processing Systems",
        "📊 Big Data & Scaling",
        "🔍 OLAP vs OLTP",
        "🧠 Data Science & Analytics",
        "📊 Control and Logs"
    ]
    
    # Company case studies section
    st.sidebar.markdown("---")
    st.sidebar.title("🏢 Company Case Studies")
    
    companies = [
        "🛒 Amazon (E-commerce)",
        "🎬 Netflix (Streaming)", 
        "💰 NYSE (Financial)",
        "🏠 Airbnb (Marketplace)",
        "🚗 Uber (Mobility)"
    ]
    
    # Initialize session state for selected module if not exists
    if 'selected_module' not in st.session_state:
        st.session_state.selected_module = "🏠 Home"
    
    # Create buttons for each module
    st.sidebar.markdown("**Choose a module:**")
    for module in modules:
        if st.sidebar.button(module, key=f"btn_{module}", use_container_width=True):
            st.session_state.selected_module = module
    
    selected_module = st.session_state.selected_module
    selected_company = st.sidebar.selectbox("Choose a company case study:", ["Select a company..."] + companies)
    
    # Main content area
    if selected_module == "🏠 Home":
        log_activity("INFO", "Navigation", "User accessed Home module")
        show_home()
    elif selected_module == "📥 Data Ingestion":
        log_activity("INFO", "Navigation", "User accessed Data Ingestion module")
        show_data_ingestion()
    elif selected_module == "💾 Data Storage":
        log_activity("INFO", "Navigation", "User accessed Data Storage module")
        show_data_storage()
    elif selected_module == "🔄 ETL/ELT Pipelines":
        log_activity("INFO", "Navigation", "User accessed ETL/ELT Pipelines module")
        show_etl_pipelines()
    elif selected_module == "⚡ Processing Systems":
        log_activity("INFO", "Navigation", "User accessed Processing Systems module")
        show_processing_systems()
    elif selected_module == "📊 Big Data & Scaling":
        log_activity("INFO", "Navigation", "User accessed Big Data & Scaling module")
        show_big_data_scaling()
    elif selected_module == "🔍 OLAP vs OLTP":
        log_activity("INFO", "Navigation", "User accessed OLAP vs OLTP module")
        show_olap_vs_oltp()
    elif selected_module == "🧠 Data Science & Analytics":
        log_activity("INFO", "Navigation", "User accessed Data Science & Analytics module")
        show_data_science_analytics()
    elif selected_module == "📊 Control and Logs":
        show_control_and_logs()
    
    # Show company case study if selected
    if selected_company != "Select a company...":
        show_company_case_study(selected_company)

def show_home():
    st.header("Welcome to the Data Architecture & Engineering Learning Hub! 🎉")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 What You'll Learn")
        st.markdown("""
        - **Data Architecture Fundamentals**: Understanding data models, schemas, and architectural patterns
        - **Data Engineering**: ETL/ELT processes, data pipelines, and workflow orchestration
        - **Storage Systems**: Databases, data lakes, data warehouses, and modern storage solutions
        - **Processing Frameworks**: Batch and stream processing technologies
        - **Data Quality**: Monitoring, validation, and governance practices
        - **Cloud Platforms**: AWS, Azure, GCP data services
        """)
    
    with col2:
        st.subheader("🛠️ Interactive Features")
        st.markdown("""
        - **Live Simulations**: Visualize data flow through pipelines
        - **Practice Datasets**: Real-world data for hands-on practice
        - **Interactive Quizzes**: Test your knowledge as you learn
        - **Architecture Diagrams**: Visual representations of systems
        - **Code Examples**: Python, SQL, and configuration samples
        - **Best Practices**: Industry standards and recommendations
        """)
    
    st.markdown("---")
    st.subheader("🚀 Get Started")
    st.markdown("Use the sidebar to navigate through different learning modules. Each module builds upon the previous ones, so we recommend following the order for the best learning experience.")
    
    # Progress tracking placeholder
    st.subheader("📈 Your Progress")
    progress_data = {
        'Module': ['Data Architecture', 'Data Engineering', 'Pipeline Simulations', 'Practice Datasets', 'Exercises'],
        'Progress': [0, 0, 0, 0, 0]
    }
    progress_df = pd.DataFrame(progress_data)
    
    fig = px.bar(progress_df, x='Module', y='Progress', title='Learning Progress')
    fig.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig, use_container_width=True)

def show_data_ingestion():
    st.header("📥 Module 1: Data Ingestion (Batch & Streaming)")
    st.markdown("""
    **Purpose:** High-cardinality event-level records to simulate ingestion pipelines.
    Explore realistic synthetic datasets for Uber, Netflix, Amazon, Airbnb, and NYSE with interactive EDA.
    """)
    
    # Company selection
    company = st.selectbox(
        "🏢 Choose Company Dataset:",
        ["🚗 Uber (Ride Events)", "🎬 Netflix (Streaming)", "🛒 Amazon (Orders)", 
         "🏠 Airbnb (Bookings)", "💰 NYSE (Trading)"]
    )
    
    # Create tabs based on company selection
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 EDA Charts", "🛠️ Interactive Demo", "📋 Raw Data", "⚙️ Technical Stack", "📚 Schema Info"])
    
    # Initialize Module 1 SQLite database
    module1_conn = init_module1_database()
    
    # Determine company details
    if "Uber" in company:
        company_name = "Uber"
        currency = "AED"
    elif "Netflix" in company:
        company_name = "Netflix"
        currency = "USD"
    elif "Amazon" in company:
        company_name = "Amazon"
        currency = "AED"
    elif "Airbnb" in company:
        company_name = "Airbnb"
        currency = "AED"
    else:  # NYSE
        company_name = "NYSE"
        currency = "USD"
    
    # Populate database with synthetic data if not exists
    populate_module1_data(module1_conn, company_name)
    
    # Load data from SQLite database
    data = load_module1_data_from_db(module1_conn, company_name)
    
    with tab1:
        st.subheader(f"📊 EDA Analysis - {company_name} Dataset")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total Records", f"{len(data):,}")
        with col2:
            st.metric("📅 Date Range", f"{len(data.columns[data.columns.str.contains('timestamp|ts|_ts', case=False)])} time cols")
        with col3:
            if company_name in ['Uber', 'Amazon', 'Airbnb']:
                revenue_col = [col for col in data.columns if 'price_aed' in col or 'total_price_aed' in col]
                if revenue_col:
                    total_revenue = data[revenue_col[0]].sum()
                    st.metric("💰 Total Revenue", f"{total_revenue:,.2f} {currency}")
            elif company_name == 'NYSE':
                avg_price = data['price'].mean()
                st.metric("💵 Avg Price", f"${avg_price:.2f}")
            else:
                st.metric("🎯 Events", f"{data['event_type'].nunique()} types")
        
        # Chart selection
        chart_type = st.selectbox(
            "Choose Chart Type:",
            ["📊 Overview Dashboard", "📈 Time Series", "🥧 Distribution", "📊 Event Analysis", "🔥 Heatmaps"]
        )
        
        if chart_type == "📊 Overview Dashboard":
            create_overview_dashboard(data, company_name)
        elif chart_type == "📈 Time Series":
            create_time_series_charts(data, company_name)
        elif chart_type == "🥧 Distribution": 
            create_distribution_charts(data, company_name)
        elif chart_type == "📊 Event Analysis":
            create_event_analysis_charts(data, company_name)
        else:  # Heatmaps
            create_heatmap_charts(data, company_name)
    
    with tab2:
        st.subheader(f"🛠️ {company_name} Interactive Streaming Simulation")
        
        st.markdown(f"""
        **Simulate real-time {company_name} event ingestion** with realistic inter-arrival times 
        following exponential distribution for streaming patterns.
        """)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            stream_duration = st.slider("Stream Duration (seconds)", 5, 30, 10)
        with col2:
            events_per_second = st.slider("Events/Second", 1, 10, 3)
        with col3:
            show_json = st.checkbox("Show JSON Format", value=True)
        
        if st.button(f"🚀 Start {company_name} Stream"):
            st.markdown("### 📊 Live Event Stream")
            
            # Create placeholder for streaming data
            placeholder = st.empty()
            metrics_placeholder = st.empty()
            
            events_processed = 0
            start_time = time.time()
            
            for i in range(stream_duration * events_per_second):
                # Generate single event based on company
                if company_name == "Uber":
                    event = generate_single_uber_event(i)
                elif company_name == "Netflix":
                    event = generate_single_netflix_event(i)
                elif company_name == "Amazon":
                    event = generate_single_amazon_event(i)
                elif company_name == "Airbnb":
                    event = generate_single_airbnb_event(i)
                else:  # NYSE
                    event = generate_single_nyse_event(i)
                
                events_processed += 1
                elapsed_time = time.time() - start_time
                
                with placeholder.container():
                    if show_json:
                        st.json(event)
                    else:
                        st.write(f"**Event {i+1}:** {event.get('event_type', 'tick')} - {event.get('timestamp', event.get('trade_ts'))}")
                
                with metrics_placeholder.container():
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Events Processed", events_processed)
                    with col2:
                        st.metric("Events/Second", f"{events_processed/elapsed_time:.1f}")
                    with col3:
                        st.metric("Elapsed Time", f"{elapsed_time:.1f}s")
                
                # Simulate exponential inter-arrival times for streaming
                time.sleep(np.random.exponential(1.0 / events_per_second))
            
            st.success(f"✅ Stream completed! Processed {events_processed} events in {elapsed_time:.1f} seconds")
    
    with tab3:
        st.subheader(f"📋 Raw {company_name} Dataset & SQL Interface")
        
        # Database connection status
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info("✅ **SQLite Database Connected**")
        with col2:
            cursor = module1_conn.cursor()
            cursor.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            st.info(f"🔧 **Journal Mode**: {journal_mode}")
        with col3:
            table_map = {
                'Uber': 'ingest_uber_events',
                'Netflix': 'ingest_netflix_events',
                'Amazon': 'ingest_amazon_orders', 
                'Airbnb': 'ingest_airbnb_bookings',
                'NYSE': 'ingest_nyse_ticks'
            }
            table_name = table_map[company_name]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]
            st.info(f"📊 **DB Records**: {total_records:,}")
        
        # SQL Query Interface
        st.markdown("### 💻 Interactive SQL Query Interface")
        st.markdown(f"""
        **Query the {company_name} ingestion data** directly from SQLite using SQL commands.
        All data is stored with optimized indexes and WAL mode for performance.
        """)
        
        # Pre-built query examples
        query_examples = {
            'Uber': [
                "SELECT event_type, COUNT(*) as count FROM ingest_uber_events GROUP BY event_type",
                "SELECT AVG(price_aed) as avg_fare, AVG(distance_km) as avg_distance FROM ingest_uber_events WHERE event_type = 'end'",
                "SELECT payment_method, SUM(price_aed) as total_revenue FROM ingest_uber_events GROUP BY payment_method ORDER BY total_revenue DESC"
            ],
            'Netflix': [
                "SELECT device_type, COUNT(*) as sessions FROM ingest_netflix_events GROUP BY device_type ORDER BY sessions DESC",
                "SELECT country, COUNT(DISTINCT user_id) as unique_users FROM ingest_netflix_events GROUP BY country ORDER BY unique_users DESC LIMIT 10",
                "SELECT content_title, AVG(duration_sec) as avg_watch_time FROM ingest_netflix_events GROUP BY content_title ORDER BY avg_watch_time DESC LIMIT 10"
            ],
            'Amazon': [
                "SELECT event_type, COUNT(*) as events FROM ingest_amazon_orders GROUP BY event_type",
                "SELECT channel, SUM(total_price_aed) as revenue FROM ingest_amazon_orders GROUP BY channel ORDER BY revenue DESC",
                "SELECT product_category, AVG(unit_price_aed) as avg_price FROM ingest_amazon_orders GROUP BY product_category ORDER BY avg_price DESC"
            ],
            'Airbnb': [
                "SELECT city, COUNT(*) as bookings FROM ingest_airbnb_bookings GROUP BY city ORDER BY bookings DESC",
                "SELECT property_type, AVG(price_per_night_aed) as avg_rate FROM ingest_airbnb_bookings GROUP BY property_type ORDER BY avg_rate DESC",
                "SELECT event_type, COUNT(*) as events FROM ingest_airbnb_bookings GROUP BY event_type"
            ],
            'NYSE': [
                "SELECT ticker, COUNT(*) as trades FROM ingest_nyse_ticks GROUP BY ticker ORDER BY trades DESC",
                "SELECT exchange, AVG(price) as avg_price FROM ingest_nyse_ticks GROUP BY exchange",
                "SELECT ticker, MIN(price) as min_price, MAX(price) as max_price FROM ingest_nyse_ticks GROUP BY ticker"
            ]
        }
        
        # Query selection
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_example = st.selectbox(
                "Choose a sample query:",
                ["Custom Query"] + [f"Example {i+1}" for i in range(len(query_examples[company_name]))]
            )
        with col2:
            execute_query = st.button("🚀 Execute Query", type="primary")
        
        # Query input
        if selected_example == "Custom Query":
            sql_query = st.text_area(
                "Enter your SQL query:",
                value=f"SELECT * FROM {table_name} LIMIT 10",
                height=100,
                help="Query the ingestion data using standard SQL syntax"
            )
        else:
            example_idx = int(selected_example.split()[1]) - 1
            sql_query = query_examples[company_name][example_idx]
            st.code(sql_query, language="sql")
        
        # Execute query
        if execute_query and sql_query.strip():
            try:
                with st.spinner("Executing SQL query..."):
                    query_result = execute_module1_sql_query(module1_conn, sql_query)
                
                st.success(f"✅ Query executed successfully! Returned {len(query_result)} rows.")
                
                # Show query results
                if len(query_result) > 0:
                    st.dataframe(query_result, use_container_width=True)
                    
                    # Query performance metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 Rows Returned", len(query_result))
                    with col2:
                        st.metric("📋 Columns", len(query_result.columns))
                    with col3:
                        memory_mb = query_result.memory_usage(deep=True).sum() / 1024**2
                        st.metric("💾 Result Size", f"{memory_mb:.1f} MB")
                else:
                    st.warning("Query returned no results.")
                    
            except Exception as e:
                st.error(f"❌ SQL Error: {str(e)}")
        
        st.markdown("---")
        
        st.markdown(f"""
        **Raw ingestion data** for {company_name} with {len(data):,} records.
        This represents the high-cardinality event-level data as specified in Module 1.
        """)
        
        # Data filtering options
        col1, col2 = st.columns(2)
        with col1:
            n_rows = st.number_input("Number of rows to display", 10, 1000, 100)
        with col2:
            if 'event_type' in data.columns:
                event_filter = st.multiselect(
                    "Filter by Event Type", 
                    data['event_type'].unique(),
                    default=list(data['event_type'].unique())
                )
                filtered_data = data[data['event_type'].isin(event_filter)] if event_filter else data
            else:
                filtered_data = data
        
        # Display dataset info
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Total Records", len(filtered_data))
        with col2:
            st.metric("📋 Columns", len(filtered_data.columns))
        with col3:
            memory_usage = filtered_data.memory_usage(deep=True).sum() / 1024**2
            st.metric("💾 Memory (MB)", f"{memory_usage:.1f}")
        with col4:
            if 'timestamp' in str(filtered_data.columns).lower():
                time_cols = [col for col in filtered_data.columns if 'timestamp' in col.lower() or 'ts' in col or 'time' in col.lower()]
                if time_cols:
                    time_range = pd.to_datetime(filtered_data[time_cols[0]]).max() - pd.to_datetime(filtered_data[time_cols[0]]).min()
                    st.metric("⏱️ Time Span", f"{time_range.days} days")
        
        # Display sample data
        st.markdown("### 📋 Sample Data")
        st.dataframe(filtered_data.head(n_rows), use_container_width=True)
        
        # Data quality summary
        st.markdown("### 🔍 Data Quality Summary")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Missing Values:**")
            missing_data = filtered_data.isnull().sum()
            missing_df = pd.DataFrame({
                'Column': missing_data.index,
                'Missing Count': missing_data.values,
                'Missing %': (missing_data.values / len(filtered_data) * 100).round(2)
            })
            st.dataframe(missing_df[missing_df['Missing Count'] > 0], use_container_width=True)
            
        with col2:
            st.markdown("**Data Types:**")
            dtype_df = pd.DataFrame({
                'Column': filtered_data.dtypes.index,
                'Data Type': filtered_data.dtypes.values.astype(str)
            })
            st.dataframe(dtype_df, use_container_width=True)
    
    with tab4:
        st.subheader(f"⚙️ {company_name} Technical Stack & Data Flow")
        
        st.markdown(f"""
        **Comprehensive technical architecture** showing how synthetic data flows from customer interactions 
        to ingestion systems for {company_name}. Each component's role is explained in detail.
        """)
        
        # Technical Architecture based on company
        if company_name == "Amazon":
            st.markdown("""
            ### 🛒 **Amazon E-commerce Technical Stack**
            
            #### **Data Flow: Customer → Ingestion**
            """)
            
            # Interactive architecture diagram
            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown("""
                **🔄 Complete Data Journey:**
                
                **1. Customer Interaction Layer**
                ```
                📱 Mobile Apps (iOS/Android)
                🌐 Web Frontend (React/Angular)
                🎙️ Alexa Voice Commerce
                🔗 Partner APIs (Marketplace)
                📊 Internal Systems (Warehouse)
                ```
                
                **2. API Gateway & Load Balancing**
                ```
                ⚡ AWS API Gateway
                 ├─ Authentication (Cognito)
                 ├─ Rate Limiting (5000 req/sec)
                 ├─ Request Validation
                 └─ Route to Services
                ```
                
                **3. Ingestion Pipeline**
                ```
                📥 Amazon Kinesis Data Streams
                 ├─ Partition Key: customer_id
                 ├─ Shards: 100 (auto-scaling)
                 ├─ Retention: 24 hours
                 └─ Throughput: 1MB/sec per shard
                ```
                """)
            
            with col2:
                st.markdown("""
                **4. Stream Processing**
                ```
                ⚙️ AWS Lambda Functions
                 ├─ Order validation
                 ├─ Inventory checks
                 ├─ Price calculations
                 └─ Event enrichment
                
                🔄 Kinesis Analytics
                 ├─ Real-time aggregations
                 ├─ Fraud detection rules
                 ├─ Recommendation triggers
                 └─ Alert generation
                ```
                
                **5. Storage Layer**
                ```
                🗄️ Primary Storage
                 ├─ DynamoDB (OLTP) - Orders
                 ├─ RDS MySQL - Customer profiles
                 └─ S3 - Raw event logs
                
                📊 Analytics Storage
                 ├─ Redshift - OLAP queries
                 ├─ ElasticSearch - Search
                 └─ S3 Data Lake - Archives
                ```
                """)
            
            st.markdown("""
            #### **🔧 Technical Components Explained**
            """)
            
            # Component explanations
            with st.expander("📱 **Customer Touch Points** - How data enters the system"):
                st.markdown("""
                **Mobile Apps**: Native iOS/Android apps using REST APIs
                - **Events Generated**: `app_launch`, `product_view`, `add_to_cart`, `checkout_start`
                - **Data Frequency**: 50-100 events per user session
                - **Payload Size**: 1-5KB per event with user context
                
                **Web Frontend**: React/Angular SPAs with real-time updates
                - **Events Generated**: `page_view`, `search_query`, `filter_applied`, `purchase`
                - **Technology**: WebSocket connections for real-time cart updates
                - **Session Tracking**: User journey analytics with 5-minute timeout
                
                **Alexa Integration**: Voice commerce with natural language processing
                - **Events Generated**: `voice_search`, `voice_order`, `voice_tracking`
                - **Processing**: Automatic Speech Recognition → Intent Classification → Order Processing
                - **Latency Requirement**: <500ms for voice response
                
                **Partner APIs**: Third-party seller integration
                - **Events Generated**: `inventory_update`, `price_change`, `new_listing`
                - **Authentication**: OAuth 2.0 with scoped permissions
                - **Rate Limits**: 1000 requests/minute per partner
                """)
            
            with st.expander("⚡ **AWS API Gateway** - Traffic management and security"):
                st.markdown("""
                **Request Processing Pipeline**:
                1. **SSL Termination**: All traffic encrypted with TLS 1.3
                2. **Authentication**: JWT token validation via Cognito
                3. **Authorization**: Role-based access control (RBAC)
                4. **Rate Limiting**: Prevent abuse with per-client quotas
                5. **Request Transformation**: JSON schema validation
                6. **Routing**: Intelligent load balancing to backend services
                
                **Performance Characteristics**:
                - **Latency**: <50ms P99 for API processing
                - **Throughput**: 10,000 concurrent connections
                - **Availability**: 99.95% SLA with multi-AZ deployment
                - **Monitoring**: CloudWatch metrics with custom dashboards
                
                **Error Handling**:
                - **Circuit Breaker**: Fail fast when services are down
                - **Retry Logic**: Exponential backoff with jitter
                - **Dead Letter Queue**: Store failed requests for replay
                """)
            
            with st.expander("📥 **Amazon Kinesis** - Real-time data streaming"):
                st.markdown("""
                **Stream Architecture**:
                - **Sharding Strategy**: Hash partition by `customer_id` for even distribution
                - **Retention Policy**: 24-hour retention for replay capability
                - **Scaling**: Auto-scaling based on incoming data rate
                - **Monitoring**: Real-time metrics on throughput and latency
                
                **Data Processing**:
                ```python
                # Example event structure
                {
                  "event_id": "amz_evt_00001234",
                  "customer_id": "cust_567890",
                  "event_type": "order_created",
                  "timestamp": "2024-08-30T16:45:00Z",
                  "payload": {
                    "order_id": "amz_order_12345678",
                    "total_price_aed": 251.00,
                    "items": [...],
                    "shipping_address": {...}
                  }
                }
                ```
                
                **Consumer Groups**:
                - **Real-time Analytics**: Lambda functions for immediate processing
                - **Data Warehouse**: Kinesis Firehose to Redshift
                - **Search Index**: ElasticSearch for product discovery
                - **ML Pipeline**: SageMaker for recommendation training
                """)
            
            with st.expander("🗄️ **Storage Systems** - Multi-model data persistence"):
                st.markdown("""
                **DynamoDB (OLTP)**:
                - **Use Case**: Real-time order processing and customer sessions
                - **Partition Key**: `customer_id` for even distribution
                - **Sort Key**: `timestamp` for chronological ordering
                - **Capacity**: On-demand scaling with burst capability
                - **Consistency**: Eventually consistent reads, strongly consistent writes
                
                **RDS MySQL (OLTP)**:
                - **Use Case**: Customer profiles, product catalog, inventory
                - **Instance Type**: r5.2xlarge with Multi-AZ deployment
                - **Backup Strategy**: Automated daily backups with 7-day retention
                - **Read Replicas**: 3 read replicas for analytical queries
                
                **Amazon S3 Data Lake**:
                - **Raw Data**: Partitioned by year/month/day/hour
                - **Storage Classes**: Intelligent tiering for cost optimization
                - **Lifecycle Policy**: Archive to Glacier after 90 days
                - **Access Patterns**: Athena for ad-hoc queries, Glue for ETL
                
                **Redshift Data Warehouse (OLAP)**:
                - **Cluster**: 8-node dc2.large cluster with columnar storage
                - **Data Distribution**: Sort and distribution keys for query optimization
                - **Vacuum**: Automated maintenance for optimal performance
                - **Workload Management**: Query queues for different user types
                """)
            
        elif company_name == "Netflix":
            st.markdown("""
            ### 🎬 **Netflix Streaming Technical Stack**
            
            #### **Data Flow: Viewer → Content Analytics**
            """)
            
            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown("""
                **🔄 Streaming Data Journey:**
                
                **1. Client Applications**
                ```
                📺 Smart TV Apps (Roku, Samsung, LG)
                📱 Mobile Apps (iOS, Android)
                💻 Web Players (HTML5 Video)
                🎮 Gaming Consoles (PS5, Xbox)
                ```
                
                **2. CDN & Video Delivery**
                ```
                🌐 Netflix Open Connect CDN
                 ├─ Edge Servers (1000+ locations)
                 ├─ Video Transcoding (H.264, HEVC)
                 ├─ Adaptive Bitrate Streaming
                 └─ Quality Metrics Collection
                ```
                
                **3. Event Collection**
                ```
                📊 Apache Kafka Clusters
                 ├─ Topics: play-events, ui-events
                 ├─ Partitions: 100 per topic
                 ├─ Replication Factor: 3
                 └─ Retention: 7 days
                ```
                """)
            
            with col2:
                st.markdown("""
                **4. Stream Processing**
                ```
                ⚡ Apache Flink Jobs
                 ├─ Windowed aggregations
                 ├─ Real-time recommendations
                 ├─ Quality of experience metrics
                 └─ Anomaly detection
                
                🔄 Kafka Streams Applications
                 ├─ User session tracking
                 ├─ Content popularity scoring
                 ├─ Geographic viewing patterns
                 └─ A/B test analysis
                ```
                
                **5. Data Storage**
                ```
                🗄️ Multi-tier Storage
                 ├─ Cassandra - User profiles
                 ├─ S3 - Raw viewing logs
                 ├─ Redshift - Analytics warehouse
                 └─ ElasticSearch - Content search
                ```
                """)
            
            st.markdown("""
            #### **🔧 Streaming Components Deep Dive**
            """)
            
            with st.expander("📺 **Client Applications** - Multi-platform streaming"):
                st.markdown("""
                **Smart TV Applications**:
                - **Platforms**: Roku, Samsung Tizen, LG webOS, Android TV
                - **Events**: `play_start`, `pause`, `seek`, `quality_change`, `buffer_event`
                - **Telemetry**: Video quality metrics every 30 seconds
                - **Local Storage**: Download progress for offline viewing
                
                **Mobile Applications** (iOS/Android):
                - **Architecture**: Native apps with embedded video players
                - **Events**: `app_foreground`, `download_start`, `cellular_warning`, `casting_start`
                - **Adaptive Streaming**: Automatic bitrate adjustment based on network conditions
                - **Background Play**: Audio-only mode for mobile data conservation
                
                **Web Players**:
                - **Technology**: HTML5 video with MSE (Media Source Extensions)
                - **Browser Support**: Chrome, Safari, Firefox, Edge optimization
                - **DRM Integration**: Widevine, PlayReady for content protection
                - **Performance**: 60fps playback with hardware acceleration
                """)
            
            with st.expander("🌐 **Netflix Open Connect CDN** - Global content delivery"):
                st.markdown("""
                **Edge Server Network**:
                - **Global Presence**: 1000+ edge servers in 200+ countries
                - **ISP Partnership**: Direct peering with major internet providers
                - **Content Caching**: ML-driven pre-positioning of popular content
                - **Load Balancing**: Geographic and network-aware routing
                
                **Video Processing Pipeline**:
                ```
                Original Content → Multiple Encodings → CDN Distribution
                     ↓              ↓                    ↓
                4K/HDR Source   →  1080p, 720p, 480p  →  Edge Caching
                Audio Tracks    →  Multiple Languages →  Localized Delivery
                Subtitles      →  VTT/SRT Formats    →  Real-time Sync
                ```
                
                **Quality Metrics Collection**:
                - **Rebuffering Events**: Track video stalls and their duration
                - **Startup Time**: Time from play button to first frame
                - **Bitrate History**: Adaptive streaming decisions over time
                - **Error Rates**: Playback failures and their root causes
                """)
            
            with st.expander("📊 **Apache Kafka** - High-throughput event streaming"):
                st.markdown("""
                **Cluster Architecture**:
                - **Brokers**: 50+ Kafka brokers across multiple data centers
                - **Topics**: Organized by event type and geographic region
                - **Partitioning**: Hash partitioning by `user_id` for session affinity
                - **Replication**: 3x replication with rack-aware placement
                
                **Event Schema Evolution**:
                ```json
                {
                  "schema_version": "v2.1",
                  "event_id": "nf_evt_00001234",
                  "user_id": "nf_usr_567890",
                  "session_id": "sess_abc123",
                  "device_info": {
                    "type": "smart_tv",
                    "model": "samsung_tizen_2023",
                    "os_version": "6.5",
                    "app_version": "8.2.1"
                  },
                  "playback_info": {
                    "content_id": "cnt_042",
                    "title": "Stranger Things S4E1",
                    "current_time_sec": 1847,
                    "video_quality": "1080p",
                    "audio_language": "en-US",
                    "subtitle_language": "ar-AE"
                  }
                }
                ```
                
                **Consumer Ecosystem**:
                - **Real-time Recommendations**: 50ms latency for homepage updates
                - **Quality Monitoring**: ISP performance dashboards
                - **Content Analytics**: Popularity trends and viewing patterns
                - **Fraud Detection**: Concurrent streaming limits enforcement
                """)
            
            with st.expander("⚡ **Apache Flink** - Real-time stream processing"):
                st.markdown("""
                **Job Architecture**:
                - **Parallelism**: 1000+ parallel tasks across cluster
                - **Checkpointing**: Exactly-once processing guarantees
                - **State Management**: RocksDB for large state storage
                - **Fault Tolerance**: Automatic recovery from node failures
                
                **Real-time Processing Jobs**:
                
                **1. Personalization Engine**:
                ```scala
                // Real-time recommendation updates
                playEvents
                  .keyBy(_.userId)
                  .window(SlidingTimeWindow.of(Time.minutes(10)))
                  .aggregate(new ViewingPatternAggregator)
                  .map(updateUserProfile)
                  .addSink(cassandraSink)
                ```
                
                **2. Quality of Experience (QoE)**:
                ```scala
                // Detect streaming issues in real-time
                qualityEvents
                  .filter(event => event.rebufferCount > 3)
                  .keyBy(_.contentId)
                  .window(TumblingTimeWindow.of(Time.minutes(5)))
                  .aggregate(new QualityIssueDetector)
                  .addSink(alertingSink)
                ```
                
                **3. Geographic Analytics**:
                - **Regional Popularity**: Content trending by country/city
                - **Network Performance**: ISP quality metrics aggregation
                - **Content Delivery**: CDN performance optimization triggers
                """)
                
        elif company_name == "Uber":
            st.markdown("""
            ### 🚗 **Uber Real-Time Mobility Technical Stack**
            
            #### **Data Flow: Rider/Driver → Surge Pricing**
            """)
            
            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown("""
                **🔄 Real-Time Journey:**
                
                **1. Mobile Applications**
                ```
                📱 Rider App (iOS/Android)
                 ├─ Location Services (GPS)
                 ├─ Trip Requests & Tracking
                 ├─ Payment Integration
                 └─ Rating & Feedback
                
                🚗 Driver App (iOS/Android)
                 ├─ Real-time GPS (2-4 sec)
                 ├─ Trip Acceptance
                 ├─ Navigation Integration
                 └─ Earnings Dashboard
                ```
                
                **2. API Gateway & Services**
                ```
                ⚡ Uber API Gateway
                 ├─ Rate Limiting (city-based)
                 ├─ Authentication (OAuth2)
                 ├─ Load Balancing
                 └─ Circuit Breakers
                ```
                
                **3. Message Queue**
                ```
                📨 Apache Kafka
                 ├─ rider-events topic
                 ├─ driver-location topic
                 ├─ trip-events topic
                 └─ surge-pricing topic
                ```
                """)
            
            with col2:
                st.markdown("""
                **4. Real-Time Processing**
                ```
                ⚡ Apache Flink (Sub-second)
                 ├─ Supply/Demand calculation
                 ├─ ETA estimation
                 ├─ Dynamic pricing
                 └─ Driver matching
                
                🔄 Kafka Streams
                 ├─ Location aggregation
                 ├─ Trip state transitions
                 ├─ Driver availability
                 └─ City-wide analytics
                ```
                
                **5. Storage Systems**
                ```
                💾 Multi-tier Storage
                 ├─ Redis - Real-time cache
                 ├─ Cassandra - Trip history
                 ├─ PostGIS - Geographic data
                 └─ S3 - Raw GPS logs
                ```
                """)
            
            st.markdown("""
            #### **🔧 Mobility Platform Components**
            """)
            
            with st.expander("📱 **Mobile Applications** - Real-time location tracking"):
                st.markdown("""
                **Rider Application Architecture**:
                - **Location Services**: High-accuracy GPS with network assistance
                - **Real-time Updates**: WebSocket connections for live driver tracking
                - **Trip Lifecycle**: `request → match → pickup → dropoff → complete → rate`
                - **Offline Capability**: Cached maps and recent trip history
                
                **Event Generation Patterns**:
                ```json
                // Rider Request Event
                {
                  "event_type": "trip_request",
                  "rider_id": "usr_98765",
                  "pickup_location": {
                    "lat": 25.2048, "lng": 55.2708,
                    "address": "Dubai Mall, Downtown Dubai"
                  },
                  "destination": {
                    "lat": 25.1972, "lng": 55.2744,
                    "address": "Burj Al Arab"
                  },
                  "ride_type": "uberx",
                  "timestamp": "2024-08-30T14:30:00.123Z"
                }
                ```
                
                **Driver Application Features**:
                - **Background Location**: Continuous GPS tracking when online
                - **Trip Management**: Accept/decline requests with smart routing
                - **Earnings Optimization**: Real-time surge area visualization
                - **Navigation**: Integrated turn-by-turn directions with traffic
                
                **Performance Requirements**:
                - **Location Accuracy**: <10 meters for pickup/dropoff matching
                - **Update Frequency**: GPS coordinates every 2-4 seconds
                - **Battery Optimization**: Adaptive location sampling based on movement
                - **Network Resilience**: Offline queuing with eventual consistency
                """)
            
            with st.expander("📨 **Apache Kafka** - High-frequency event streaming"):
                st.markdown("""
                **Topic Architecture**:
                
                **driver-location** (High Volume)
                - **Events/sec**: 50,000+ location updates
                - **Partitioning**: By `driver_id` for consistent ordering
                - **Retention**: 1 hour (for replay during outages)
                - **Compression**: Snappy compression for network efficiency
                
                **trip-events** (Critical Path)
                - **Events**: `request`, `accept`, `start`, `complete`, `cancel`
                - **Partitioning**: By `city_id` for geographic processing
                - **Replication**: 5x replication for fault tolerance
                - **Monitoring**: End-to-end latency tracking
                
                **Real-time Schema Evolution**:
                ```json
                // Driver Location Event (every 2-4 seconds)
                {
                  "driver_id": "drv_5678",
                  "location": {
                    "lat": 25.2048,
                    "lng": 55.2708,
                    "accuracy": 8.5,
                    "bearing": 127.3,
                    "speed_kmh": 42.7
                  },
                  "status": "available|busy|offline",
                  "timestamp": "2024-08-30T14:30:45.678Z",
                  "trip_id": null
                }
                ```
                """)
            
            with st.expander("⚡ **Apache Flink** - Sub-second surge pricing"):
                st.markdown("""
                **Real-time Processing Architecture**:
                
                **Supply-Demand Engine** (< 1 second latency):
                ```scala
                // Calculate real-time supply/demand ratio
                val supplyDemandStream = driverLocations
                  .keyBy(_.cityHex) // H3 geospatial indexing
                  .window(SlidingTimeWindow.of(Time.minutes(5), Time.seconds(30)))
                  .aggregate(new SupplyDemandAggregator)
                  
                val surgeMultiplier = supplyDemandStream
                  .map(calculateSurgeMultiplier)
                  .keyBy(_.cityHex)
                  .process(new SurgeCalculator)
                ```
                
                **Driver Matching Algorithm**:
                ```scala
                // Real-time driver-rider matching
                tripRequests
                  .connect(availableDrivers)
                  .keyBy(_.pickupHex, _.currentHex)
                  .process(new MatchingFunction {
                    override def processElement1(request: TripRequest) = {
                      // Find nearest available drivers
                      // Calculate ETA for each option
                      // Score based on rating, distance, wait time
                      // Send match to highest-scoring driver
                    }
                  })
                ```
                
                **ETA Prediction Pipeline**:
                - **Historical Data**: Traffic patterns by time/day/weather
                - **Real-time Traffic**: Integration with Google/Apple traffic APIs
                - **Machine Learning**: XGBoost models for route optimization
                - **Fallback Logic**: Deterministic calculation when ML unavailable
                
                **Surge Pricing Logic**:
                1. **Geospatial Clustering**: H3 hexagonal grid for city subdivision
                2. **Supply Counting**: Active drivers in each hex every 30 seconds  
                3. **Demand Calculation**: Trip requests + estimated future demand
                4. **Price Multiplier**: `max(1.0, min(5.0, demand/supply * base_multiplier))`
                5. **Smoothing**: Prevent price volatility with exponential moving average
                """)
            
            with st.expander("💾 **Storage Systems** - Multi-modal data persistence"):
                st.markdown("""
                **Redis Cluster** (Sub-millisecond cache):
                - **Driver Locations**: Real-time coordinates for matching
                - **Surge Multipliers**: Current pricing by geographic region  
                - **Session Data**: Active trips and driver availability
                - **Architecture**: 100+ node cluster with consistent hashing
                
                **Cassandra** (Trip history and analytics):
                - **Data Model**: Wide-column design for time-series data
                - **Partition Key**: `(city_id, date)` for even distribution
                - **Clustering Key**: `trip_timestamp` for chronological order
                - **Replication**: RF=3 with multi-datacenter deployment
                
                **PostGIS** (Geospatial operations):
                - **Use Cases**: Geofencing, polygon matching, route calculation
                - **Indexes**: R-tree spatial indexes for fast location queries
                - **Functions**: Distance calculation, point-in-polygon tests
                - **Data**: City boundaries, airport zones, surge pricing areas
                
                **Amazon S3** (Data lake for ML):
                - **Raw GPS Logs**: Partitioned by city/date for efficient querying
                - **Trip Records**: Complete trip history for machine learning
                - **Analytics**: Athena queries for business intelligence
                - **Lifecycle**: Intelligent tiering to Glacier for cost optimization
                """)
                
        elif company_name == "Airbnb":
            st.markdown("""
            ### 🏠 **Airbnb Marketplace Technical Stack**
            
            #### **Data Flow: Guest Search → Host Analytics**
            """)
            
            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown("""
                **🔄 Marketplace Data Journey:**
                
                **1. User Interfaces**
                ```
                🌐 Web Application (React/Redux)
                📱 Mobile Apps (React Native)
                🏠 Host Dashboard (Vue.js)
                🔗 Partner APIs (Channel Manager)
                ```
                
                **2. Search & Discovery**
                ```
                🔍 Elasticsearch Cluster
                 ├─ Property search index
                 ├─ Geographic filtering
                 ├─ Price/availability filters
                 └─ Machine learning ranking
                ```
                
                **3. Message Queue**
                ```
                📨 RabbitMQ + Apache Kafka
                 ├─ booking-events queue
                 ├─ search-events stream
                 ├─ pricing-updates topic
                 └─ review-events queue
                ```
                """)
            
            with col2:
                st.markdown("""
                **4. Workflow Orchestration**
                ```
                🔄 Apache Airflow DAGs
                 ├─ Daily property updates
                 ├─ Pricing optimization
                 ├─ Review sentiment analysis
                 └─ Host payout processing
                
                ⚡ Real-time Processing
                 ├─ Search result ranking
                 ├─ Availability updates
                 ├─ Dynamic pricing
                 └─ Fraud detection
                ```
                
                **5. Data Storage**
                ```
                🗄️ Hybrid Storage
                 ├─ MySQL - Bookings/Users
                 ├─ MongoDB - Property data
                 ├─ S3 - Images/Documents
                 └─ Hive - Analytics warehouse
                ```
                """)
            
            st.markdown("""
            #### **🔧 Marketplace Platform Components**
            """)
            
            with st.expander("🔍 **Search & Discovery Engine** - Property matching"):
                st.markdown("""
                **Elasticsearch Architecture**:
                - **Cluster Size**: 50+ nodes with hot/warm/cold architecture
                - **Index Strategy**: Time-based indices with alias rotation
                - **Sharding**: Geographic sharding for localized searches
                - **Replication**: 2x replication with cross-zone distribution
                
                **Search Query Pipeline**:
                ```json
                // Guest search request
                {
                  "location": "Dubai, UAE",
                  "checkin": "2024-09-01",
                  "checkout": "2024-09-05", 
                  "guests": 4,
                  "filters": {
                    "price_range": [150, 2500],
                    "property_type": ["apartment", "villa"],
                    "amenities": ["wifi", "pool", "parking"]
                  },
                  "sort": "price_asc"
                }
                ```
                
                **Machine Learning Ranking**:
                - **Features**: Historical booking rates, host response time, review scores
                - **Model**: Gradient boosted trees with online learning
                - **Personalization**: User preferences and previous booking behavior
                - **A/B Testing**: Continuous ranking algorithm optimization
                
                **Geographic Indexing**:
                - **Geohash**: Hierarchical location encoding for proximity search
                - **Polygon Matching**: Neighborhood and district boundary detection
                - **Distance Calculation**: Haversine formula for accurate distances
                - **Map Integration**: Google Maps API for location validation
                """)
            
            with st.expander("🔄 **Apache Airflow** - Workflow orchestration"):
                st.markdown("""
                **DAG Architecture**:
                
                **Daily Property Updates**:
                ```python
                @dag(schedule_interval='@daily')
                def property_data_pipeline():
                    
                    extract_listings = PythonOperator(
                        task_id='extract_property_data',
                        python_callable=extract_from_sources
                    )
                    
                    validate_data = DataQualityOperator(
                        task_id='validate_property_data',
                        checks=[
                            {'sql': 'SELECT COUNT(*) FROM properties WHERE price_aed < 0', 'expected': 0},
                            {'sql': 'SELECT COUNT(*) FROM properties WHERE location IS NULL', 'expected': 0}
                        ]
                    )
                    
                    update_elasticsearch = BashOperator(
                        task_id='reindex_properties',
                        bash_command='python scripts/elasticsearch_bulk_update.py'
                    )
                    
                    extract_listings >> validate_data >> update_elasticsearch
                ```
                
                **Pricing Optimization DAG**:
                ```python
                # Daily dynamic pricing updates
                pricing_dag = DAG(
                    'dynamic_pricing',
                    schedule_interval='0 2 * * *',  # 2 AM daily
                    tasks=[
                        'fetch_market_data',
                        'calculate_demand_forecast',
                        'run_pricing_model',
                        'update_property_prices',
                        'notify_hosts_of_changes'
                    ]
                )
                ```
                
                **Review Processing Pipeline**:
                - **Text Extraction**: PDF/image OCR for scanned reviews
                - **Sentiment Analysis**: BERT-based models for emotion detection
                - **Language Detection**: Multi-language support for global reviews
                - **Spam Detection**: ML models to filter fake reviews
                - **Host Notifications**: Automated response suggestions
                """)
            
            with st.expander("📨 **Hybrid Message Systems** - Event-driven architecture"):
                st.markdown("""
                **RabbitMQ (Reliable Messaging)**:
                ```
                Exchange: booking-exchange
                 ├─ booking.created → Host notification service
                 ├─ booking.confirmed → Payment processing
                 ├─ booking.cancelled → Refund workflow  
                 └─ booking.completed → Review request trigger
                
                Exchange: pricing-exchange
                 ├─ price.updated → Search index refresh
                 ├─ availability.changed → Calendar sync
                 └─ promotion.activated → Marketing campaigns
                ```
                
                **Apache Kafka (High-throughput Streaming)**:
                ```json
                // Search event for analytics
                {
                  "event_type": "property_search",
                  "session_id": "sess_abc123",
                  "user_id": "guest_987654",
                  "search_criteria": {
                    "location": "Dubai Marina",
                    "dates": "2024-09-01 to 2024-09-05",
                    "price_filter": "150-500 AED"
                  },
                  "results_count": 247,
                  "filters_applied": ["pool", "sea_view"],
                  "timestamp": "2024-08-30T16:45:00Z"
                }
                ```
                
                **Event Processing Patterns**:
                - **CQRS**: Command-Query Responsibility Segregation for scalability
                - **Event Sourcing**: Immutable event log for audit trails
                - **Saga Pattern**: Distributed transaction management across services
                - **Circuit Breakers**: Fault tolerance with automatic recovery
                """)
            
            with st.expander("🗄️ **Hybrid Storage** - Multi-model data architecture"):
                st.markdown("""
                **MySQL (OLTP Operations)**:
                ```sql
                -- Booking management
                CREATE TABLE bookings (
                    booking_id VARCHAR(20) PRIMARY KEY,
                    guest_id VARCHAR(20) NOT NULL,
                    property_id VARCHAR(20) NOT NULL,
                    checkin_date DATE NOT NULL,
                    checkout_date DATE NOT NULL,
                    total_price_aed DECIMAL(10,2),
                    status ENUM('requested', 'confirmed', 'cancelled', 'completed'),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_guest_bookings (guest_id, created_at),
                    INDEX idx_property_availability (property_id, checkin_date)
                );
                ```
                
                **MongoDB (Semi-structured Property Data)**:
                ```javascript
                // Property document structure
                {
                  "_id": ObjectId("64a1b2c3d4e5f6789012345"),
                  "property_id": "prop_12345",
                  "host_id": "host_56789",
                  "location": {
                    "city": "Dubai",
                    "neighborhood": "Dubai Marina",
                    "coordinates": [25.0772, 55.1343],
                    "address": "Marina Walk, Dubai Marina"
                  },
                  "property_details": {
                    "type": "apartment",
                    "bedrooms": 2,
                    "bathrooms": 2,
                    "max_guests": 6,
                    "square_meters": 95
                  },
                  "amenities": ["wifi", "pool", "gym", "parking", "balcony"],
                  "pricing": {
                    "base_price_aed": 450,
                    "cleaning_fee_aed": 75,
                    "extra_guest_fee_aed": 25
                  },
                  "availability_calendar": {
                    "2024-09": {
                      "available_dates": [1,2,3,5,6,7...],
                      "blocked_dates": [4,8,15,22,29]
                    }
                  }
                }
                ```
                
                **Apache Hive (Analytics Warehouse)**:
                - **Partitioning**: By city and date for efficient queries
                - **File Format**: Parquet with snappy compression
                - **Schema Evolution**: Support for adding new fields over time
                - **Query Engine**: Presto for interactive analytics
                """)
                
        else:  # NYSE
            st.markdown("""
            ### 💰 **NYSE High-Frequency Trading Technical Stack**
            
            #### **Data Flow: Trading Systems → Market Data Distribution**
            """)
            
            col1, col2 = st.columns([1, 1])
            with col1:
                st.markdown("""
                **🔄 Ultra-Low Latency Journey:**
                
                **1. Trading Terminals**
                ```
                💻 Trading Workstations
                 ├─ Order Management Systems
                 ├─ Risk Management Engines
                 ├─ Algorithmic Trading Bots
                 └─ Market Data Displays
                ```
                
                **2. Market Infrastructure**
                ```
                ⚡ NYSE Matching Engine
                 ├─ Order Book Management
                 ├─ Trade Execution (μs)
                 ├─ Price Discovery
                 └─ Market Maker Integration
                ```
                
                **3. Ultra-Fast Messaging**
                ```
                🚀 Custom Binary Protocol
                 ├─ Kernel Bypass (DPDK)
                 ├─ FPGA Acceleration
                 ├─ Multicast Distribution
                 └─ Sequence Number Protocol
                ```
                """)
            
            with col2:
                st.markdown("""
                **4. Real-time Processing**
                ```
                ⚡ In-Memory Engines (μs latency)
                 ├─ Tick-by-tick aggregation
                 ├─ VWAP calculations
                 ├─ Circuit breaker monitoring
                 └─ Regulatory reporting
                
                🔄 Stream Processing
                 ├─ Market surveillance
                 ├─ Cross-market arbitrage
                 ├─ Volatility detection
                 └─ News correlation
                ```
                
                **5. Data Storage**
                ```
                💾 Tiered Storage
                 ├─ Memory - Active trading
                 ├─ SSD - Intraday history
                 ├─ HDD - Historical data
                 └─ Tape - Long-term archive
                ```
                """)
            
            st.markdown("""
            #### **🔧 High-Frequency Trading Components**
            """)
            
            with st.expander("💻 **Trading Infrastructure** - Microsecond precision systems"):
                st.markdown("""
                **Order Management Systems (OMS)**:
                - **Latency Requirement**: <50 microseconds order-to-wire
                - **Architecture**: Custom C++ applications with lock-free data structures
                - **Memory Management**: Pre-allocated pools to avoid garbage collection
                - **Network**: Kernel bypass with DPDK for zero-copy networking
                
                **Risk Management Integration**:
                ```cpp
                // Real-time risk checks (sub-microsecond)
                struct RiskCheck {
                    bool position_limit_ok;
                    bool daily_loss_ok; 
                    bool concentration_ok;
                    bool market_hours_ok;
                    uint64_t check_timestamp_ns;
                };
                
                inline bool validate_order(const Order& order) {
                    return risk_engine.fast_check(order) && 
                           compliance_engine.validate(order) &&
                           credit_engine.has_capacity(order);
                }
                ```
                
                **Algorithmic Trading Strategies**:
                - **Market Making**: Provide liquidity with bid/ask spreads
                - **Statistical Arbitrage**: Mean reversion and momentum strategies  
                - **Index Arbitrage**: ETF vs underlying basket discrepancies
                - **News-based**: React to earnings, economic data, breaking news
                
                **Hardware Optimization**:
                - **CPU**: Intel Xeon with AVX-512 vector instructions
                - **Memory**: DDR4-3200 with ECC for data integrity
                - **Network**: 100Gbps Ethernet with SR-IOV virtualization
                - **Storage**: NVMe SSDs in RAID configuration
                """)
            
            with st.expander("⚡ **NYSE Matching Engine** - Core market infrastructure"):
                st.markdown("""
                **Order Book Architecture**:
                ```
                Price-Time Priority Matching
                 ├─ Buy Orders (Descending Price)
                 │   ├─ $175.25 (500 shares) - 14:30:45.123456
                 │   ├─ $175.24 (1000 shares) - 14:30:45.234567
                 │   └─ $175.23 (750 shares) - 14:30:45.345678
                 └─ Sell Orders (Ascending Price)
                     ├─ $175.26 (300 shares) - 14:30:45.456789
                     ├─ $175.27 (800 shares) - 14:30:45.567890
                     └─ $175.28 (1200 shares) - 14:30:45.678901
                ```
                
                **Matching Algorithm**:
                ```cpp
                // Simplified matching logic
                class MatchingEngine {
                private:
                    OrderBook buy_orders;  // Max heap by price
                    OrderBook sell_orders; // Min heap by price
                    
                public:
                    vector<Trade> process_order(const Order& order) {
                        vector<Trade> trades;
                        
                        if (order.side == BUY) {
                            while (!sell_orders.empty() && 
                                   sell_orders.top().price <= order.price &&
                                   order.remaining_quantity > 0) {
                                
                                Trade trade = execute_match(order, sell_orders.top());
                                trades.push_back(trade);
                                broadcast_trade(trade); // <1μs to market data
                            }
                        }
                        return trades;
                    }
                };
                ```
                
                **Market Data Generation**:
                - **L1 Data**: Best bid/offer prices and sizes
                - **L2 Data**: Full order book depth (10 levels)
                - **L3 Data**: Complete order-by-order information
                - **Trade Ticks**: Executed trades with timestamp precision
                
                **Performance Metrics**:
                - **Matching Latency**: <10 microseconds 99.9th percentile
                - **Throughput**: 1M+ messages/second during peak hours
                - **Availability**: 99.99% uptime with hot failover
                - **Data Accuracy**: Zero tolerance for pricing errors
                """)
            
            with st.expander("🚀 **Ultra-Fast Messaging** - Nanosecond data distribution"):
                st.markdown("""
                **Custom Binary Protocol**:
                ```c
                // Market data message format (64 bytes)
                struct MarketDataMessage {
                    uint64_t sequence_number;    // 8 bytes
                    uint64_t timestamp_ns;       // 8 bytes (nanosecond precision)
                    uint32_t symbol_id;          // 4 bytes (AAPL = 12345)
                    uint32_t price;              // 4 bytes (fixed point)
                    uint32_t size;               // 4 bytes (share quantity)
                    uint8_t  message_type;       // 1 byte (trade/quote)
                    uint8_t  side;               // 1 byte (buy/sell)
                    uint16_t flags;              // 2 bytes (halt, circuit breaker)
                    char     padding[32];        // Align to cache line
                } __attribute__((packed));
                ```
                
                **FPGA Acceleration**:
                - **Purpose**: Hardware-based message parsing and routing
                - **Latency**: <100 nanoseconds processing per message
                - **Throughput**: 10M+ messages/second sustained rate
                - **Functions**: Checksum validation, sequence gap detection
                
                **Multicast Distribution**:
                ```
                Market Data Feeds:
                 ├─ Feed A (Primary) - 224.0.1.1:30001
                 ├─ Feed B (Secondary) - 224.0.1.2:30001  
                 ├─ Historical Replay - 224.0.1.10:30010
                 └─ Test Environment - 224.0.1.100:30100
                
                Subscription Groups:
                 ├─ Equities - NYSE, NASDAQ, AMEX
                 ├─ Options - All option chains
                 ├─ ETFs - Exchange-traded funds
                 └─ Indices - S&P 500, Dow Jones, etc.
                ```
                
                **Network Optimization**:
                - **Kernel Bypass**: DPDK for zero-copy packet processing
                - **CPU Affinity**: Dedicated cores for network interrupts
                - **Memory Pools**: Pre-allocated buffers to avoid allocation overhead
                - **Batch Processing**: Handle multiple packets per system call
                """)
            
            with st.expander("💾 **Tiered Storage** - Massive scale data management"):
                st.markdown("""
                **Storage Hierarchy**:
                
                **L1: In-Memory (Active Trading)**:
                ```cpp
                // Real-time order book in memory
                class InMemoryOrderBook {
                private:
                    std::array<OrderLevel, 1000> bid_levels;
                    std::array<OrderLevel, 1000> ask_levels;
                    CircularBuffer<Trade, 10000> recent_trades;
                    
                public:
                    // Sub-microsecond lookups
                    inline Price get_best_bid() const { return bid_levels[0].price; }
                    inline Size get_bid_size() const { return bid_levels[0].size; }
                    inline const Trade& get_last_trade() const { 
                        return recent_trades.back(); 
                    }
                };
                ```
                
                **L2: SSD Storage (Intraday History)**:
                - **Capacity**: 100TB NVMe SSD arrays
                - **Performance**: 500K IOPS for random reads
                - **Data**: Complete order book snapshots every second
                - **Queries**: Intraday analysis and regulatory reporting
                
                **L3: HDD Arrays (Daily Archives)**:
                - **Capacity**: 10PB spinning disk storage
                - **Format**: Parquet columnar format for analytics
                - **Compression**: LZ4 compression (3:1 ratio typical)
                - **Access Pattern**: Batch processing for end-of-day reports
                
                **L4: Tape Archive (Long-term Retention)**:
                - **Capacity**: Unlimited with LTO-9 tape libraries
                - **Retention**: 7+ years for regulatory compliance
                - **Cost**: <$0.01/GB/month for cold storage
                - **Retrieval**: 24-48 hours for historical research
                
                **Database Architecture**:
                ```sql
                -- Time-series optimized schema
                CREATE TABLE market_trades (
                    trade_id BIGINT PRIMARY KEY,
                    symbol_id INT NOT NULL,
                    trade_timestamp TIMESTAMP(9) NOT NULL, -- Nanosecond precision
                    price DECIMAL(10,4) NOT NULL,
                    size INT NOT NULL,
                    buyer_id INT,
                    seller_id INT,
                    trade_conditions CHAR(4),
                    
                    INDEX idx_symbol_time (symbol_id, trade_timestamp),
                    INDEX idx_timestamp (trade_timestamp)
                ) PARTITION BY RANGE (HOUR(trade_timestamp))
                  SUBPARTITION BY HASH(symbol_id);
                ```
                """)
        
        # Common learning outcomes section
        st.markdown("---")
        st.markdown(f"""
        ### 🎯 **{company_name} Technical Learning Outcomes**
        
        **Architecture Patterns Demonstrated:**
        - ✅ **Event-Driven Design**: Asynchronous processing with message queues
        - ✅ **Microservices Architecture**: Loosely coupled, independently scalable services
        - ✅ **Data Pipeline Design**: Ingestion → Processing → Storage → Analytics
        - ✅ **Performance Optimization**: Latency-critical path optimization
        - ✅ **Fault Tolerance**: Circuit breakers, retries, and failover mechanisms
        
        **Technologies in Action:**
        - 📨 **Message Brokers**: Kafka, RabbitMQ for reliable event delivery
        - ⚡ **Stream Processing**: Flink, Kafka Streams for real-time analytics
        - 🗄️ **Storage Systems**: Multi-model persistence (OLTP, OLAP, NoSQL)
        - 🔧 **Infrastructure**: API gateways, load balancers, CDNs
        - 📊 **Monitoring**: Real-time metrics and alerting systems
        
        This technical stack provides hands-on experience with production-grade architectures used by industry leaders.
        """)
    
    with tab5:
        st.subheader(f"📚 {company_name} Schema Information")
        
        st.markdown(f"""
        **Module 1 Schema Specification** for {company_name} ingestion events.
        All data follows the reproducibility rules with deterministic seeds and realistic AED pricing.
        """)
        
        # Schema documentation based on company
        if company_name == "Uber":
            st.markdown("""
            ### 🚗 Uber Ingestion Events Schema
            
            **Purpose:** High-cardinality ride event records for ingestion pipeline simulation.
            
            | Field | Type | Description | Example |
            |-------|------|-------------|---------|
            | `event_id` | TEXT | Unique event identifier | evt_00001234 |
            | `ride_id` | TEXT | Ride identifier | ride_001234 |
            | `driver_id` | TEXT | Driver identifier | drv_5678 |
            | `rider_id` | TEXT | Rider identifier | usr_98765 |
            | `event_type` | TEXT | Event type (request/accept/start/end/cancel) | start |
            | `pickup_ts` | TEXT | Pickup timestamp | 2024-08-30 14:30:00 |
            | `dropoff_ts` | TEXT | Dropoff timestamp | 2024-08-30 14:45:00 |
            | `pickup_lat` | REAL | Pickup latitude (Dubai area) | 25.2048 |
            | `pickup_lng` | REAL | Pickup longitude (Dubai area) | 55.2708 |
            | `dropoff_lat` | REAL | Dropoff latitude | 25.2156 |
            | `dropoff_lng` | REAL | Dropoff longitude | 55.2834 |
            | `distance_km` | REAL | Trip distance in kilometers | 12.5 |
            | `price_aed` | REAL | Trip price in AED (5-300 range) | 45.75 |
            | `payment_method` | TEXT | Payment method | credit_card |
            | `status` | TEXT | Trip status | completed |
            | `ingestion_ts` | TEXT | Event ingestion timestamp | 2024-08-30 14:30:03 |
            
            **Notes:** 
            - Uses Dubai coordinates for realistic geolocation
            - Exponential inter-arrival times for streaming simulation
            - AED pricing follows specified 5-300 range
            """)
            
        elif company_name == "Netflix":
            st.markdown("""
            ### 🎬 Netflix Ingestion Events Schema
            
            **Purpose:** Streaming event records for content consumption analysis.
            
            | Field | Type | Description | Example |
            |-------|------|-------------|---------|
            | `event_id` | TEXT | Unique event identifier | nf_evt_00001234 |
            | `user_id` | TEXT | User identifier | nf_usr_567890 |
            | `device_type` | TEXT | Device type | smart_tv |
            | `content_id` | TEXT | Content identifier | cnt_042 |
            | `content_title` | TEXT | Content title | Stranger Things |
            | `event_type` | TEXT | Event type (play/pause/stop/seek/resume) | play |
            | `timestamp` | TEXT | Event timestamp | 2024-08-30 20:15:00 |
            | `duration_sec` | INTEGER | Duration in seconds | 3600 |
            | `bitrate_kbps` | INTEGER | Video bitrate | 1080 |
            | `country` | TEXT | User country | UAE |
            | `subscription_tier` | TEXT | Subscription level | premium |
            
            **Notes:**
            - Global content catalog with popular titles
            - Multiple device types and countries
            - Realistic streaming durations and bitrates
            """)
            
        elif company_name == "Amazon":
            st.markdown("""
            ### 🛒 Amazon Order Ingestion Events Schema
            
            **Purpose:** E-commerce order event records for transaction processing.
            
            | Field | Type | Description | Example |
            |-------|------|-------------|---------|
            | `event_id` | TEXT | Unique event identifier | amz_evt_00001234 |
            | `order_id` | TEXT | Order identifier | amz_order_12345678 |
            | `customer_id` | TEXT | Customer identifier | cust_567890 |
            | `product_id` | TEXT | Product identifier | prod_123456 |
            | `event_type` | TEXT | Event type (created/paid/shipped/delivered/returned) | paid |
            | `quantity` | INTEGER | Product quantity | 2 |
            | `unit_price_aed` | REAL | Unit price in AED | 125.50 |
            | `total_price_aed` | REAL | Total price in AED (10-5000 range) | 251.00 |
            | `timestamp` | TEXT | Event timestamp | 2024-08-30 16:45:00 |
            | `channel` | TEXT | Order channel | mobile_app |
            | `product_category` | TEXT | Product category | electronics |
            
            **Notes:**
            - AED pricing follows specified 10-5000 range
            - Multiple order channels and product categories
            - Order lifecycle event tracking
            """)
            
        elif company_name == "Airbnb":
            st.markdown("""
            ### 🏠 Airbnb Booking Ingestion Events Schema
            
            **Purpose:** Accommodation booking event records for hospitality analytics.
            
            | Field | Type | Description | Example |
            |-------|------|-------------|---------|
            | `event_id` | TEXT | Unique event identifier | bnb_evt_00001234 |
            | `booking_id` | TEXT | Booking identifier | bnb_book_12345678 |
            | `host_id` | TEXT | Host identifier | host_56789 |
            | `guest_id` | TEXT | Guest identifier | guest_987654 |
            | `property_id` | TEXT | Property identifier | prop_12345 |
            | `event_type` | TEXT | Event type (requested/confirmed/cancelled/checked_in/checked_out) | confirmed |
            | `checkin` | TEXT | Check-in date | 2024-09-01 |
            | `checkout` | TEXT | Check-out date | 2024-09-05 |
            | `price_per_night_aed` | REAL | Nightly rate in AED (150-2500 range) | 450.00 |
            | `total_price_aed` | REAL | Total booking price | 1800.00 |
            | `nights` | INTEGER | Number of nights | 4 |
            | `timestamp` | TEXT | Event timestamp | 2024-08-30 18:30:00 |
            | `city` | TEXT | UAE city | Dubai |
            | `property_type` | TEXT | Property type | apartment |
            
            **Notes:**
            - UAE-focused with realistic city distribution
            - AED pricing follows specified 150-2500 per night range
            - Booking lifecycle event tracking
            """)
            
        else:  # NYSE
            st.markdown("""
            ### 💰 NYSE Trading Ticks Schema (High-Frequency)
            
            **Purpose:** High-frequency trading tick records for financial market analysis.
            
            | Field | Type | Description | Example |
            |-------|------|-------------|---------|
            | `tick_id` | TEXT | Unique tick identifier | tick_0000001234 |
            | `ticker` | TEXT | Stock symbol | AAPL |
            | `trade_ts` | TEXT | Trade timestamp (ms precision) | 2024-08-30 14:30:45.123 |
            | `price` | REAL | Trade price in USD | 175.25 |
            | `size` | INTEGER | Share volume | 1500 |
            | `trade_type` | TEXT | Trade type (buy/sell) | buy |
            | `exchange` | TEXT | Trading exchange | NYSE |
            | `order_id` | TEXT | Order identifier | ord_0000001234 |
            
            **Notes:**
            - Millisecond precision timestamps for HFT simulation
            - Realistic price movements with 0.1% volatility
            - Multiple exchanges and major stock symbols
            - High-frequency data (10K records vs 5K for others)
            """)
        
        # Schema validation info
        st.markdown("""
        ### ✅ Data Validation Rules
        
        **Reproducibility:**
        - Deterministic seed (42) for consistent data generation
        - 90-day time window for realistic historical data
        - Exponential inter-arrival times for streaming patterns
        
        **Quality Checks:**
        - No null values in key identifier fields
        - Timestamp formats follow ISO standards
        - Price ranges adhere to specified AED/USD limits
        - Event types follow defined categorical sets
        
        **Performance:**
        - Optimized for SQLite storage with proper indexing
        - Batch insert operations for ingestion speed
        - JSON payloads stored as TEXT for schema evolution
        """)
        
        # Database optimization tips
        st.markdown("""
        ### 🗄️ SQLite Optimization
        
        ```sql
        -- Recommended PRAGMA settings
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        
        -- Index creation for performance
        CREATE INDEX IF NOT EXISTS idx_timestamp ON ingest_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_event_type ON ingest_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_user_id ON ingest_events(user_id);
        ```
        """)
        
        # Add detailed ingestion architecture explanation
        st.markdown("---")
        st.markdown(f"### 🏗️ {company_name} Ingestion Architecture")
        
        if company_name == "Amazon":
            st.markdown("""
            #### 🛒 **Amazon E-commerce Ingestion Pattern**
            
            **Architecture**: AWS-Driven Multi-Model Ingestion
            
            **Batch Ingestion:**
            - 📦 Order Processing: Daily/hourly batch uploads from warehouses
            - 📋 Product Catalogs: CSV/JSON uploads via S3 + Glue  
            - 📊 Inventory Updates: API pulls from supplier systems
            - 👥 Customer Data: CRM system imports via Lambda
            
            **Real-time Ingestion:**
            - 🚀 **Primary Tool**: Amazon Kinesis Data Streams for order events
            - 📈 **Volume**: ~300,000 orders/second during peak times
            - 🔄 **Event Flow**: `placed → paid → shipped → delivered → returned`
            - 🗂️ **Partitioning**: By customer_id and geographic region
            
            **Technical Stack:**
            ```
            Web/Mobile → API Gateway → Kinesis → Lambda → DynamoDB/S3
                      ↘ Kinesis Analytics ↘ Real-time recommendations
            ```
            
            **Our Implementation:**
            - ✅ AED pricing (10-5000 range) for realistic UAE market data
            - ✅ Multiple channels: web, mobile_app, alexa, api, marketplace
            - ✅ Product categories: electronics, books, clothing, home_garden
            - ✅ Order lifecycle events with proper state transitions
            """)
            
        elif company_name == "Netflix":
            st.markdown("""
            #### 🎬 **Netflix Streaming Ingestion Pattern**
            
            **Architecture**: Event-Driven Real-Time Processing
            
            **Batch Ingestion:**
            - 🎥 Content Metadata: Daily content catalog updates
            - 👤 User Profiles: Batch preference calculations  
            - 🧪 A/B Test Results: Daily experiment result aggregations
            
            **Real-time Ingestion:**
            - 🚀 **Primary Tool**: Apache Kafka for play events
            - 📈 **Volume**: ~8 billion events/day globally
            - 🔄 **Event Flow**: `play → pause → seek → resume → stop → complete`
            - 📊 **Topics**: play-events, user-interactions, content-performance
            
            **Technical Stack:**
            ```
            Streaming Apps → Kafka → Spark Streaming → Cassandra + S3
                          ↘ Kafka Streams ↘ Real-time personalization
            ```
            
            **Our Implementation:**
            - ✅ Global content catalog with popular Netflix shows
            - ✅ Multiple device types: smart_tv, mobile, tablet, laptop
            - ✅ International audience: UAE, USA, UK, Canada, etc.
            - ✅ Realistic streaming durations and bitrates
            - ✅ Subscription tiers: basic, standard, premium
            """)
            
        elif company_name == "Uber":
            st.markdown("""
            #### 🚗 **Uber Real-Time Mobility Pattern**
            
            **Architecture**: High-Frequency GPS + Ride Events
            
            **Batch Ingestion:**
            - 👨‍✈️ Driver Profiles: Daily driver onboarding/updates
            - 🗺️ Route Planning: Historical traffic pattern analysis
            - 💰 Financial Settlements: Daily driver/rider payment processing
            
            **Real-time Ingestion:**
            - 🚀 **Primary Tool**: Apache Kafka for GPS + ride events
            - 📍 **GPS Stream**: Driver locations every 2-4 seconds
            - 📈 **Volume**: ~15 million trips/day with GPS tracking
            - 🔄 **Event Flow**: `request → accept → start → end → cancel`
            - 🏙️ **Partitioning**: By city/geographic regions
            
            **Technical Stack:**
            ```
            Driver Apps → Kafka → Flink → Redis + Cassandra
                      ↘ Kafka Streams ↘ Surge pricing (sub-second)
            ```
            
            **Our Implementation:**
            - ✅ Dubai-based coordinates (25.2048, 55.2708) for realistic geolocation
            - ✅ AED pricing (5-300 range) with surge multipliers
            - ✅ Payment methods: credit_card, cash, wallet, corporate
            - ✅ Exponential inter-arrival times for streaming simulation
            - ✅ Distance-based fare calculations with surge pricing
            """)
            
        elif company_name == "Airbnb":
            st.markdown("""
            #### 🏠 **Airbnb Marketplace Ingestion Pattern**
            
            **Architecture**: Orchestrated Batch + Event Processing
            
            **Batch Ingestion:**
            - 🏘️ Property Listings: Daily property updates via CSV/API
            - 👥 Host Profiles: Batch profile and verification data
            - ⭐ Review Processing: Daily review sentiment analysis
            - 💰 Pricing Optimization: Historical booking pattern analysis
            
            **Real-time Ingestion:**
            - 🔍 Search Events: Real-time search and booking requests
            - 📈 **Volume**: ~5 million searches/day globally  
            - 🔄 **Event Flow**: `requested → confirmed → cancelled → checked_in → checked_out`
            - 💭 User Activity: Page views, wish-list updates, messages
            
            **Technical Stack:**
            ```
            Web/Mobile → Message Queue → Airflow DAGs → MySQL + S3 + Hive
                      ↘ Real-time search ranking ↘ Elasticsearch
            ```
            
            **Our Implementation:**
            - ✅ UAE-focused: Dubai, Abu Dhabi, Sharjah, and 4 other emirates
            - ✅ AED pricing (150-2500/night) with seasonal variations
            - ✅ Property types: apartment, villa, hotel_room, entire_home
            - ✅ Realistic booking lifecycle with proper state management
            - ✅ Multi-night stays with dynamic total pricing
            """)
            
        else:  # NYSE
            st.markdown("""
            #### 💰 **NYSE High-Frequency Trading Pattern**
            
            **Architecture**: Ultra-Low Latency Trading Systems
            
            **Batch Ingestion:**
            - 📊 Market Data: End-of-day settlement and reconciliation
            - 🏢 Corporate Actions: Dividend, split, earnings announcements
            - 📋 Regulatory Reports: Daily compliance and audit data
            
            **Real-time Ingestion:**
            - ⚡ **Ultra-Fast**: Microsecond-precision trade execution data
            - 📈 **Volume**: ~5 billion messages/day, 1M+ msgs/second peak
            - 🔄 **Data Types**: Trading ticks, order book, market data feeds
            - ⏱️ **Latency**: Sub-millisecond processing requirements
            
            **Technical Stack:**
            ```
            Trading Systems → Ultra-fast messaging → In-memory → HDB
                           ↘ Real-time risk management ↘ Compliance
            ```
            
            **Our Implementation:**
            - ✅ High-frequency: 10K records vs 5K for other companies
            - ✅ Millisecond precision timestamps for HFT simulation
            - ✅ Major tickers: AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA
            - ✅ Multiple exchanges: NYSE, NASDAQ, BATS, IEX
            - ✅ Realistic price movements with 0.1% volatility
            - ✅ Trading volumes from 100 to 50,000 shares
            """)
        
        st.markdown("""
        ### 📚 **Learning Outcomes by Company**
        
        | Company | Key Learning | Architecture Focus | Data Volume |
        |---------|--------------|-------------------|-------------|
        | 🛒 **Amazon** | Multi-channel order lifecycle | AWS-native ingestion | 300K orders/sec |
        | 🎬 **Netflix** | Content-driven event streaming | Global scale processing | 8B events/day |
        | 🚗 **Uber** | Location-based real-time processing | Sub-second surge pricing | 15M trips/day |
        | 🏠 **Airbnb** | Marketplace search and booking dynamics | Orchestrated workflows | 5M searches/day |
        | 💰 **NYSE** | Ultra-low latency financial processing | Microsecond precision | 5B messages/day |
        
        Each implementation demonstrates realistic production patterns with proper:
        - 🗄️ **Database design** with optimized indexing
        - 💰 **Currency handling** (AED for UAE companies)
        - 📊 **Event distributions** matching real-world patterns
        - ⚡ **Performance optimization** with WAL mode and transactions
        """)
        
        # Generate comprehensive sample dataset for EDA
        np.random.seed(42)
        n_records = 5000
        
        # Create comprehensive dataset
        sample_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=n_records, freq='15min'),
            'user_id': np.random.randint(1000, 9999, n_records),
            'event_type': np.random.choice(['purchase', 'view', 'click', 'login', 'logout'], n_records, p=[0.1, 0.4, 0.3, 0.1, 0.1]),
            'value': np.random.lognormal(mean=3, sigma=1, size=n_records).round(2),
            'source': np.random.choice(['mobile', 'web', 'api', 'batch'], n_records, p=[0.4, 0.3, 0.2, 0.1]),
            'region': np.random.choice(['US', 'EU', 'ASIA', 'LATAM'], n_records, p=[0.4, 0.3, 0.2, 0.1]),
            'processing_time_ms': np.random.exponential(scale=50, size=n_records).round(1),
            'success': np.random.choice([True, False], n_records, p=[0.95, 0.05])
        })
        
        # Add derived columns
        sample_data['hour'] = sample_data['timestamp'].dt.hour
        sample_data['day_of_week'] = sample_data['timestamp'].dt.day_name()
        sample_data['month'] = sample_data['timestamp'].dt.month
        
        chart_type = st.selectbox("Choose Chart Type:", 
            ["Histograms", "Bar Charts", "Pie Charts", "Donut Charts", "Time Series", "Correlation Matrix", "Box Plots"])
        
        if chart_type == "Histograms":
            col1, col2 = st.columns(2)
            
            with col1:
                # Value distribution histogram
                fig_hist1 = px.histogram(sample_data, x='value', nbins=50, 
                                       title='Distribution of Transaction Values',
                                       labels={'value': 'Transaction Value ($)', 'count': 'Frequency'})
                fig_hist1.update_layout(height=400)
                st.plotly_chart(fig_hist1, use_container_width=True)
                
            with col2:
                # Processing time histogram
                fig_hist2 = px.histogram(sample_data, x='processing_time_ms', nbins=30,
                                       title='Distribution of Processing Times',
                                       labels={'processing_time_ms': 'Processing Time (ms)', 'count': 'Frequency'})
                fig_hist2.update_layout(height=400)
                st.plotly_chart(fig_hist2, use_container_width=True)
            
            # Hourly distribution
            fig_hist3 = px.histogram(sample_data, x='hour', nbins=24,
                                   title='Data Ingestion by Hour of Day',
                                   labels={'hour': 'Hour of Day', 'count': 'Number of Records'})
            st.plotly_chart(fig_hist3, use_container_width=True)
            
        elif chart_type == "Bar Charts":
            col1, col2 = st.columns(2)
            
            with col1:
                # Event type distribution
                event_counts = sample_data['event_type'].value_counts()
                fig_bar1 = px.bar(x=event_counts.index, y=event_counts.values,
                                title='Events by Type',
                                labels={'x': 'Event Type', 'y': 'Count'})
                fig_bar1.update_layout(height=400)
                st.plotly_chart(fig_bar1, use_container_width=True)
                
            with col2:
                # Source distribution
                source_counts = sample_data['source'].value_counts()
                fig_bar2 = px.bar(x=source_counts.index, y=source_counts.values,
                                title='Data Sources',
                                labels={'x': 'Source', 'y': 'Count'})
                fig_bar2.update_layout(height=400)
                st.plotly_chart(fig_bar2, use_container_width=True)
            
            # Average value by region
            avg_by_region = sample_data.groupby('region')['value'].mean().sort_values(ascending=False)
            fig_bar3 = px.bar(x=avg_by_region.index, y=avg_by_region.values,
                            title='Average Transaction Value by Region',
                            labels={'x': 'Region', 'y': 'Average Value ($)'})
            st.plotly_chart(fig_bar3, use_container_width=True)
            
        elif chart_type == "Pie Charts":
            col1, col2 = st.columns(2)
            
            with col1:
                # Event type pie chart
                event_counts = sample_data['event_type'].value_counts()
                fig_pie1 = px.pie(values=event_counts.values, names=event_counts.index,
                                title='Distribution of Event Types')
                fig_pie1.update_layout(height=400)
                st.plotly_chart(fig_pie1, use_container_width=True)
                
            with col2:
                # Region distribution pie chart
                region_counts = sample_data['region'].value_counts()
                fig_pie2 = px.pie(values=region_counts.values, names=region_counts.index,
                                title='Distribution by Region')
                fig_pie2.update_layout(height=400)
                st.plotly_chart(fig_pie2, use_container_width=True)
            
            # Success rate pie chart
            success_counts = sample_data['success'].value_counts()
            fig_pie3 = px.pie(values=success_counts.values, 
                            names=['Success', 'Failed'] if success_counts.index[0] else ['Failed', 'Success'],
                            title='Success vs Failed Ingestions',
                            color_discrete_map={'Success': 'green', 'Failed': 'red'})
            st.plotly_chart(fig_pie3, use_container_width=True)
            
        elif chart_type == "Donut Charts":
            col1, col2 = st.columns(2)
            
            with col1:
                # Source donut chart
                source_counts = sample_data['source'].value_counts()
                fig_donut1 = go.Figure(data=[go.Pie(labels=source_counts.index, values=source_counts.values, hole=.3)])
                fig_donut1.update_layout(title="Data Sources Distribution", height=400)
                st.plotly_chart(fig_donut1, use_container_width=True)
                
            with col2:
                # Day of week donut chart
                dow_counts = sample_data['day_of_week'].value_counts()
                fig_donut2 = go.Figure(data=[go.Pie(labels=dow_counts.index, values=dow_counts.values, hole=.3)])
                fig_donut2.update_layout(title="Ingestion by Day of Week", height=400)
                st.plotly_chart(fig_donut2, use_container_width=True)
            
            # Processing time categories donut
            sample_data['processing_category'] = pd.cut(sample_data['processing_time_ms'], 
                                                      bins=[0, 25, 50, 100, float('inf')], 
                                                      labels=['Fast', 'Medium', 'Slow', 'Very Slow'])
            proc_counts = sample_data['processing_category'].value_counts()
            fig_donut3 = go.Figure(data=[go.Pie(labels=proc_counts.index, values=proc_counts.values, hole=.3)])
            fig_donut3.update_layout(title="Processing Speed Distribution")
            st.plotly_chart(fig_donut3, use_container_width=True)
            
        elif chart_type == "Time Series":
            # Hourly ingestion rate
            hourly_data = sample_data.groupby(sample_data['timestamp'].dt.floor('h')).size().reset_index()
            hourly_data.columns = ['timestamp', 'count']
            
            fig_ts1 = px.line(hourly_data, x='timestamp', y='count',
                            title='Hourly Data Ingestion Rate',
                            labels={'count': 'Records per Hour', 'timestamp': 'Time'})
            st.plotly_chart(fig_ts1, use_container_width=True)
            
            # Daily average processing time
            daily_proc = sample_data.groupby(sample_data['timestamp'].dt.date)['processing_time_ms'].mean().reset_index()
            daily_proc.columns = ['date', 'avg_processing_time']
            
            fig_ts2 = px.line(daily_proc, x='date', y='avg_processing_time',
                            title='Daily Average Processing Time Trend',
                            labels={'avg_processing_time': 'Average Processing Time (ms)', 'date': 'Date'})
            st.plotly_chart(fig_ts2, use_container_width=True)
            
        elif chart_type == "Correlation Matrix":
            # Prepare numerical data for correlation
            numerical_data = sample_data[['value', 'processing_time_ms', 'hour', 'month']].copy()
            numerical_data['success_int'] = sample_data['success'].astype(int)
            
            # Calculate correlation matrix
            corr_matrix = numerical_data.corr()
            
            fig_corr = px.imshow(corr_matrix, 
                               text_auto=True, 
                               aspect="auto",
                               title="Correlation Matrix of Numerical Variables")
            st.plotly_chart(fig_corr, use_container_width=True)
            
        elif chart_type == "Box Plots":
            col1, col2 = st.columns(2)
            
            with col1:
                # Processing time by source
                fig_box1 = px.box(sample_data, x='source', y='processing_time_ms',
                                title='Processing Time Distribution by Source')
                fig_box1.update_layout(height=400)
                st.plotly_chart(fig_box1, use_container_width=True)
                
            with col2:
                # Value distribution by event type
                fig_box2 = px.box(sample_data, x='event_type', y='value',
                                title='Transaction Value Distribution by Event Type')
                fig_box2.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig_box2, use_container_width=True)
            
            # Processing time by success status
            fig_box3 = px.box(sample_data, x='success', y='processing_time_ms',
                            title='Processing Time: Success vs Failed Ingestions')
            st.plotly_chart(fig_box3, use_container_width=True)
    
    with tab4:
        st.subheader("🔄 Data Ingestion Flow Charts")
        
        flow_type = st.selectbox("Choose Flow Chart:", 
            ["Batch Ingestion Process", "Real-time Ingestion Process", "Hybrid Architecture", "Error Handling Flow"])
        
        if flow_type == "Batch Ingestion Process":
            # Create Netflix-style architecture diagram
            st.markdown("""
            <div style="background: linear-gradient(135deg, #2D3748, #4A5568); padding: 30px; border-radius: 15px; margin: 20px 0; border: 2px solid #E2E8F0;">
                <div style="text-align: center; margin-bottom: 30px;">
                    <div style="background: #F7FAFC; padding: 15px 30px; border-radius: 25px; display: inline-block; border: 2px solid white;">
                        <h2 style="color: #2D3748; font-size: 24px; margin: 0; font-weight: bold;">
                            BATCH DATA INGESTION
                        </h2>
                    </div>
                </div>
                
                <!-- Data Sources Row -->
                <div style="display: flex; justify-content: center; margin: 30px 0;">
                    <div style="text-align: center; margin: 0 15px;">
                        <div style="background: #4299E1; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Database</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">🗄️</div>
                            <div style="color: #BEE3F8; font-size: 12px;">PostgreSQL</div>
                        </div>
                    </div>
                    <div style="text-align: center; margin: 0 15px;">
                        <div style="background: #4299E1; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Files</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">📄</div>
                            <div style="color: #BEE3F8; font-size: 12px;">CSV/JSON</div>
                        </div>
                    </div>
                    <div style="text-align: center; margin: 0 15px;">
                        <div style="background: #4299E1; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">APIs</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">🌐</div>
                            <div style="color: #BEE3F8; font-size: 12px;">REST</div>
                        </div>
                    </div>
                </div>
                
                <!-- Orchestration Layer -->
                <div style="text-align: center; margin: 40px 0;">
                    <div style="background: #48BB78; padding: 25px 40px; border-radius: 8px; display: inline-block; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
                        <div style="color: white; font-weight: bold; font-size: 16px; margin-bottom: 5px;">Orchestration</div>
                        <div style="color: white; font-size: 24px; margin: 8px 0;">⚙️</div>
                        <div style="color: #C6F6D5; font-size: 14px;">Apache Airflow</div>
                    </div>
                </div>
                
                <!-- ETL Pipeline Row -->
                <div style="display: flex; justify-content: center; margin: 30px 0;">
                    <div style="text-align: center; margin: 0 15px;">
                        <div style="background: #ED8936; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Extract</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">📥</div>
                            <div style="color: #FEEBC8; font-size: 12px;">Python</div>
                        </div>
                    </div>
                    <div style="text-align: center; margin: 0 15px;">
                        <div style="background: #ED8936; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Transform</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">⚡</div>
                            <div style="color: #FEEBC8; font-size: 12px;">Spark</div>
                        </div>
                    </div>
                    <div style="text-align: center; margin: 0 15px;">
                        <div style="background: #ED8936; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 120px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Load</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">📤</div>
                            <div style="color: #FEEBC8; font-size: 12px;">Warehouse</div>
                        </div>
                    </div>
                </div>
                
                <!-- Storage & Monitoring Row -->
                <div style="display: flex; justify-content: center; margin: 30px 0;">
                    <div style="text-align: center; margin: 0 20px;">
                        <div style="background: #38B2AC; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 140px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Storage</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">🏢</div>
                            <div style="color: #B2F5EA; font-size: 12px;">Snowflake</div>
                        </div>
                    </div>
                    <div style="text-align: center; margin: 0 20px;">
                        <div style="background: #805AD5; padding: 20px; border-radius: 8px; border: 3px solid white; box-shadow: 0 4px 12px rgba(0,0,0,0.15); min-width: 140px;">
                            <div style="color: white; font-weight: bold; font-size: 14px; margin-bottom: 5px;">Monitoring</div>
                            <div style="color: white; font-size: 20px; margin: 8px 0;">📊</div>
                            <div style="color: #E9D8FD; font-size: 12px;">Grafana</div>
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Add process explanation
            st.markdown("""
            **Batch Ingestion Process Steps:**
            1. **Source**: Data available in source system
            2. **Scheduler**: Triggers processing at defined intervals
            3. **Extract**: Pulls data from source systems
            4. **Validate**: Checks data quality and format
            5. **Transform**: Applies business rules and transformations
            6. **Load**: Stores processed data in target system
            7. **Success/Error**: Final status with retry mechanism
            """)
            
        elif flow_type == "Real-time Ingestion Process":
            # Create enhanced real-time processing flow chart
            fig_realtime = go.Figure()
            
            # Define enhanced nodes with icons and better styling
            nodes = {
                '🚀 Event\nSources\n(IoT/Logs)': {'pos': (2, 8), 'color': '#4A90E2', 'category': 'source'},
                '📨 Message Queue\n(Kafka/RabbitMQ)': {'pos': (6, 8), 'color': '#FF6B35', 'category': 'messaging'},
                '⚡ Stream Processor\n(Spark/Flink)': {'pos': (10, 8), 'color': '#BD10E0', 'category': 'processing'},
                '✅ Real-time\nValidation': {'pos': (14, 8), 'color': '#F5A623', 'category': 'validation'},
                '🔄 Transform\nOn-the-fly': {'pos': (18, 8), 'color': '#F5A623', 'category': 'transform'},
                '💾 Output Sink\n(DB/Cache)': {'pos': (22, 8), 'color': '#50E3C2', 'category': 'storage'},
                '☠️ Dead Letter\nQueue': {'pos': (10, 5), 'color': '#D0021B', 'category': 'error'},
                '📊 Real-time\nMonitoring': {'pos': (14, 5), 'color': '#9013FE', 'category': 'monitoring'},
                '🚨 Instant\nAlerts': {'pos': (18, 5), 'color': '#D0021B', 'category': 'alerts'}
            }
            
            # Add enhanced nodes with modern styling
            for node, config in nodes.items():
                x, y = config['pos']
                color = config['color']
                
                # Create modern card-like design
                fig_realtime.add_shape(
                    type="rect",
                    x0=x-1.4, y0=y-0.7, x1=x+1.4, y1=y+0.7,
                    fillcolor=color,
                    line=dict(color='white', width=3),
                    layer="below"
                )
                
                # Add shadow effect
                fig_realtime.add_shape(
                    type="rect",
                    x0=x-1.35, y0=y-0.65, x1=x+1.45, y1=y+0.75,
                    fillcolor='rgba(0,0,0,0.15)',
                    line=dict(color='rgba(0,0,0,0)', width=0),
                    layer="below"
                )
                
                # Add text with better formatting
                fig_realtime.add_annotation(
                    x=x, y=y, 
                    text=node, 
                    showarrow=False, 
                    font=dict(size=10, color='white', family="Arial Black"),
                    align="center"
                )
            
            # Add enhanced connections with streaming flow styling
            connections = [
                ('🚀 Event\nSources\n(IoT/Logs)', '📨 Message Queue\n(Kafka/RabbitMQ)'),
                ('📨 Message Queue\n(Kafka/RabbitMQ)', '⚡ Stream Processor\n(Spark/Flink)'),
                ('⚡ Stream Processor\n(Spark/Flink)', '✅ Real-time\nValidation'),
                ('✅ Real-time\nValidation', '🔄 Transform\nOn-the-fly'),
                ('🔄 Transform\nOn-the-fly', '💾 Output Sink\n(DB/Cache)'),
                ('⚡ Stream Processor\n(Spark/Flink)', '☠️ Dead Letter\nQueue'),
                ('✅ Real-time\nValidation', '📊 Real-time\nMonitoring'),
                ('🔄 Transform\nOn-the-fly', '📊 Real-time\nMonitoring'),
                ('📊 Real-time\nMonitoring', '🚨 Instant\nAlerts')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]['pos']
                x1, y1 = nodes[end]['pos']
                
                # Add streaming arrows with different styling
                if y0 == y1:  # Horizontal arrows (main flow)
                    arrow_color = '#FF6B35'  # Orange for main stream
                    arrow_width = 4
                else:  # Vertical arrows (monitoring/errors)
                    arrow_color = '#666666'  # Gray for side flows
                    arrow_width = 2
                
                fig_realtime.add_annotation(
                    ax=x0+1.4 if x1 > x0 else x0-1.4,
                    ay=y0,
                    x=x1-1.4 if x1 > x0 else x1+1.4,
                    y=y1,
                    arrowhead=3,
                    arrowsize=2,
                    arrowwidth=arrow_width,
                    arrowcolor=arrow_color
                )
            
            fig_realtime.update_layout(
                title={
                    'text': "⚡ REAL-TIME STREAMING ARCHITECTURE",
                    'x': 0.5,
                    'font': {'size': 20, 'color': '#333333', 'family': 'Arial Black'}
                },
                xaxis=dict(range=[0, 24], showgrid=False, showticklabels=False, zeroline=False),
                yaxis=dict(range=[3, 10], showgrid=False, showticklabels=False, zeroline=False),
                height=600,
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(248,249,250,1)'
            )
            st.plotly_chart(fig_realtime, use_container_width=True)
            
            st.markdown("""
            **Real-time Ingestion Process:**
            1. **Event Source**: Continuous data streams (IoT, logs, transactions)
            2. **Message Queue**: Buffers incoming events (Kafka, RabbitMQ)
            3. **Stream Processor**: Real-time processing engine (Spark, Flink)
            4. **Validate**: Real-time data quality checks
            5. **Transform**: Apply transformations on-the-fly
            6. **Output Sink**: Store in target systems (DB, warehouse)
            7. **Dead Letter Queue**: Handle failed messages
            8. **Monitoring**: Track processing health and performance
            """)
            
        elif flow_type == "Hybrid Architecture":
            fig_hybrid = go.Figure()
            
            # Complex hybrid architecture
            nodes = {
                'Transactional\nDB': (1, 9),
                'Logs': (1, 7),
                'IoT Sensors': (1, 5),
                'CDC': (3, 9),
                'Log Shipper': (3, 7),
                'IoT Gateway': (3, 5),
                'Kafka': (5, 7),
                'Batch ETL': (7, 9),
                'Stream\nProcessor': (7, 5),
                'Data Lake': (9, 7),
                'Data\nWarehouse': (11, 9),
                'Real-time\nDashboard': (11, 5),
                'Analytics': (13, 7)
            }
            
            for node, (x, y) in nodes.items():
                if 'DB' in node or 'Lake' in node or 'Warehouse' in node:
                    color = 'lightgreen'
                elif 'Kafka' in node:
                    color = 'orange'
                elif 'ETL' in node or 'Processor' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_hybrid.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                   fillcolor=color, line=dict(color="black", width=2))
                fig_hybrid.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            # Add connections for hybrid flow
            connections = [
                ('Transactional\nDB', 'CDC'), ('Logs', 'Log Shipper'), ('IoT Sensors', 'IoT Gateway'),
                ('CDC', 'Kafka'), ('Log Shipper', 'Kafka'), ('IoT Gateway', 'Kafka'),
                ('Kafka', 'Batch ETL'), ('Kafka', 'Stream\nProcessor'),
                ('Batch ETL', 'Data Lake'), ('Stream\nProcessor', 'Data Lake'),
                ('Data Lake', 'Data\nWarehouse'), ('Stream\nProcessor', 'Real-time\nDashboard'),
                ('Data\nWarehouse', 'Analytics'), ('Real-time\nDashboard', 'Analytics')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_hybrid.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_hybrid.update_layout(
                title="Hybrid Data Ingestion Architecture",
                xaxis=dict(range=[0, 14], showgrid=False, showticklabels=False),
                yaxis=dict(range=[4, 10], showgrid=False, showticklabels=False),
                height=600,
                showlegend=False
            )
            st.plotly_chart(fig_hybrid, use_container_width=True)
            
            st.markdown("""
            **Hybrid Architecture Components:**
            - **Sources**: Multiple data sources with different characteristics
            - **Kafka**: Central message broker for both batch and stream processing
            - **Dual Processing**: Both batch ETL and stream processing paths
            - **Storage**: Data lake for raw data, warehouse for structured analytics
            - **Consumption**: Real-time dashboards and batch analytics
            """)
            
        elif flow_type == "Error Handling Flow":
            fig_error = go.Figure()
            
            nodes = {
                'Data\nIngestion': (2, 8),
                'Validation': (4, 8),
                'Success': (6, 9),
                'Error\nDetected': (6, 7),
                'Error\nClassification': (8, 7),
                'Transient\nError': (10, 8),
                'Permanent\nError': (10, 6),
                'Retry\nQueue': (12, 8),
                'Dead Letter\nQueue': (12, 6),
                'Alert\nSystem': (14, 7),
                'Manual\nReview': (14, 5)
            }
            
            for node, (x, y) in nodes.items():
                if 'Success' in node:
                    color = 'lightgreen'
                elif 'Error' in node or 'Dead' in node:
                    color = 'lightcoral'
                elif 'Retry' in node:
                    color = 'orange'
                else:
                    color = 'lightblue'
                    
                fig_error.add_shape(type="rect", x0=x-0.8, y0=y-0.3, x1=x+0.8, y1=y+0.3,
                                  fillcolor=color, line=dict(color="black", width=2))
                fig_error.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Data\nIngestion', 'Validation'), ('Validation', 'Success'),
                ('Validation', 'Error\nDetected'), ('Error\nDetected', 'Error\nClassification'),
                ('Error\nClassification', 'Transient\nError'), ('Error\nClassification', 'Permanent\nError'),
                ('Transient\nError', 'Retry\nQueue'), ('Permanent\nError', 'Dead Letter\nQueue'),
                ('Retry\nQueue', 'Data\nIngestion'), ('Dead Letter\nQueue', 'Alert\nSystem'),
                ('Alert\nSystem', 'Manual\nReview')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_error.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_error.update_layout(
                title="Error Handling Flow in Data Ingestion",
                xaxis=dict(range=[1, 15], showgrid=False, showticklabels=False),
                yaxis=dict(range=[4, 10], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_error, use_container_width=True)
            
            st.markdown("""
            **Error Handling Strategy:**
            1. **Error Detection**: Identify issues during validation
            2. **Classification**: Distinguish between transient and permanent errors
            3. **Retry Logic**: Automatic retry for transient errors
            4. **Dead Letter Queue**: Store permanently failed messages
            5. **Alerting**: Notify operations team of critical issues
            6. **Manual Review**: Human intervention for complex failures
            """)

def show_data_storage():
    st.header("💾 Module 2: Raw Landing Storage (Unstructured)")
    st.markdown("""
    **Purpose:** Store raw payloads for replay, schema evolution, and lineage.
    Explore unstructured JSON storage patterns for each company with realistic synthetic data.
    """)
    
    # Company selection
    company = st.selectbox(
        "🏢 Choose Company Raw Storage:",
        ["🚗 Uber (Raw Events)", "🎬 Netflix (Raw Streams)", "🛒 Amazon (Raw Orders)", 
         "🏠 Airbnb (Raw Bookings)", "💰 NYSE (Raw Trades)"]
    )
    
    # Create tabs based on company selection
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 EDA Charts", "🛠️ Interactive Demo", "📋 Raw Data", "⚙️ Technical Stack", "📚 Schema Info"])
    # Initialize Module 2 SQLite database
    module2_conn = init_module2_database()
    
    # Determine company details
    if "Uber" in company:
        company_name = "Uber"
        currency = "AED"
    elif "Netflix" in company:
        company_name = "Netflix"
        currency = "USD"
    elif "Amazon" in company:
        company_name = "Amazon"
        currency = "AED"
    elif "Airbnb" in company:
        company_name = "Airbnb"
        currency = "AED"
    else:  # NYSE
        company_name = "NYSE"
        currency = "USD"
    
    # Populate database with synthetic raw landing data if not exists
    populate_module2_data(module2_conn, company_name)
    
    # Load data from SQLite database
    data = load_module2_data_from_db(module2_conn, company_name)
    
    with tab1:
        st.subheader(f"📊 Raw Landing EDA - {company_name} Dataset")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total Raw Records", f"{len(data):,}")
        with col2:
            avg_payload_size = data['payload_size_bytes'].mean()
            st.metric("📦 Avg Payload Size", f"{avg_payload_size:.0f} bytes")
        with col3:
            processed_rate = (data['processing_status'] == 'processed').sum() / len(data) * 100
            st.metric("✅ Processing Rate", f"{processed_rate:.1f}%")
        
        # Chart selection for raw landing analysis
        chart_type = st.selectbox(
            "Choose EDA Analysis:",
            ["📊 Storage Overview", "📈 Arrival Patterns", "🔍 Payload Analysis", "📊 Source Systems", "📋 Processing Status"]
        )
        
        if chart_type == "📊 Storage Overview":
            create_storage_overview_dashboard(data, company_name)
        elif chart_type == "📈 Arrival Patterns":
            create_arrival_patterns_charts(data, company_name)
        elif chart_type == "🔍 Payload Analysis": 
            create_payload_analysis_charts(data, company_name)
        elif chart_type == "📊 Source Systems":
            create_source_systems_charts(data, company_name)
        else:  # Processing Status
            create_processing_status_charts(data, company_name)
    
    with tab2:
        st.subheader(f"🛠️ {company_name} Raw Landing Demo")
        
        st.markdown(f"""
        **Interactive demonstration** of raw payload landing and processing for {company_name}.
        Simulate JSON parsing, schema validation, and partition management.
        """)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            demo_type = st.selectbox("Demo Type", ["JSON Parsing", "Schema Evolution", "Batch Processing"])
        with col2:
            n_samples = st.slider("Sample Size", 5, 50, 10)
        with col3:
            show_raw_json = st.checkbox("Show Raw JSON", value=False)
        
        if st.button(f"🚀 Start {company_name} Demo"):
            st.markdown("### 📊 Raw Landing Simulation")
            
            # Get sample records
            sample_data = data.sample(n=min(n_samples, len(data)))
            
            if demo_type == "JSON Parsing":
                st.markdown("**JSON Payload Parsing Demonstration:**")
                for idx, row in sample_data.head(5).iterrows():
                    with st.expander(f"Raw Record: {row['raw_id']}"):
                        if show_raw_json:
                            st.json(json.loads(row['raw_payload']))
                        else:
                            parsed = json.loads(row['raw_payload'])
                            st.write(f"**Source System**: {row['source_system']}")
                            st.write(f"**Payload Size**: {row['payload_size_bytes']} bytes")
                            st.write(f"**Schema Version**: {row['schema_version']}")
                            st.write(f"**Processing Status**: {row['processing_status']}")
                            st.write("**Key Fields Extracted:**")
                            if 'metadata' in parsed:
                                st.write(f"- Event Version: {parsed['metadata'].get('event_version')}")
                            if 'event_metadata' in parsed:
                                st.write(f"- Schema: {parsed['event_metadata'].get('schema_version')}")
                            
            elif demo_type == "Schema Evolution":
                st.markdown("**Schema Version Distribution:**")
                schema_counts = sample_data['schema_version'].value_counts()
                fig = px.bar(x=schema_counts.index, y=schema_counts.values, 
                           title="Schema Version Usage")
                st.plotly_chart(fig, use_container_width=True)
                
            else:  # Batch Processing
                st.markdown("**Batch Processing Simulation:**")
                processing_summary = sample_data['processing_status'].value_counts()
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("✅ Processed", processing_summary.get('processed', 0))
                with col2:
                    st.metric("⏳ Pending", processing_summary.get('pending', 0))
                with col3:
                    st.metric("❌ Failed", processing_summary.get('failed', 0))
    
    with tab3:
        st.subheader(f"📋 {company_name} Raw Landing Data & SQL Interface")
        
        # Database connection status
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info("✅ **SQLite Database Connected**")
        with col2:
            cursor = module2_conn.cursor()
            cursor.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            st.info(f"🔧 **Journal Mode**: {journal_mode}")
        with col3:
            cursor.execute(f"SELECT COUNT(*) FROM raw_landing WHERE company = '{company_name}'")
            total_records = cursor.fetchone()[0]
            st.info(f"📊 **DB Records**: {total_records:,}")
        
        # SQL Query Interface for raw landing
        st.markdown("### 💻 Raw Landing SQL Queries")
        st.markdown(f"""
        **Query the {company_name} raw landing data** directly from SQLite.
        Explore JSON payloads, partition keys, and processing status.
        """)
        
        # Pre-built query examples
        query_examples = {
            'Uber': [
                f"SELECT source_system, COUNT(*) as count FROM raw_landing WHERE company = 'Uber' GROUP BY source_system",
                f"SELECT processing_status, AVG(payload_size_bytes) as avg_size FROM raw_landing WHERE company = 'Uber' GROUP BY processing_status",
                f"SELECT partition_key, COUNT(*) as records FROM raw_landing WHERE company = 'Uber' GROUP BY partition_key ORDER BY records DESC LIMIT 10"
            ],
            'Netflix': [
                f"SELECT source_system, COUNT(*) as sessions FROM raw_landing WHERE company = 'Netflix' GROUP BY source_system",
                f"SELECT DATE(arrival_ts) as date, COUNT(*) as events FROM raw_landing WHERE company = 'Netflix' GROUP BY DATE(arrival_ts) ORDER BY date DESC",
                f"SELECT schema_version, COUNT(*) as records FROM raw_landing WHERE company = 'Netflix' GROUP BY schema_version"
            ],
            'Amazon': [
                f"SELECT source_system, AVG(payload_size_bytes) as avg_payload FROM raw_landing WHERE company = 'Amazon' GROUP BY source_system",
                f"SELECT processing_status, COUNT(*) as count FROM raw_landing WHERE company = 'Amazon' GROUP BY processing_status",
                f"SELECT SUBSTR(partition_key, 1, 20) as partition_prefix, COUNT(*) as records FROM raw_landing WHERE company = 'Amazon' GROUP BY partition_prefix"
            ],
            'Airbnb': [
                f"SELECT source_system, COUNT(*) as bookings FROM raw_landing WHERE company = 'Airbnb' GROUP BY source_system",
                f"SELECT processing_status, SUM(payload_size_bytes) as total_size FROM raw_landing WHERE company = 'Airbnb' GROUP BY processing_status",
                f"SELECT DATE(arrival_ts) as arrival_date, COUNT(*) as daily_events FROM raw_landing WHERE company = 'Airbnb' GROUP BY DATE(arrival_ts)"
            ],
            'NYSE': [
                f"SELECT source_system, COUNT(*) as trades FROM raw_landing WHERE company = 'NYSE' GROUP BY source_system",
                f"SELECT processing_status, COUNT(*) as status_count FROM raw_landing WHERE company = 'NYSE' GROUP BY processing_status",
                f"SELECT SUBSTR(arrival_ts, 1, 13) as hour, COUNT(*) as trades_per_hour FROM raw_landing WHERE company = 'NYSE' GROUP BY SUBSTR(arrival_ts, 1, 13) ORDER BY hour DESC LIMIT 24"
            ]
        }
        
        # Query selection
        col1, col2 = st.columns([2, 1])
        with col1:
            selected_example = st.selectbox(
                "Choose a raw storage query:",
                ["Custom Query"] + [f"Example {i+1}" for i in range(len(query_examples[company_name]))]
            )
        with col2:
            execute_query = st.button("🚀 Execute Query", type="primary")
        
        # Query input
        if selected_example == "Custom Query":
            sql_query = st.text_area(
                "Enter your SQL query:",
                value=f"SELECT * FROM raw_landing WHERE company = '{company_name}' LIMIT 10",
                height=100
            )
        else:
            example_idx = int(selected_example.split()[1]) - 1
            sql_query = query_examples[company_name][example_idx]
            st.code(sql_query, language="sql")
        
        # Execute query
        if execute_query and sql_query.strip():
            try:
                with st.spinner("Executing SQL query..."):
                    query_result = execute_module2_sql_query(module2_conn, sql_query)
                
                st.success(f"✅ Query executed! Returned {len(query_result)} rows.")
                
                if len(query_result) > 0:
                    st.dataframe(query_result, use_container_width=True)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 Rows", len(query_result))
                    with col2:
                        st.metric("📋 Columns", len(query_result.columns))
                    with col3:
                        memory_mb = query_result.memory_usage(deep=True).sum() / 1024**2
                        st.metric("💾 Size", f"{memory_mb:.1f} MB")
                else:
                    st.warning("Query returned no results.")
                    
            except Exception as e:
                st.error(f"❌ SQL Error: {str(e)}")
        
        st.markdown("---")
        st.markdown(f"### 📋 Sample Raw Landing Data")
        
        # Data filtering options
        col1, col2 = st.columns(2)
        with col1:
            n_rows = st.number_input("Rows to display", 5, 100, 20)
        with col2:
            status_filter = st.multiselect(
                "Filter by Processing Status",
                data['processing_status'].unique(),
                default=list(data['processing_status'].unique())
            )
        
        if status_filter:
            filtered_data = data[data['processing_status'].isin(status_filter)]
            st.dataframe(filtered_data.head(n_rows), use_container_width=True)
        
    with tab4:
        st.subheader(f"⚙️ {company_name} Raw Landing Technical Stack")
        st.markdown("**Technical architecture for raw data landing and storage**")
        
        if company_name == "Uber":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 🚗 Uber Raw Landing Architecture
                
                **Data Sources:**
                - Mobile app events (rider/driver interactions)
                - GPS tracking streams
                - Payment processing events
                - Trip lifecycle events
                
                **Raw Landing Layer:**
                - **Storage:** Amazon S3 (Data Lake)
                - **Format:** JSON payloads with metadata
                - **Partitioning:** By date/region for performance
                - **Compression:** Snappy for balance of speed/size
                """)
                
            with col2:
                st.markdown("""
                **Technical Components:**
                - **Ingestion:** Apache Kafka (high-throughput streaming)
                - **Schema Registry:** Confluent Schema Registry
                - **Processing:** Apache Spark (batch processing)
                - **Monitoring:** DataDog, custom metrics
                - **Security:** IAM roles, encryption at rest/transit
                
                **Data Governance:**
                - **Retention:** 7 years for compliance
                - **Access Control:** Role-based permissions
                - **Audit Trail:** All data access logged
                - **Quality Checks:** Schema validation on ingestion
                """)
                
            st.markdown("---")
            st.markdown("""
            **Raw Payload Example (Uber Trip Event):**
            ```json
            {
              "event_id": "evt_uber_20241201_001",
              "timestamp": "2024-12-01T14:30:00Z",
              "source_system": "uber_mobile_app",
              "event_type": "trip_started",
              "payload": {
                "trip_id": "trip_789xyz",
                "rider_id": "rider_456abc",
                "driver_id": "driver_123def",
                "pickup_location": {"lat": 40.7589, "lng": -73.9851},
                "estimated_fare": 15.50,
                "device_info": {"os": "iOS", "version": "15.4"}
              },
              "metadata": {
                "app_version": "4.382.10004",
                "region": "NYC",
                "file_size_bytes": 1024
              }
            }
            ```
            """)
            
        elif company_name == "Netflix":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 🎬 Netflix Raw Landing Architecture
                
                **Data Sources:**
                - Video streaming events
                - User interaction logs
                - Content recommendation clicks
                - Device performance metrics
                
                **Raw Landing Layer:**
                - **Storage:** Amazon S3 (multi-region)
                - **Format:** Avro for schema evolution
                - **Partitioning:** By hour/content_type
                - **Replication:** Cross-region for disaster recovery
                """)
                
            with col2:
                st.markdown("""
                **Technical Components:**
                - **Ingestion:** Apache Kafka (200+ GB/day)
                - **Stream Processing:** Apache Flink
                - **Batch Processing:** Apache Spark on EMR
                - **Orchestration:** Apache Airflow
                - **Monitoring:** Custom tools + Grafana
                
                **Performance Optimizations:**
                - **Compression:** GZIP for cold storage
                - **Indexing:** Elasticsearch for log search
                - **Caching:** Redis for frequent access patterns
                - **CDN Integration:** CloudFront for global access
                """)
                
            st.markdown("---")
            st.markdown("""
            **Raw Payload Example (Netflix Viewing Event):**
            ```json
            {
              "event_id": "evt_netflix_20241201_001",
              "timestamp": "2024-12-01T20:15:30Z",
              "source_system": "netflix_player",
              "event_type": "playback_quality_change",
              "payload": {
                "user_id": "user_987xyz",
                "content_id": "movie_654abc",
                "session_id": "sess_321def",
                "quality_from": "720p",
                "quality_to": "1080p",
                "bandwidth_mbps": 25.4,
                "device_type": "smart_tv"
              },
              "metadata": {
                "player_version": "6.0045.123.321",
                "country": "US",
                "isp": "comcast"
              }
            }
            ```
            """)
            
        elif company_name == "Amazon":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 📦 Amazon Raw Landing Architecture
                
                **Data Sources:**
                - E-commerce transaction logs
                - Inventory management events
                - Customer behavior tracking
                - Supply chain data feeds
                
                **Raw Landing Layer:**
                - **Storage:** S3 (petabyte scale)
                - **Format:** Mixed (JSON, Parquet, CSV)
                - **Tiering:** Intelligent tiering for cost optimization
                - **Lifecycle:** Auto-archival to Glacier
                """)
                
            with col2:
                st.markdown("""
                **Technical Components:**
                - **Ingestion:** Amazon Kinesis Data Firehose
                - **Processing:** AWS Glue + Lambda
                - **Analytics:** Amazon Athena for querying
                - **ML Pipeline:** SageMaker integration
                - **Monitoring:** CloudWatch + X-Ray
                
                **Scalability Features:**
                - **Auto-scaling:** Based on ingestion volume
                - **Load Balancing:** Application Load Balancer
                - **Fault Tolerance:** Multi-AZ deployment
                - **Cost Optimization:** Spot instances for processing
                """)
                
            st.markdown("---")
            st.markdown("""
            **Raw Payload Example (Amazon Order Event):**
            ```json
            {
              "event_id": "evt_amazon_20241201_001",
              "timestamp": "2024-12-01T16:45:22Z",
              "source_system": "amazon_checkout",
              "event_type": "order_placed",
              "payload": {
                "order_id": "order_789xyz123",
                "customer_id": "cust_456abc789",
                "items": [
                  {"product_id": "B08N5WRWNW", "quantity": 2, "price_usd": 29.99},
                  {"product_id": "B07FZ8S74R", "quantity": 1, "price_usd": 199.00}
                ],
                "shipping_address": {"country": "US", "zip": "10001"},
                "payment_method": "credit_card"
              },
              "metadata": {
                "user_agent": "Mozilla/5.0...",
                "warehouse": "fulfillment_center_nyc1"
              }
            }
            ```
            """)
            
        elif company_name == "Airbnb":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 🏠 Airbnb Raw Landing Architecture
                
                **Data Sources:**
                - Property search events
                - Booking lifecycle data
                - Host-guest messaging logs
                - Pricing optimization data
                
                **Raw Landing Layer:**
                - **Storage:** S3 + HDFS hybrid
                - **Format:** JSON with nested structures
                - **Partitioning:** By region/booking_date
                - **Backup:** Cross-region replication
                """)
                
            with col2:
                st.markdown("""
                **Technical Components:**
                - **Ingestion:** Apache Kafka + Airflow
                - **Processing:** Spark on Kubernetes
                - **Workflow:** Airflow (1000+ DAGs)
                - **Search:** Elasticsearch cluster
                - **Monitoring:** Datadog + internal tools
                
                **Data Quality:**
                - **Validation:** Great Expectations framework
                - **Lineage:** Apache Atlas integration
                - **Testing:** Data unit tests in CI/CD
                - **Alerts:** PagerDuty for data quality issues
                """)
                
            st.markdown("---")
            st.markdown("""
            **Raw Payload Example (Airbnb Booking Event):**
            ```json
            {
              "event_id": "evt_airbnb_20241201_001",
              "timestamp": "2024-12-01T11:20:15Z",
              "source_system": "airbnb_booking_service",
              "event_type": "booking_confirmed",
              "payload": {
                "booking_id": "booking_abc123xyz",
                "host_id": "host_987def",
                "guest_id": "guest_654ghi",
                "property_id": "prop_321jkl",
                "check_in": "2024-12-15",
                "check_out": "2024-12-20",
                "total_price_usd": 850.00,
                "guests": 4
              },
              "metadata": {
                "booking_channel": "mobile_app",
                "market": "san_francisco",
                "host_response_time": "2_hours"
              }
            }
            ```
            """)
            
        elif company_name == "NYSE":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 💰 NYSE Raw Landing Architecture
                
                **Data Sources:**
                - High-frequency trading data
                - Market data feeds
                - Order book snapshots
                - Regulatory compliance logs
                
                **Raw Landing Layer:**
                - **Storage:** Time-series databases + S3
                - **Format:** Binary + JSON for different data types
                - **Latency:** Sub-millisecond requirements
                - **Retention:** 10+ years for compliance
                """)
                
            with col2:
                st.markdown("""
                **Technical Components:**
                - **Ingestion:** Custom high-speed data feeds
                - **Processing:** In-memory computing (Hazelcast)
                - **Storage:** InfluxDB + TimescaleDB
                - **Networking:** Ultra-low latency networks
                - **Compliance:** SEC/FINRA reporting tools
                
                **Performance Critical:**
                - **Latency:** <1ms for critical paths
                - **Throughput:** 1M+ messages/second
                - **Availability:** 99.999% uptime requirement
                - **Security:** Multiple encryption layers
                """)
                
            st.markdown("---")
            st.markdown("""
            **Raw Payload Example (NYSE Trade Event):**
            ```json
            {
              "event_id": "evt_nyse_20241201_001",
              "timestamp": "2024-12-01T14:30:00.123456Z",
              "source_system": "nyse_trading_floor",
              "event_type": "trade_executed",
              "payload": {
                "ticker": "AAPL",
                "price": 193.75,
                "volume": 500,
                "trade_id": "TRD_789ABC123",
                "buyer_firm": "GS",
                "seller_firm": "MS",
                "execution_venue": "NYSE_ARCA"
              },
              "metadata": {
                "exchange": "NYSE",
                "trade_type": "regular_way",
                "settlement_date": "2024-12-03"
              }
            }
            ```
            """)
        
        st.markdown("---")
        st.markdown("### 🔧 Common Technical Patterns Across Companies")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            **Ingestion Patterns:**
            - Event streaming (Kafka)
            - Batch file uploads
            - Real-time APIs
            - Change data capture (CDC)
            """)
            
        with col2:
            st.markdown("""
            **Storage Patterns:**
            - Object storage (S3)
            - Data lakes architecture
            - Partitioned by time/region
            - Compression for cost efficiency
            """)
            
        with col3:
            st.markdown("""
            **Processing Patterns:**
            - Schema-on-read approach
            - Metadata catalogs
            - Data lineage tracking
            - Quality validation gates
            """)
    
    with tab5:
        st.subheader(f"📚 {company_name} Raw Landing Schema")
        st.markdown("**Module 2 Raw Landing Schema specification**")
        
        st.markdown("### 📋 Core Raw Landing Table Schema")
        
        # Display the raw_landing table schema
        schema_df = pd.DataFrame({
            'Column': ['raw_id', 'company', 'source_system', 'raw_payload', 'file_name', 'arrival_ts', 'partition_key', 'payload_size_bytes', 'processing_status'],
            'Type': ['TEXT PRIMARY KEY', 'TEXT NOT NULL', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'TEXT', 'INTEGER', 'TEXT DEFAULT "pending"'],
            'Description': [
                'Unique identifier for raw data record',
                'Company name (uber, netflix, amazon, airbnb, nyse)',
                'Source system that generated the data',
                'Raw JSON payload containing the actual data',
                'Original file name or source identifier',
                'Timestamp when data arrived in raw landing',
                'Partition key for data organization (date-based)',
                'Size of raw payload in bytes',
                'Processing status (pending, processed, failed)'
            ]
        })
        
        st.dataframe(schema_df, use_container_width=True)
        
        st.markdown("### 🗂️ Schema Design Principles")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Raw Landing Design:**
            - **Schema-on-Read**: Store data first, define schema later
            - **JSON Payloads**: Flexible nested structure support
            - **Metadata Tracking**: Capture source and lineage info
            - **Partition Strategy**: Enable efficient querying
            - **Processing Status**: Track data processing lifecycle
            """)
            
        with col2:
            st.markdown("""
            **Benefits:**
            - **Flexibility**: Handle schema evolution gracefully
            - **Speed**: Fast ingestion without validation delays
            - **Replay**: Ability to reprocess raw data
            - **Audit**: Complete lineage and processing history
            - **Compliance**: Long-term retention for regulations
            """)
        
        st.markdown("---")
        st.markdown("### 🏗️ SQLite Database Setup")
        
        st.code("""
-- Create raw_landing table for Module 2
CREATE TABLE IF NOT EXISTS raw_landing (
    raw_id TEXT PRIMARY KEY,
    company TEXT NOT NULL,
    source_system TEXT,
    raw_payload TEXT,          -- JSON data stored as text
    file_name TEXT,
    arrival_ts TEXT,           -- ISO timestamp
    partition_key TEXT,        -- Usually date-based (YYYY-MM-DD)
    payload_size_bytes INTEGER DEFAULT 0,
    processing_status TEXT DEFAULT 'pending'  -- pending, processed, failed
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_company ON raw_landing(company);
CREATE INDEX IF NOT EXISTS idx_raw_arrival_ts ON raw_landing(arrival_ts);
CREATE INDEX IF NOT EXISTS idx_raw_partition ON raw_landing(partition_key);
CREATE INDEX IF NOT EXISTS idx_raw_status ON raw_landing(processing_status);

-- SQLite optimizations for raw data workloads
PRAGMA journal_mode = WAL;          -- Better concurrency
PRAGMA synchronous = NORMAL;        -- Balance safety/performance
PRAGMA cache_size = -64000;         -- 64MB cache
PRAGMA temp_store = memory;         -- Temp data in memory
        """, language='sql')
        
        st.markdown("### 📊 Raw Payload Structure by Company")
        
        # Show example payload structures for each company
        if company_name == "Uber":
            st.markdown("**Uber Raw Payload Structure:**")
            st.code("""
{
  "ride_data": {
    "trip_id": "string",
    "driver_id": "string", 
    "rider_id": "string",
    "status": "requested|accepted|started|completed|cancelled",
    "pickup_location": {"lat": float, "lng": float},
    "dropoff_location": {"lat": float, "lng": float},
    "estimated_fare": float,
    "actual_fare": float
  },
  "timestamps": {
    "request_time": "ISO datetime",
    "pickup_time": "ISO datetime", 
    "dropoff_time": "ISO datetime"
  },
  "metadata": {
    "app_version": "string",
    "device_type": "string",
    "city": "string"
  }
}
            """, language='json')
            
        elif company_name == "Netflix":
            st.markdown("**Netflix Raw Payload Structure:**")
            st.code("""
{
  "viewing_data": {
    "user_id": "string",
    "content_id": "string",
    "session_id": "string",
    "event_type": "play|pause|stop|seek|quality_change",
    "playback_position_sec": integer,
    "video_quality": "string",
    "audio_language": "string"
  },
  "device_info": {
    "device_type": "smart_tv|mobile|desktop|tablet",
    "os": "string",
    "app_version": "string"
  },
  "network_data": {
    "bandwidth_mbps": float,
    "connection_type": "string",
    "isp": "string"
  }
}
            """, language='json')
            
        elif company_name == "Amazon":
            st.markdown("**Amazon Raw Payload Structure:**")
            st.code("""
{
  "order_data": {
    "order_id": "string",
    "customer_id": "string", 
    "items": [
      {
        "product_id": "string",
        "quantity": integer,
        "unit_price": float,
        "category": "string"
      }
    ],
    "order_total": float,
    "shipping_cost": float,
    "tax_amount": float
  },
  "fulfillment": {
    "warehouse": "string",
    "shipping_method": "string",
    "estimated_delivery": "ISO date"
  },
  "customer_info": {
    "shipping_address": {"country": "string", "zip": "string"},
    "payment_method": "string"
  }
}
            """, language='json')
            
        elif company_name == "Airbnb":
            st.markdown("**Airbnb Raw Payload Structure:**")
            st.code("""
{
  "booking_data": {
    "booking_id": "string",
    "host_id": "string",
    "guest_id": "string", 
    "property_id": "string",
    "check_in_date": "ISO date",
    "check_out_date": "ISO date",
    "total_nights": integer,
    "total_price": float,
    "guest_count": integer
  },
  "property_info": {
    "property_type": "string",
    "city": "string",
    "country": "string",
    "amenities": ["string"]
  },
  "booking_details": {
    "booking_channel": "web|mobile|api",
    "instant_book": boolean,
    "cancellation_policy": "string"
  }
}
            """, language='json')
            
        elif company_name == "NYSE":
            st.markdown("**NYSE Raw Payload Structure:**")
            st.code("""
{
  "trade_data": {
    "ticker": "string",
    "trade_price": float,
    "trade_volume": integer,
    "trade_timestamp": "ISO datetime with microseconds",
    "trade_id": "string",
    "execution_venue": "string"
  },
  "market_data": {
    "bid_price": float,
    "ask_price": float,
    "bid_size": integer,
    "ask_size": integer,
    "last_price": float
  },
  "regulatory": {
    "trade_type": "regular_way|odd_lot|block",
    "settlement_date": "ISO date",
    "reporting_party": "string"
  }
}
            """, language='json')
        
        st.markdown("---")
        st.markdown("### 🔄 Data Processing Lifecycle")
        
        lifecycle_df = pd.DataFrame({
            'Stage': ['Raw Ingestion', 'Schema Validation', 'Payload Storage', 'Processing Queue', 'Transformation', 'Quality Checks', 'Archive'],
            'Status': ['pending', 'pending', 'pending', 'processing', 'processing', 'processed/failed', 'processed'],
            'Description': [
                'Data arrives from source systems',
                'Basic JSON validity checks',
                'Store in raw_landing table',
                'Queue for downstream processing',
                'Extract and transform to staging',
                'Validate data quality rules',
                'Move to long-term storage'
            ],
            'Retention': ['90 days', '90 days', '90 days', '1 day', '30 days', '365 days', '7+ years']
        })
        
        st.dataframe(lifecycle_df, use_container_width=True)
        
        st.markdown("### 📈 Schema Evolution Strategy")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Handling Schema Changes:**
            - **Additive Changes**: New fields added to JSON
            - **Field Renames**: Map old → new field names
            - **Type Changes**: Handle gracefully with defaults
            - **Version Tracking**: Track schema versions in metadata
            """)
            
        with col2:
            st.markdown("""
            **Best Practices:**
            - **Backward Compatible**: Old schemas still work
            - **Default Values**: Provide sensible defaults
            - **Migration Scripts**: Transform historical data
            - **Documentation**: Track all schema changes
            """)

# ============================================================================
# MODULE 2: RAW LANDING - CHART HELPER FUNCTIONS  
# ============================================================================

def create_storage_overview_dashboard(data, company_name):
    """Create storage overview dashboard for raw landing data"""
    st.markdown(f"### 📊 {company_name} Raw Landing Overview")
    
    col1, col2 = st.columns(2)
    with col1:
        # Source system distribution
        source_counts = data['source_system'].value_counts()
        fig = px.pie(values=source_counts.values, names=source_counts.index,
                    title="Data Sources Distribution")
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Processing status distribution
        status_counts = data['processing_status'].value_counts()
        fig = px.bar(x=status_counts.index, y=status_counts.values,
                    title="Processing Status")
        st.plotly_chart(fig, use_container_width=True)
    
    # Payload size distribution
    fig = px.histogram(data, x='payload_size_bytes', nbins=50,
                     title="Payload Size Distribution (bytes)")
    st.plotly_chart(fig, use_container_width=True)

def create_arrival_patterns_charts(data, company_name):
    """Create arrival pattern analysis charts"""
    st.markdown(f"### 📈 {company_name} Arrival Patterns")
    
    # Convert arrival_ts to datetime
    data['arrival_datetime'] = pd.to_datetime(data['arrival_ts'])
    data['arrival_hour'] = data['arrival_datetime'].dt.hour
    data['arrival_date'] = data['arrival_datetime'].dt.date
    
    col1, col2 = st.columns(2)
    with col1:
        # Hourly arrival pattern
        hourly_counts = data['arrival_hour'].value_counts().sort_index()
        fig = px.line(x=hourly_counts.index, y=hourly_counts.values,
                     title="Data Arrival by Hour",
                     labels={'x': 'Hour of Day', 'y': 'Event Count'})
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Daily arrival pattern
        daily_counts = data['arrival_date'].value_counts().sort_index()
        fig = px.line(x=daily_counts.index, y=daily_counts.values,
                     title="Daily Data Arrival Volume",
                     labels={'x': 'Date', 'y': 'Event Count'})
        st.plotly_chart(fig, use_container_width=True)

def create_payload_analysis_charts(data, company_name):
    """Create payload analysis charts"""
    st.markdown(f"### 🔍 {company_name} Payload Analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        # Payload size by source system
        fig = px.box(data, x='source_system', y='payload_size_bytes',
                    title="Payload Size by Source System")
        fig.update_xaxis(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Schema version distribution
        schema_counts = data['schema_version'].value_counts()
        fig = px.pie(values=schema_counts.values, names=schema_counts.index,
                    title="Schema Version Distribution")
        st.plotly_chart(fig, use_container_width=True)

def create_source_systems_charts(data, company_name):
    """Create source system analysis charts"""
    st.markdown(f"### 📊 {company_name} Source Systems Analysis")
    
    # Source system metrics
    source_metrics = data.groupby('source_system').agg({
        'raw_id': 'count',
        'payload_size_bytes': ['mean', 'sum'],
        'processing_status': lambda x: (x == 'processed').sum() / len(x) * 100
    }).round(2)
    
    source_metrics.columns = ['Record Count', 'Avg Payload Size', 'Total Payload Size', 'Success Rate %']
    
    st.dataframe(source_metrics, use_container_width=True)
    
    # Source system performance
    fig = px.scatter(data, x='payload_size_bytes', y='source_system',
                    color='processing_status',
                    title="Source System Performance vs Payload Size")
    st.plotly_chart(fig, use_container_width=True)

def create_processing_status_charts(data, company_name):
    """Create processing status analysis charts"""
    st.markdown(f"### 📋 {company_name} Processing Status Analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        # Processing status by source
        status_by_source = pd.crosstab(data['source_system'], data['processing_status'])
        fig = px.bar(status_by_source, 
                    title="Processing Status by Source System",
                    barmode='stack')
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Processing success rate over time
        data['arrival_datetime'] = pd.to_datetime(data['arrival_ts'])
        daily_success = data.groupby(data['arrival_datetime'].dt.date).agg({
            'processing_status': lambda x: (x == 'processed').sum() / len(x) * 100
        }).round(1)
        
        fig = px.line(x=daily_success.index, y=daily_success['processing_status'],
                     title="Daily Processing Success Rate (%)")
        st.plotly_chart(fig, use_container_width=True)

def show_etl_pipelines():
    st.header("🔄 ETL/ELT Pipelines")
    st.markdown("Learn about Extract, Transform, Load processes and orchestration")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📚 ETL vs ELT", "🛠️ Pipeline Builder", "📊 ETL Analytics", "🔄 ETL Flow Charts", "🏢 Real Examples"])
    
    with tab1:
        st.subheader("ETL vs ELT: What's the difference?")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            ### 🔄 ETL (Extract, Transform, Load)
            **Traditional approach**
            
            **Process Flow:**
            1. **Extract** data from sources
            2. **Transform** data in staging area
            3. **Load** transformed data to destination
            
            **Characteristics:**
            - Transform before loading
            - Requires staging area
            - Better for structured data
            - More processing upfront
            
            **Tools:** Talend, Informatica, SSIS, Pentaho
            """)
            
            # ETL Flow visualization
            etl_flow = pd.DataFrame({
                'Step': ['Source', 'Staging', 'Transform', 'Destination'],
                'Data_Volume': [100, 100, 80, 80],
                'Processing': [0, 20, 80, 0]
            })
            fig_etl = px.bar(etl_flow, x='Step', y='Data_Volume', 
                           title='ETL Flow (Transform before Load)')
            st.plotly_chart(fig_etl, use_container_width=True)
            
        with col2:
            st.markdown("""
            ### 🔄 ELT (Extract, Load, Transform)
            **Modern cloud approach**
            
            **Process Flow:**
            1. **Extract** data from sources
            2. **Load** raw data to destination
            3. **Transform** data in destination system
            
            **Characteristics:**
            - Load raw data first
            - Transform in powerful cloud systems
            - Better for big data & unstructured
            - Leverages cloud compute power
            
            **Tools:** Fivetran, Stitch, dbt, AWS Glue
            """)
            
            # ELT Flow visualization  
            elt_flow = pd.DataFrame({
                'Step': ['Source', 'Raw Load', 'Destination', 'Transform'],
                'Data_Volume': [100, 100, 100, 85],
                'Processing': [0, 10, 20, 70]
            })
            fig_elt = px.bar(elt_flow, x='Step', y='Data_Volume',
                           title='ELT Flow (Transform after Load)')
            st.plotly_chart(fig_elt, use_container_width=True)
        
        # When to use which
        st.markdown("---")
        st.subheader("🎯 When to use ETL vs ELT?")
        
        choice_factors = st.selectbox("Choose a factor:", 
            ["Data Volume", "Data Type", "Compute Resources", "Cost"])
        
        if choice_factors == "Data Volume":
            st.markdown("""
            - **Small to Medium Data (< 1TB):** ETL works fine
            - **Big Data (> 1TB):** ELT leverages cloud scale better
            - **Real-time streams:** ELT for immediate loading, transform later
            """)
        elif choice_factors == "Data Type":
            st.markdown("""
            - **Structured Data (SQL tables):** ETL traditional strength
            - **Semi-structured (JSON, XML):** ELT handles variety better  
            - **Unstructured (logs, images):** ELT stores raw, transform as needed
            """)
        elif choice_factors == "Compute Resources":
            st.markdown("""
            - **Limited processing power:** ETL with dedicated transform servers
            - **Cloud-native:** ELT leverages scalable cloud compute
            - **On-premise:** ETL might be more cost-effective
            """)
    
    with tab2:
        st.subheader("🛠️ Interactive Pipeline Builder")
        
        # Pipeline configuration
        st.markdown("### Build Your Pipeline")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 📥 Extract")
            source_type = st.selectbox("Data Source:", 
                ["Database", "API", "Files", "Streaming"])
            if source_type == "Database":
                db_type = st.selectbox("Database Type:", ["MySQL", "PostgreSQL", "MongoDB"])
            elif source_type == "API":
                api_type = st.selectbox("API Type:", ["REST", "GraphQL", "SOAP"])
            elif source_type == "Files":
                file_type = st.selectbox("File Type:", ["CSV", "JSON", "Parquet", "XML"])
        
        with col2:
            st.markdown("#### 🔄 Transform")
            pipeline_type = st.radio("Pipeline Type:", ["ETL", "ELT"])
            
            transforms = st.multiselect("Select Transformations:", 
                ["Data Cleaning", "Type Conversion", "Aggregation", 
                 "Joining", "Filtering", "Enrichment"])
            
        with col3:
            st.markdown("#### 📤 Load")
            destination = st.selectbox("Destination:", 
                ["Data Warehouse", "Data Lake", "Database", "API"])
            
            load_mode = st.selectbox("Load Mode:", 
                ["Full Load", "Incremental", "Upsert", "Append"])
        
        if st.button("Generate Pipeline Code"):
            st.markdown("### 🐍 Generated Python Pipeline")
            
            pipeline_code = f"""
import pandas as pd
from datetime import datetime

def {pipeline_type.lower()}_pipeline():
    # Extract from {source_type}
    print(f"Extracting data from {source_type}...")
    
    # Sample extraction code
    if "{source_type}" == "Database":
        data = pd.read_sql("SELECT * FROM source_table", connection)
    elif "{source_type}" == "API":
        data = pd.read_json("https://api.example.com/data")
    elif "{source_type}" == "Files":
        data = pd.read_csv("source_file.csv")
    
    print(f"Extracted {{len(data)}} records")
    
    {"# Transform data (ETL approach)" if pipeline_type == "ETL" else "# Load raw data first (ELT approach)"}
    {"transform_data(data)" if pipeline_type == "ETL" else "load_raw_data(data)"}
    
    # Transformations: {', '.join(transforms) if transforms else 'None selected'}
    {"for transform in transforms:" if transforms else "# No transformations selected"}
    {"    data = apply_transform(data, transform)" if transforms else ""}
    
    # Load to {destination}
    print(f"Loading to {destination} using {load_mode} mode...")
    
    return data

# Run pipeline
result = {pipeline_type.lower()}_pipeline()
print("Pipeline completed successfully!")
            """
            st.code(pipeline_code, language='python')
            
            # Pipeline visualization
            st.markdown("### 📊 Pipeline Flow")
            
            if pipeline_type == "ETL":
                flow_data = pd.DataFrame({
                    'Stage': ['Extract', 'Transform', 'Load'],
                    'Duration': [2, 8, 3],
                    'Data_Size': [100, 85, 85]
                })
            else:
                flow_data = pd.DataFrame({
                    'Stage': ['Extract', 'Load', 'Transform'],
                    'Duration': [2, 3, 6], 
                    'Data_Size': [100, 100, 85]
                })
            
            fig = px.line(flow_data, x='Stage', y=['Duration', 'Data_Size'],
                         title=f'{pipeline_type} Pipeline Metrics')
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("📊 ETL Pipeline Analytics")
        
        # Generate sample ETL pipeline performance data
        np.random.seed(44)
        n_runs = 100
        
        etl_data = pd.DataFrame({
            'pipeline_id': np.random.choice(['user_pipeline', 'transaction_pipeline', 'analytics_pipeline', 'ml_pipeline'], n_runs),
            'execution_time_min': np.random.lognormal(mean=2, sigma=0.8, size=n_runs).round(1),
            'records_processed': np.random.exponential(scale=50000, size=n_runs).round(0).astype(int),
            'extract_time_min': np.random.gamma(shape=2, scale=5, size=n_runs).round(1),
            'transform_time_min': np.random.gamma(shape=3, scale=8, size=n_runs).round(1),
            'load_time_min': np.random.gamma(shape=1.5, scale=3, size=n_runs).round(1),
            'success_rate': np.random.beta(a=9, b=1, size=n_runs) * 100,
            'data_quality_score': np.random.beta(a=8, b=2, size=n_runs) * 100,
            'cpu_usage_percent': np.random.normal(loc=60, scale=15, size=n_runs).clip(10, 100).round(1),
            'memory_usage_gb': np.random.exponential(scale=8, size=n_runs).round(1),
            'error_count': np.random.poisson(lam=2, size=n_runs),
            'pipeline_type': np.random.choice(['Batch', 'Streaming', 'Micro-batch'], n_runs, p=[0.6, 0.25, 0.15])
        })
        
        # Add derived metrics
        etl_data['throughput'] = etl_data['records_processed'] / etl_data['execution_time_min']
        etl_data['efficiency'] = (etl_data['records_processed'] / 1000) / etl_data['cpu_usage_percent']
        
        chart_type = st.selectbox("Choose ETL Analytics:", 
            ["Pipeline Performance", "Resource Utilization", "Data Quality Metrics", "Error Analysis", "Throughput Analysis"])
        
        if chart_type == "Pipeline Performance":
            col1, col2 = st.columns(2)
            
            with col1:
                # Execution time by pipeline
                fig_perf1 = px.box(etl_data, x='pipeline_id', y='execution_time_min',
                                  title='Execution Time by Pipeline',
                                  labels={'execution_time_min': 'Execution Time (min)', 'pipeline_id': 'Pipeline'})
                fig_perf1.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig_perf1, use_container_width=True)
                
            with col2:
                # Success rate distribution
                fig_perf2 = px.histogram(etl_data, x='success_rate', nbins=20,
                                        title='Success Rate Distribution',
                                        labels={'success_rate': 'Success Rate (%)', 'count': 'Pipeline Runs'})
                fig_perf2.update_layout(height=400)
                st.plotly_chart(fig_perf2, use_container_width=True)
            
            # ETL stages breakdown
            stage_data = pd.DataFrame({
                'Stage': ['Extract', 'Transform', 'Load'] * len(etl_data),
                'Time': list(etl_data['extract_time_min']) + list(etl_data['transform_time_min']) + list(etl_data['load_time_min']),
                'Pipeline': list(etl_data['pipeline_id']) * 3
            })
            
            fig_perf3 = px.violin(stage_data, x='Stage', y='Time', color='Stage',
                                 title='ETL Stage Time Distribution')
            st.plotly_chart(fig_perf3, use_container_width=True)
            
        elif chart_type == "Resource Utilization":
            col1, col2 = st.columns(2)
            
            with col1:
                # CPU vs Memory usage
                fig_res1 = px.scatter(etl_data, x='cpu_usage_percent', y='memory_usage_gb',
                                     color='pipeline_type', size='records_processed',
                                     title='Resource Usage by Pipeline Type',
                                     labels={'cpu_usage_percent': 'CPU Usage (%)', 'memory_usage_gb': 'Memory Usage (GB)'})
                fig_res1.update_layout(height=400)
                st.plotly_chart(fig_res1, use_container_width=True)
                
            with col2:
                # Resource usage by pipeline
                fig_res2 = px.box(etl_data, x='pipeline_id', y='cpu_usage_percent',
                                 title='CPU Usage by Pipeline',
                                 labels={'cpu_usage_percent': 'CPU Usage (%)', 'pipeline_id': 'Pipeline'})
                fig_res2.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig_res2, use_container_width=True)
            
            # Memory efficiency
            fig_res3 = px.bar(etl_data.groupby('pipeline_type')['memory_usage_gb'].mean().reset_index(),
                             x='pipeline_type', y='memory_usage_gb',
                             title='Average Memory Usage by Pipeline Type',
                             labels={'memory_usage_gb': 'Average Memory Usage (GB)', 'pipeline_type': 'Pipeline Type'})
            st.plotly_chart(fig_res3, use_container_width=True)
            
        elif chart_type == "Data Quality Metrics":
            col1, col2 = st.columns(2)
            
            with col1:
                # Data quality by pipeline
                fig_qual1 = px.violin(etl_data, x='pipeline_id', y='data_quality_score',
                                     title='Data Quality Score by Pipeline',
                                     labels={'data_quality_score': 'Quality Score (%)', 'pipeline_id': 'Pipeline'})
                fig_qual1.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig_qual1, use_container_width=True)
                
            with col2:
                # Quality vs execution time
                fig_qual2 = px.scatter(etl_data, x='execution_time_min', y='data_quality_score',
                                      color='pipeline_type',
                                      title='Quality vs Execution Time',
                                      labels={'execution_time_min': 'Execution Time (min)', 'data_quality_score': 'Quality Score (%)'})
                fig_qual2.update_layout(height=400)
                st.plotly_chart(fig_qual2, use_container_width=True)
            
            # Quality trends
            quality_by_type = etl_data.groupby('pipeline_type')['data_quality_score'].mean().sort_values(ascending=False)
            fig_qual3 = px.bar(x=quality_by_type.index, y=quality_by_type.values,
                              title='Average Data Quality by Pipeline Type',
                              labels={'x': 'Pipeline Type', 'y': 'Average Quality Score (%)'})
            st.plotly_chart(fig_qual3, use_container_width=True)
            
        elif chart_type == "Error Analysis":
            col1, col2 = st.columns(2)
            
            with col1:
                # Error count distribution
                fig_err1 = px.histogram(etl_data, x='error_count', nbins=15,
                                       title='Error Count Distribution',
                                       labels={'error_count': 'Number of Errors', 'count': 'Pipeline Runs'})
                fig_err1.update_layout(height=400)
                st.plotly_chart(fig_err1, use_container_width=True)
                
            with col2:
                # Errors by pipeline type
                fig_err2 = px.box(etl_data, x='pipeline_type', y='error_count',
                                 title='Errors by Pipeline Type',
                                 labels={'error_count': 'Number of Errors', 'pipeline_type': 'Pipeline Type'})
                fig_err2.update_layout(height=400)
                st.plotly_chart(fig_err2, use_container_width=True)
            
            # Error correlation with performance
            fig_err3 = px.scatter(etl_data, x='error_count', y='execution_time_min',
                                 color='pipeline_id', size='records_processed',
                                 title='Error Impact on Performance',
                                 labels={'error_count': 'Number of Errors', 'execution_time_min': 'Execution Time (min)'})
            st.plotly_chart(fig_err3, use_container_width=True)
            
        elif chart_type == "Throughput Analysis":
            col1, col2 = st.columns(2)
            
            with col1:
                # Throughput by pipeline
                fig_thru1 = px.box(etl_data, x='pipeline_id', y='throughput',
                                  title='Throughput by Pipeline',
                                  labels={'throughput': 'Records/Min', 'pipeline_id': 'Pipeline'})
                fig_thru1.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig_thru1, use_container_width=True)
                
            with col2:
                # Efficiency vs records processed
                fig_thru2 = px.scatter(etl_data, x='records_processed', y='efficiency',
                                      color='pipeline_type',
                                      title='Pipeline Efficiency',
                                      labels={'records_processed': 'Records Processed', 'efficiency': 'Efficiency Score'})
                fig_thru2.update_layout(height=400)
                st.plotly_chart(fig_thru2, use_container_width=True)
            
            # Throughput trends
            throughput_stats = etl_data.groupby('pipeline_type')['throughput'].agg(['mean', 'std']).reset_index()
            fig_thru3 = go.Figure()
            fig_thru3.add_trace(go.Bar(x=throughput_stats['pipeline_type'], 
                                      y=throughput_stats['mean'],
                                      error_y=dict(type='data', array=throughput_stats['std']),
                                      name='Average Throughput'))
            fig_thru3.update_layout(title='Throughput by Pipeline Type (with std dev)',
                                   xaxis_title='Pipeline Type',
                                   yaxis_title='Throughput (Records/Min)')
            st.plotly_chart(fig_thru3, use_container_width=True)
    
    with tab4:
        st.subheader("🔄 ETL Process Flow Charts")
        
        flow_type = st.selectbox("Choose ETL Flow:", 
            ["Classic ETL Process", "Modern ELT Process", "Lambda Architecture", "Kappa Architecture", "DataOps Pipeline"])
        
        if flow_type == "Classic ETL Process":
            fig_etl = go.Figure()
            
            nodes = {
                'Source\nSystems': (1, 8),
                'Data\nExtraction': (3, 8),
                'Staging\nArea': (5, 8),
                'Data\nValidation': (7, 8),
                'Data\nTransformation': (9, 8),
                'Data\nCleansing': (9, 6),
                'Business\nRules': (11, 8),
                'Target\nDatabase': (13, 8),
                'Data\nQuality': (7, 6),
                'Error\nHandling': (5, 6),
                'Monitoring': (11, 6)
            }
            
            for node, (x, y) in nodes.items():
                if 'Source' in node or 'Target' in node:
                    color = 'lightgreen'
                elif 'Transform' in node or 'Rules' in node:
                    color = 'orange'
                elif 'Error' in node or 'Quality' in node or 'Monitoring' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_etl.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                 fillcolor=color, line=dict(color="black", width=2))
                fig_etl.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Source\nSystems', 'Data\nExtraction'), ('Data\nExtraction', 'Staging\nArea'),
                ('Staging\nArea', 'Data\nValidation'), ('Data\nValidation', 'Data\nTransformation'),
                ('Data\nTransformation', 'Business\nRules'), ('Business\nRules', 'Target\nDatabase'),
                ('Data\nTransformation', 'Data\nCleansing'), ('Data\nValidation', 'Data\nQuality'),
                ('Staging\nArea', 'Error\nHandling'), ('Business\nRules', 'Monitoring')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_etl.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_etl.update_layout(
                title="Classic ETL Process Flow",
                xaxis=dict(range=[0, 14], showgrid=False, showticklabels=False),
                yaxis=dict(range=[5, 9], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_etl, use_container_width=True)
            
            st.markdown("""
            **Classic ETL Components:**
            - **Extract**: Pull data from source systems
            - **Staging**: Temporary storage for raw data
            - **Validation**: Check data integrity and format
            - **Transform**: Apply business logic and data conversion
            - **Load**: Insert processed data into target system
            - **Quality & Monitoring**: Ensure data quality and track performance
            """)
            
        elif flow_type == "Modern ELT Process":
            fig_elt = go.Figure()
            
            nodes = {
                'Data\nSources': (1, 8),
                'Raw Data\nIngestion': (3, 8),
                'Data\nLake': (5, 8),
                'Schema\nRegistry': (5, 6),
                'Transform\nEngine': (7, 8),
                'Data\nCatalog': (7, 6),
                'Curated\nData': (9, 8),
                'Analytics\nWorkbench': (11, 8),
                'BI Tools': (13, 9),
                'ML Platform': (13, 7),
                'Governance': (9, 6)
            }
            
            for node, (x, y) in nodes.items():
                if 'Lake' in node or 'Curated' in node:
                    color = 'lightgreen'
                elif 'Transform' in node or 'Analytics' in node:
                    color = 'orange'
                elif 'Registry' in node or 'Catalog' in node or 'Governance' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_elt.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                 fillcolor=color, line=dict(color="black", width=2))
                fig_elt.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Data\nSources', 'Raw Data\nIngestion'), ('Raw Data\nIngestion', 'Data\nLake'),
                ('Data\nLake', 'Schema\nRegistry'), ('Data\nLake', 'Transform\nEngine'),
                ('Transform\nEngine', 'Data\nCatalog'), ('Transform\nEngine', 'Curated\nData'),
                ('Curated\nData', 'Analytics\nWorkbench'), ('Analytics\nWorkbench', 'BI Tools'),
                ('Analytics\nWorkbench', 'ML Platform'), ('Curated\nData', 'Governance')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_elt.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_elt.update_layout(
                title="Modern ELT Process Flow",
                xaxis=dict(range=[0, 14], showgrid=False, showticklabels=False),
                yaxis=dict(range=[5, 10], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_elt, use_container_width=True)
            
            st.markdown("""
            **Modern ELT Advantages:**
            - **Raw Data First**: Store data in original format
            - **Schema on Read**: Define schema when accessing data
            - **Scalable Storage**: Use cloud data lakes for massive scale
            - **Flexible Transformations**: Apply transforms as needed
            - **Multiple Consumers**: BI, ML, and analytics from same source
            - **Data Governance**: Built-in cataloging and lineage tracking
            """)
            
        elif flow_type == "Lambda Architecture":
            fig_lambda = go.Figure()
            
            nodes = {
                'Data\nSources': (1, 8),
                'Message\nQueue': (3, 8),
                'Batch\nLayer': (5, 9),
                'Speed\nLayer': (5, 7),
                'Master\nDataset': (7, 9),
                'Real-time\nViews': (7, 7),
                'Serving\nLayer': (9, 8),
                'Batch\nViews': (9, 10),
                'Combined\nViews': (11, 8),
                'Applications': (13, 8)
            }
            
            for node, (x, y) in nodes.items():
                if 'Batch' in node:
                    color = 'lightgreen'
                elif 'Speed' in node or 'Real-time' in node:
                    color = 'orange'
                elif 'Serving' in node or 'Combined' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_lambda.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                    fillcolor=color, line=dict(color="black", width=2))
                fig_lambda.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Data\nSources', 'Message\nQueue'), ('Message\nQueue', 'Batch\nLayer'),
                ('Message\nQueue', 'Speed\nLayer'), ('Batch\nLayer', 'Master\nDataset'),
                ('Speed\nLayer', 'Real-time\nViews'), ('Master\nDataset', 'Batch\nViews'),
                ('Batch\nViews', 'Serving\nLayer'), ('Real-time\nViews', 'Serving\nLayer'),
                ('Serving\nLayer', 'Combined\nViews'), ('Combined\nViews', 'Applications')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_lambda.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_lambda.update_layout(
                title="Lambda Architecture Flow",
                xaxis=dict(range=[0, 14], showgrid=False, showticklabels=False),
                yaxis=dict(range=[6, 11], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_lambda, use_container_width=True)
            
            st.markdown("""
            **Lambda Architecture Benefits:**
            - **Dual Processing**: Batch for accuracy, stream for speed
            - **Fault Tolerance**: Batch layer provides backup for speed layer
            - **Comprehensive Views**: Combines batch and real-time perspectives
            - **Scalability**: Each layer can scale independently
            - **Complexity Trade-off**: More complex but handles all data scenarios
            """)
            
        elif flow_type == "Kappa Architecture":
            fig_kappa = go.Figure()
            
            nodes = {
                'Data\nSources': (1, 8),
                'Event\nStreaming': (3, 8),
                'Stream\nProcessing': (5, 8),
                'Reprocessing\nCapability': (5, 6),
                'Speed\nLayer': (7, 8),
                'Storage\nLayer': (9, 8),
                'Serving\nLayer': (11, 8),
                'Applications': (13, 8),
                'Checkpointing': (7, 6),
                'State\nManagement': (9, 6)
            }
            
            for node, (x, y) in nodes.items():
                if 'Stream' in node or 'Speed' in node:
                    color = 'orange'
                elif 'Storage' in node or 'Serving' in node:
                    color = 'lightgreen'
                elif 'Reprocessing' in node or 'Checkpointing' in node or 'State' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_kappa.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                   fillcolor=color, line=dict(color="black", width=2))
                fig_kappa.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Data\nSources', 'Event\nStreaming'), ('Event\nStreaming', 'Stream\nProcessing'),
                ('Stream\nProcessing', 'Reprocessing\nCapability'), ('Stream\nProcessing', 'Speed\nLayer'),
                ('Speed\nLayer', 'Storage\nLayer'), ('Speed\nLayer', 'Checkpointing'),
                ('Storage\nLayer', 'Serving\nLayer'), ('Storage\nLayer', 'State\nManagement'),
                ('Serving\nLayer', 'Applications'), ('Reprocessing\nCapability', 'Speed\nLayer')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_kappa.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_kappa.update_layout(
                title="Kappa Architecture Flow",
                xaxis=dict(range=[0, 14], showgrid=False, showticklabels=False),
                yaxis=dict(range=[5, 9], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_kappa, use_container_width=True)
            
            st.markdown("""
            **Kappa Architecture Principles:**
            - **Stream-Only**: Everything is processed as streams
            - **Reprocessing**: Historical data can be reprocessed from event logs
            - **Simplified**: Single processing paradigm reduces complexity
            - **Real-time Focus**: Optimized for low-latency processing
            - **State Management**: Maintains processing state for fault tolerance
            """)
            
        elif flow_type == "DataOps Pipeline":
            fig_dataops = go.Figure()
            
            nodes = {
                'Source\nCode': (1, 9),
                'Data\nSources': (1, 7),
                'Version\nControl': (3, 9),
                'CI/CD\nPipeline': (5, 9),
                'Testing\nFramework': (5, 7),
                'Data\nValidation': (7, 7),
                'Staging\nEnvironment': (7, 9),
                'Production\nDeployment': (9, 9),
                'Monitoring': (11, 9),
                'Alerting': (11, 7),
                'Rollback\nCapability': (9, 7),
                'Documentation': (3, 7)
            }
            
            for node, (x, y) in nodes.items():
                if 'Production' in node or 'Staging' in node:
                    color = 'lightgreen'
                elif 'Testing' in node or 'Validation' in node:
                    color = 'orange'
                elif 'Monitoring' in node or 'Alerting' in node or 'Rollback' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_dataops.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                     fillcolor=color, line=dict(color="black", width=2))
                fig_dataops.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Source\nCode', 'Version\nControl'), ('Version\nControl', 'CI/CD\nPipeline'),
                ('CI/CD\nPipeline', 'Testing\nFramework'), ('Testing\nFramework', 'Data\nValidation'),
                ('CI/CD\nPipeline', 'Staging\nEnvironment'), ('Data\nValidation', 'Staging\nEnvironment'),
                ('Staging\nEnvironment', 'Production\nDeployment'), ('Production\nDeployment', 'Monitoring'),
                ('Monitoring', 'Alerting'), ('Alerting', 'Rollback\nCapability'),
                ('Version\nControl', 'Documentation'), ('Data\nSources', 'Testing\nFramework')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_dataops.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_dataops.update_layout(
                title="DataOps Pipeline Flow",
                xaxis=dict(range=[0, 12], showgrid=False, showticklabels=False),
                yaxis=dict(range=[6, 10], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_dataops, use_container_width=True)
            
            st.markdown("""
            **DataOps Pipeline Features:**
            - **Version Control**: Track changes to data pipelines and schemas
            - **CI/CD Integration**: Automated testing and deployment
            - **Data Testing**: Validate data quality and business rules
            - **Environment Management**: Staging and production environments
            - **Monitoring**: Real-time pipeline health and performance tracking
            - **Automated Rollback**: Quick recovery from failed deployments
            """)
    
    with tab5:
        st.subheader("🏢 Real-World Pipeline Examples")
        
        # Pipeline orchestration tools
        st.markdown("### 🎼 Orchestration Tools")
        
        orchestration_tools = {
            "Apache Airflow": {
                "icon": "🌪️",
                "description": "Python-based workflow orchestration",
                "use_case": "Complex dependencies, Python-heavy pipelines",
                "companies": ["Airbnb", "Netflix", "Adobe"]
            },
            "AWS Glue": {
                "icon": "🔗", 
                "description": "Serverless ETL service",
                "use_case": "Cloud-native ETL, automatic scaling",
                "companies": ["Amazon", "Capital One", "Johnson & Johnson"]
            },
            "dbt": {
                "icon": "🛠️",
                "description": "Transform data in warehouse using SQL",
                "use_case": "Analytics engineering, ELT transformations", 
                "companies": ["GitLab", "Shopify", "Fishtown Analytics"]
            }
        }
        
        for tool, details in orchestration_tools.items():
            with st.expander(f"{details['icon']} {tool}"):
                st.markdown(f"**Description:** {details['description']}")
                st.markdown(f"**Best for:** {details['use_case']}")
                st.markdown(f"**Used by:** {', '.join(details['companies'])}")
        
        # Company-specific pipelines
        st.markdown("### 🏢 Company Pipeline Examples")
        
        pipeline_examples = {
            "Airbnb": {
                "icon": "🏠",
                "pipeline": "User data → Airflow → Spark → Hive → Presto",
                "frequency": "Hourly batch jobs",
                "challenge": "Handling seasonal booking patterns",
                "solution": "Dynamic scaling with Airflow + Kubernetes"
            },
            "Amazon": {
                "icon": "🛒", 
                "pipeline": "Orders → Kinesis → Lambda → Glue → Redshift",
                "frequency": "Real-time + daily aggregations",
                "challenge": "Processing millions of orders",
                "solution": "Serverless architecture with auto-scaling"
            },
            "Netflix": {
                "icon": "🎬",
                "pipeline": "Viewing events → Kafka → Spark → S3 → ML models",
                "frequency": "Real-time streaming",
                "challenge": "Personalized recommendations at scale", 
                "solution": "Stream processing + batch ML training"
            }
        }
        
        for company, pipeline in pipeline_examples.items():
            with st.expander(f"{pipeline['icon']} {company} Pipeline"):
                st.markdown(f"**Pipeline:** {pipeline['pipeline']}")
                st.markdown(f"**Frequency:** {pipeline['frequency']}")
                st.markdown(f"**Challenge:** {pipeline['challenge']}")
                st.markdown(f"**Solution:** {pipeline['solution']}")

def show_processing_systems():
    st.header("⚡ Processing Systems")
    st.markdown("Learn about batch and stream processing frameworks")
    
    tab1, tab2, tab3 = st.tabs(["📚 Batch vs Stream", "🛠️ Framework Comparison", "🏢 Real Examples"])
    
    with tab1:
        st.subheader("Batch vs Stream Processing")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            ### 📦 Batch Processing
            **Process data in large chunks**
            
            **Characteristics:**
            - Process data at scheduled intervals
            - High throughput, higher latency
            - Cost-effective for large datasets
            - Fault-tolerant and reliable
            
            **Use Cases:**
            - Daily reports and analytics
            - Historical data processing
            - Machine learning training
            - Data warehouse ETL
            
            **Tools:** Hadoop MapReduce, Apache Spark, AWS EMR
            """)
            
            # Batch processing simulation
            st.markdown("#### 📊 Batch Processing Simulation")
            batch_interval = st.selectbox("Batch Interval:", ["Every hour", "Every day", "Every week"])
            data_size = st.slider("Data Size (GB):", 1, 1000, 100)
            
            if st.button("Simulate Batch Job"):
                processing_time = data_size * 0.1  # Simulate processing time
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                import time
                for i in range(101):
                    progress_bar.progress(i)
                    status_text.text(f'Processing batch job... {i}% complete')
                    time.sleep(0.02)
                
                st.success(f"✅ Batch job completed! Processed {data_size}GB in {processing_time:.1f} minutes")
                
                # Show batch metrics
                metrics_data = pd.DataFrame({
                    'Time': pd.date_range('00:00', periods=24, freq='1H'),
                    'Data_Processed_GB': np.random.randint(50, 200, 24)
                })
                fig = px.bar(metrics_data, x='Time', y='Data_Processed_GB', 
                           title='Hourly Batch Processing Volume')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("""
            ### ⚡ Stream Processing
            **Process data in real-time**
            
            **Characteristics:**
            - Process data as it arrives
            - Low latency, continuous processing
            - Real-time insights and actions
            - More complex error handling
            
            **Use Cases:**
            - Real-time alerts and monitoring
            - Live dashboards
            - Fraud detection
            - Dynamic pricing
            
            **Tools:** Apache Kafka, Apache Flink, Spark Streaming
            """)
            
            # Stream processing simulation
            st.markdown("#### 📊 Stream Processing Simulation")
            stream_rate = st.selectbox("Stream Rate:", ["100 events/sec", "1K events/sec", "10K events/sec"])
            
            if st.button("Start Stream Processing"):
                st.markdown("**Live Event Stream:**")
                placeholder = st.empty()
                
                for i in range(15):
                    event = {
                        'timestamp': datetime.now().strftime('%H:%M:%S.%f')[:-3],
                        'event_id': f"evt_{np.random.randint(10000, 99999)}",
                        'user_id': np.random.randint(1000, 9999),
                        'action': np.random.choice(['purchase', 'view', 'click', 'scroll']),
                        'value': round(np.random.uniform(1, 1000), 2)
                    }
                    
                    with placeholder.container():
                        st.json(event)
                    time.sleep(0.3)
                
                st.info("Stream processing completed - In production, this runs continuously!")
        
        # Comparison table
        st.markdown("---")
        st.subheader("⚖️ Batch vs Stream Comparison")
        
        comparison_data = pd.DataFrame({
            'Aspect': ['Latency', 'Throughput', 'Cost', 'Complexity', 'Use Cases'],
            'Batch Processing': ['Minutes to Hours', 'Very High', 'Lower', 'Simpler', 'Reports, ETL, ML Training'],
            'Stream Processing': ['Milliseconds', 'High', 'Higher', 'More Complex', 'Alerts, Monitoring, Real-time Analytics']
        })
        st.table(comparison_data)
    
    with tab2:
        st.subheader("🛠️ Processing Framework Comparison")
        
        framework_type = st.selectbox("Choose framework type:", ["Batch Frameworks", "Stream Frameworks", "Hybrid Frameworks"])
        
        if framework_type == "Batch Frameworks":
            frameworks = {
                "Apache Spark": {
                    "icon": "⚡",
                    "description": "Unified analytics engine for large-scale data processing",
                    "pros": ["Fast in-memory processing", "Supports multiple languages", "Rich ecosystem"],
                    "cons": ["Memory intensive", "Steep learning curve"],
                    "best_for": "Large-scale ETL, machine learning, interactive analytics"
                },
                "Hadoop MapReduce": {
                    "icon": "🐘", 
                    "description": "Original big data processing framework",
                    "pros": ["Highly fault-tolerant", "Handles very large datasets", "Battle-tested"],
                    "cons": ["Slow (disk-based)", "Complex programming model"],
                    "best_for": "Massive batch jobs, cost-sensitive processing"
                },
                "AWS EMR": {
                    "icon": "☁️",
                    "description": "Managed cluster platform for big data frameworks",
                    "pros": ["Fully managed", "Auto-scaling", "Multiple framework support"],
                    "cons": ["AWS lock-in", "Cost can be high"],
                    "best_for": "Cloud-native batch processing, temporary clusters"
                }
            }
            
        elif framework_type == "Stream Frameworks":
            frameworks = {
                "Apache Kafka": {
                    "icon": "🌊",
                    "description": "Distributed streaming platform and message broker",
                    "pros": ["High throughput", "Durable", "Real-time processing"],
                    "cons": ["Complex setup", "Operational overhead"],
                    "best_for": "Event streaming, real-time data pipelines"
                },
                "Apache Flink": {
                    "icon": "🏃",
                    "description": "Stream processing framework with low latency",
                    "pros": ["True streaming", "Low latency", "Event-time processing"],
                    "cons": ["Smaller community", "Memory management complexity"],
                    "best_for": "Ultra-low latency, complex event processing"
                },
                "Spark Streaming": {
                    "icon": "⚡",
                    "description": "Micro-batch stream processing on Spark",
                    "pros": ["Unified batch/stream API", "Rich ecosystem", "Fault-tolerant"],
                    "cons": ["Higher latency than true streaming", "Memory requirements"],
                    "best_for": "Near real-time processing, mixed workloads"
                }
            }
        
        else:  # Hybrid Frameworks
            frameworks = {
                "Apache Beam": {
                    "icon": "🌉",
                    "description": "Unified programming model for batch and stream",
                    "pros": ["Single API", "Multiple runners", "Portable"],
                    "cons": ["Abstraction overhead", "Runner-specific optimizations"],
                    "best_for": "Unified batch/stream pipelines, multi-cloud"
                },
                "Databricks": {
                    "icon": "🧱",
                    "description": "Unified analytics platform built on Spark",
                    "pros": ["Collaborative notebooks", "Auto-scaling", "Delta Lake integration"],
                    "cons": ["Vendor lock-in", "Cost", "Complex pricing"],
                    "best_for": "Data science workflows, collaborative analytics"
                }
            }
        
        for framework, details in frameworks.items():
            with st.expander(f"{details['icon']} {framework}"):
                st.markdown(f"**Description:** {details['description']}")
                st.markdown(f"**Best for:** {details['best_for']}")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**✅ Pros:**")
                    for pro in details['pros']:
                        st.markdown(f"• {pro}")
                with col2:
                    st.markdown("**❌ Cons:**")
                    for con in details['cons']:
                        st.markdown(f"• {con}")
        
        # Framework selector tool
        st.markdown("---")
        st.subheader("🎯 Which Framework Should You Choose?")
        
        col1, col2 = st.columns(2)
        with col1:
            latency_req = st.selectbox("Latency Requirement:", 
                ["Sub-second", "Few seconds", "Minutes", "Hours"])
            data_volume = st.selectbox("Data Volume:", 
                ["Small (GB)", "Medium (TB)", "Large (PB)"])
        with col2:
            use_case = st.selectbox("Primary Use Case:", 
                ["Real-time alerts", "Analytics", "ETL", "ML Training"])
            cloud_pref = st.selectbox("Cloud Preference:", 
                ["AWS", "Azure", "GCP", "Multi-cloud", "On-premise"])
        
        if st.button("Get Framework Recommendation"):
            if latency_req == "Sub-second":
                st.success("🏃 **Recommendation: Apache Flink** - Best for ultra-low latency streaming")
            elif use_case == "Real-time alerts":
                st.success("🌊 **Recommendation: Apache Kafka + Kafka Streams** - Perfect for real-time event processing")
            elif data_volume == "Large (PB)" and latency_req == "Hours":
                st.success("🐘 **Recommendation: Hadoop MapReduce** - Cost-effective for massive batch jobs")
            elif use_case == "ML Training":
                st.success("⚡ **Recommendation: Apache Spark** - Excellent for large-scale ML workloads")
            else:
                st.success("⚡ **Recommendation: Apache Spark** - Versatile choice for most use cases")
    
    with tab3:
        st.subheader("🏢 Real-World Processing Examples")
        
        processing_examples = {
            "Uber": {
                "icon": "🚗",
                "batch": {
                    "use_case": "Daily driver analytics, trip summaries",
                    "framework": "Spark on Hadoop",
                    "frequency": "Daily batch jobs",
                    "data_size": "Terabytes per day"
                },
                "stream": {
                    "use_case": "Real-time surge pricing, ETA calculation",
                    "framework": "Apache Flink",
                    "latency": "Sub-second",
                    "throughput": "Millions of events/second"
                }
            },
            "Netflix": {
                "icon": "🎬",
                "batch": {
                    "use_case": "Recommendation model training",
                    "framework": "Spark + TensorFlow",
                    "frequency": "Multiple times per day",
                    "data_size": "Petabytes of viewing data"
                },
                "stream": {
                    "use_case": "Real-time content personalization",
                    "framework": "Kafka + Flink",
                    "latency": "Milliseconds", 
                    "throughput": "Billions of events/day"
                }
            },
            "Amazon": {
                "icon": "🛒",
                "batch": {
                    "use_case": "Daily sales reports, inventory optimization",
                    "framework": "EMR with Spark",
                    "frequency": "Hourly and daily",
                    "data_size": "Multi-petabyte data lake"
                },
                "stream": {
                    "use_case": "Real-time recommendation updates",
                    "framework": "Kinesis + Lambda",
                    "latency": "Few seconds",
                    "throughput": "Millions of events/second"
                }
            }
        }
        
        for company, examples in processing_examples.items():
            with st.expander(f"{examples['icon']} {company} Processing Architecture"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 📦 Batch Processing")
                    st.markdown(f"**Use Case:** {examples['batch']['use_case']}")
                    st.markdown(f"**Framework:** {examples['batch']['framework']}")
                    st.markdown(f"**Frequency:** {examples['batch']['frequency']}")
                    st.markdown(f"**Data Size:** {examples['batch']['data_size']}")
                    
                with col2:
                    st.markdown("### ⚡ Stream Processing")
                    st.markdown(f"**Use Case:** {examples['stream']['use_case']}")
                    st.markdown(f"**Framework:** {examples['stream']['framework']}")
                    st.markdown(f"**Latency:** {examples['stream']['latency']}")
                    st.markdown(f"**Throughput:** {examples['stream']['throughput']}")
        
        # Processing architecture patterns
        st.markdown("---")
        st.markdown("### 🏗️ Common Architecture Patterns")
        
        patterns = {
            "Lambda Architecture": {
                "description": "Batch + Stream layers with serving layer",
                "components": ["Batch Layer (Hadoop/Spark)", "Speed Layer (Storm/Flink)", "Serving Layer (HBase/Cassandra)"],
                "pros": "Fault-tolerant, handles both batch and real-time",
                "cons": "Complex, duplicate logic in batch/stream layers"
            },
            "Kappa Architecture": {
                "description": "Stream-only processing with replayable logs",
                "components": ["Stream Processing (Kafka/Flink)", "Replayable Log (Kafka)", "Serving Layer"],
                "pros": "Simpler, single codebase, easier to maintain",
                "cons": "All processing must be streamable"
            }
        }
        
        for pattern, details in patterns.items():
            with st.expander(f"🏗️ {pattern}"):
                st.markdown(f"**Description:** {details['description']}")
                st.markdown("**Components:**")
                for component in details['components']:
                    st.markdown(f"• {component}")
                st.markdown(f"**✅ Pros:** {details['pros']}")
                st.markdown(f"**❌ Cons:** {details['cons']}")

def show_big_data_scaling():
    st.header("📊 Big Data & Scaling")
    st.markdown("Understanding the 3 Vs of Big Data and scaling challenges")
    
    tab1, tab2, tab3 = st.tabs(["📚 3 Vs of Big Data", "🛠️ Scaling Strategies", "🏢 Real Examples"])
    
    with tab1:
        st.subheader("The 3 Vs of Big Data")
        
        vs_selection = st.selectbox("Choose a V to explore:", ["Volume", "Velocity", "Variety"])
        
        if vs_selection == "Volume":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 📏 Volume - Scale of Data
                **The sheer amount of data generated and stored**
                
                **Characteristics:**
                - Terabytes to Petabytes of data
                - Exponential growth rates
                - Storage and processing challenges
                - Cost optimization needs
                
                **Examples:**
                - Facebook: 300+ TB of new data daily
                - YouTube: 500+ hours of video uploaded per minute
                - Google: Processes 20+ PB of data daily
                - Walmart: 2.5 PB of data from customer transactions hourly
                """)
            
            with col2:
                # Volume visualization
                st.markdown("#### 📊 Data Volume Growth Simulation")
                
                company_type = st.selectbox("Company Type:", ["E-commerce", "Social Media", "Streaming", "Financial"])
                years = st.slider("Years to simulate:", 1, 10, 5)
                
                if company_type == "E-commerce":
                    base_volume = 1  # TB
                    growth_rate = 1.5
                elif company_type == "Social Media":
                    base_volume = 5  # TB
                    growth_rate = 2.0
                elif company_type == "Streaming":
                    base_volume = 10  # TB
                    growth_rate = 1.8
                else:  # Financial
                    base_volume = 2  # TB
                    growth_rate = 1.3
                
                # Simulate data growth
                volume_data = []
                for year in range(years + 1):
                    volume = base_volume * (growth_rate ** year)
                    volume_data.append({
                        'Year': f'Year {year}',
                        'Volume_TB': volume,
                        'Storage_Cost_USD': volume * 50  # $50 per TB
                    })
                
                volume_df = pd.DataFrame(volume_data)
                
                fig = px.line(volume_df, x='Year', y='Volume_TB', 
                             title=f'{company_type} Data Volume Growth')
                fig.update_layout(yaxis_title='Volume (TB)')
                st.plotly_chart(fig, use_container_width=True)
                
                st.metric(
                    label=f"Final Volume ({years} years)",
                    value=f"{volume_df.iloc[-1]['Volume_TB']:.1f} TB",
                    delta=f"${volume_df.iloc[-1]['Storage_Cost_USD']:,.0f} storage cost"
                )
        
        elif vs_selection == "Velocity":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### ⚡ Velocity - Speed of Data
                **The rate at which data is generated and processed**
                
                **Characteristics:**
                - Real-time or near real-time processing
                - High-frequency data streams
                - Time-sensitive decision making
                - Streaming architectures
                
                **Examples:**
                - Twitter: 6,000+ tweets per second
                - NYSE: Processes millions of trades per second
                - Netflix: Billions of events per day
                - Uber: GPS updates every few seconds from millions of devices
                """)
            
            with col2:
                st.markdown("#### ⚡ Velocity Simulation")
                
                scenario = st.selectbox("Choose scenario:", 
                    ["Social Media Posts", "Stock Trades", "IoT Sensors", "Web Clicks"])
                
                if scenario == "Social Media Posts":
                    rate_per_sec = np.random.randint(5000, 8000)
                    unit = "posts/second"
                elif scenario == "Stock Trades":
                    rate_per_sec = np.random.randint(50000, 100000)
                    unit = "trades/second"
                elif scenario == "IoT Sensors":
                    rate_per_sec = np.random.randint(10000, 50000)
                    unit = "sensor readings/second"
                else:  # Web Clicks
                    rate_per_sec = np.random.randint(1000, 5000)
                    unit = "clicks/second"
                
                if st.button("Start Velocity Simulation"):
                    velocity_placeholder = st.empty()
                    
                    for i in range(10):
                        current_rate = rate_per_sec + np.random.randint(-1000, 1000)
                        
                        with velocity_placeholder.container():
                            st.metric(
                                label=f"Current {scenario} Rate",
                                value=f"{current_rate:,} {unit}",
                                delta=f"{current_rate * 60:,} per minute"
                            )
                            
                            # Show processing challenge
                            if current_rate > rate_per_sec * 1.2:
                                st.error("🚨 High velocity detected! Scaling required!")
                            elif current_rate < rate_per_sec * 0.8:
                                st.success("✅ Normal processing capacity")
                            else:
                                st.warning("⚠️ Approaching capacity limits")
                        
                        time.sleep(0.5)
        
        elif vs_selection == "Variety":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 🎭 Variety - Types of Data
                **Different formats and sources of data**
                
                **Characteristics:**
                - Structured, semi-structured, unstructured
                - Multiple data sources and formats
                - Schema evolution challenges
                - Integration complexity
                
                **Data Types:**
                - **Structured:** SQL databases, CSV files
                - **Semi-structured:** JSON, XML, logs
                - **Unstructured:** Images, videos, text, audio
                """)
            
            with col2:
                st.markdown("#### 🎭 Data Variety Example")
                
                data_sources = {
                    "Customer Database": {"type": "Structured", "format": "SQL", "size": "10 GB"},
                    "Web Logs": {"type": "Semi-structured", "format": "JSON", "size": "100 GB"},
                    "Product Images": {"type": "Unstructured", "format": "PNG/JPG", "size": "500 GB"},
                    "Customer Reviews": {"type": "Unstructured", "format": "Text", "size": "50 GB"},
                    "API Responses": {"type": "Semi-structured", "format": "JSON/XML", "size": "25 GB"},
                    "Email Archives": {"type": "Unstructured", "format": "Text/HTML", "size": "200 GB"}
                }
                
                st.markdown("**E-commerce Data Sources:**")
                
                variety_df = pd.DataFrame([
                    {"Source": source, **details}
                    for source, details in data_sources.items()
                ])
                st.dataframe(variety_df, use_container_width=True)
                
                # Variety challenges
                st.markdown("**Integration Challenges:**")
                challenges = [
                    "🔄 Different update frequencies",
                    "🗂️ Schema inconsistencies",
                    "🔧 Multiple processing tools needed",
                    "📊 Complex joins across formats",
                    "🛡️ Different security requirements"
                ]
                
                for challenge in challenges:
                    st.markdown(f"• {challenge}")
    
    with tab2:
        st.subheader("🛠️ Scaling Strategies")
        
        scaling_type = st.selectbox("Choose scaling approach:", 
            ["Horizontal vs Vertical", "Partitioning", "Caching", "Load Balancing"])
        
        if scaling_type == "Horizontal vs Vertical":
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                ### 📈 Vertical Scaling (Scale Up)
                **Add more power to existing machines**
                
                **Approach:**
                - Increase CPU, RAM, storage on single machine
                - Upgrade to more powerful hardware
                - Single point of processing
                
                **Pros:**
                - Simpler architecture
                - No data distribution complexity
                - Better for single-threaded applications
                
                **Cons:**
                - Hardware limits
                - Single point of failure
                - Expensive high-end hardware
                
                **Best for:** Traditional databases, applications with limited parallelization
                """)
            
            with col2:
                st.markdown("""
                ### 📊 Horizontal Scaling (Scale Out)
                **Add more machines to the system**
                
                **Approach:**
                - Distribute load across multiple machines
                - Add commodity hardware as needed
                - Parallel processing
                
                **Pros:**
                - Nearly unlimited scaling
                - Fault tolerance through redundancy
                - Cost-effective commodity hardware
                
                **Cons:**
                - Complex architecture
                - Data consistency challenges
                - Network overhead
                
                **Best for:** Big data processing, web applications, distributed systems
                """)
            
            # Interactive scaling simulator
            st.markdown("---")
            st.markdown("#### 🎮 Scaling Simulator")
            
            current_load = st.slider("Current System Load (%):", 0, 200, 80)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Scale Vertically"):
                    new_capacity = min(current_load * 0.5, 50)  # Vertical scaling limited
                    st.success(f"✅ Vertical scaling: Load reduced to {new_capacity:.0f}%")
                    if new_capacity > 30:
                        st.warning("⚠️ Still approaching limits - consider horizontal scaling")
            
            with col2:
                if st.button("Scale Horizontally"):
                    new_capacity = current_load * 0.3  # Horizontal scaling more effective
                    st.success(f"✅ Horizontal scaling: Load distributed to {new_capacity:.0f}%")
                    st.info("💡 Added 2 new nodes to cluster")
        
        elif scaling_type == "Partitioning":
            st.markdown("""
            ### 🔀 Data Partitioning Strategies
            **Divide data across multiple storage units**
            """)
            
            partition_strategy = st.selectbox("Partitioning Strategy:", 
                ["Range Partitioning", "Hash Partitioning", "List Partitioning"])
            
            if partition_strategy == "Range Partitioning":
                st.markdown("""
                **Range Partitioning:** Data divided based on value ranges
                
                **Example - E-commerce Orders by Date:**
                - Partition 1: Jan 2024 orders
                - Partition 2: Feb 2024 orders  
                - Partition 3: Mar 2024 orders
                
                **Pros:** Easy to understand, good for time-series data
                **Cons:** Potential for uneven distribution (hot partitions)
                """)
                
                # Range partitioning simulation
                date_range = pd.date_range('2024-01-01', '2024-12-31', freq='M')
                orders_per_month = np.random.randint(1000, 5000, len(date_range))
                
                partition_df = pd.DataFrame({
                    'Partition': [f"Partition {i+1}" for i in range(len(date_range))],
                    'Date_Range': [date.strftime('%b %Y') for date in date_range],
                    'Orders': orders_per_month
                })
                
                fig = px.bar(partition_df, x='Date_Range', y='Orders',
                           title='Range Partitioning - Orders by Month')
                st.plotly_chart(fig, use_container_width=True)
            
            elif partition_strategy == "Hash Partitioning":
                st.markdown("""
                **Hash Partitioning:** Data divided using hash function
                
                **Example - User Data by User ID Hash:**
                - Partition 1: hash(user_id) % 4 == 0
                - Partition 2: hash(user_id) % 4 == 1
                - Partition 3: hash(user_id) % 4 == 2
                - Partition 4: hash(user_id) % 4 == 3
                
                **Pros:** Even distribution, good for random access
                **Cons:** Range queries require scanning all partitions
                """)
                
                # Hash partitioning simulation
                partitions = 4
                users_per_partition = np.random.randint(8000, 12000, partitions)
                
                hash_df = pd.DataFrame({
                    'Partition': [f"Partition {i+1}" for i in range(partitions)],
                    'Users': users_per_partition,
                    'Hash_Range': [f"{i}-{i}" for i in range(partitions)]
                })
                
                fig = px.bar(hash_df, x='Partition', y='Users',
                           title='Hash Partitioning - Even Distribution')
                st.plotly_chart(fig, use_container_width=True)
        
        elif scaling_type == "Caching":
            st.markdown("""
            ### 🚀 Caching Strategies
            **Store frequently accessed data in fast storage**
            """)
            
            cache_level = st.selectbox("Cache Level:", 
                ["Application Cache", "Database Cache", "CDN Cache", "Distributed Cache"])
            
            # Cache hit ratio simulation
            cache_size = st.slider("Cache Size (MB):", 100, 10000, 1000)
            hit_ratio = min(95, 40 + (cache_size / 200))  # Simulate hit ratio based on cache size
            
            st.metric(
                label="Cache Hit Ratio",
                value=f"{hit_ratio:.1f}%",
                delta=f"Response time: {1000/hit_ratio:.0f}ms avg"
            )
            
            if hit_ratio > 80:
                st.success("✅ Excellent cache performance!")
            elif hit_ratio > 60:
                st.warning("⚠️ Good cache performance, consider optimization")
            else:
                st.error("❌ Poor cache performance, increase cache size")
    
    with tab3:
        st.subheader("🏢 Real-World Big Data Examples")
        
        # Company big data examples
        big_data_examples = {
            "Netflix": {
                "icon": "🎬",
                "volume": "15+ PB of data stored",
                "velocity": "500+ GB of new data per day",
                "variety": "Video files, viewing logs, user interactions, A/B test data",
                "challenges": ["Content delivery at global scale", "Real-time recommendations", "Video encoding efficiency"],
                "solutions": ["Global CDN network", "Microservices architecture", "Apache Kafka for streaming"]
            },
            "NYSE": {
                "icon": "💰", 
                "volume": "5+ TB of trade data daily",
                "velocity": "Millions of transactions per second during peak",
                "variety": "Trade data, market feeds, news, social sentiment",
                "challenges": ["Ultra-low latency requirements", "Regulatory compliance", "Market data distribution"],
                "solutions": ["In-memory computing", "Co-located servers", "Custom hardware acceleration"]
            },
            "Amazon": {
                "icon": "🛒",
                "volume": "Multi-exabyte data lake",
                "velocity": "Millions of events per second",
                "variety": "Product catalogs, customer data, logistics, IoT sensors",
                "challenges": ["Global inventory management", "Personalization at scale", "Supply chain optimization"],
                "solutions": ["Distributed computing", "Machine learning pipelines", "Real-time analytics"]
            },
            "Uber": {
                "icon": "🚗",
                "volume": "100+ PB of trip and location data",
                "velocity": "Millions of GPS updates per second",
                "variety": "Location data, trip data, driver data, payment data, maps",
                "challenges": ["Real-time matching", "Dynamic pricing", "Route optimization"],
                "solutions": ["Stream processing", "Geospatial databases", "Predictive analytics"]
            }
        }
        
        for company, data in big_data_examples.items():
            with st.expander(f"{data['icon']} {company} Big Data Challenge"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 📊 The 3 Vs")
                    st.markdown(f"**📏 Volume:** {data['volume']}")
                    st.markdown(f"**⚡ Velocity:** {data['velocity']}")
                    st.markdown(f"**🎭 Variety:** {data['variety']}")
                
                with col2:
                    st.markdown("### 🎯 Challenges & Solutions")
                    st.markdown("**Challenges:**")
                    for challenge in data['challenges']:
                        st.markdown(f"• {challenge}")
                    
                    st.markdown("**Solutions:**")
                    for solution in data['solutions']:
                        st.markdown(f"• {solution}")
        
        # Big data technology stack
        st.markdown("---")
        st.markdown("### 🛠️ Big Data Technology Stack")
        
        tech_stack = {
            "Storage": ["HDFS", "Amazon S3", "Google Cloud Storage", "Apache Cassandra"],
            "Processing": ["Apache Spark", "Apache Flink", "Hadoop MapReduce", "Apache Storm"],
            "Querying": ["Apache Presto", "Apache Drill", "Amazon Athena", "Google BigQuery"],
            "Streaming": ["Apache Kafka", "Amazon Kinesis", "Apache Pulsar", "Azure Event Hubs"],
            "Orchestration": ["Apache Airflow", "Luigi", "AWS Step Functions", "Kubeflow"],
            "Monitoring": ["Apache Ambari", "Cloudera Manager", "Datadog", "New Relic"]
        }
        
        selected_layer = st.selectbox("Choose technology layer:", list(tech_stack.keys()))
        
        st.markdown(f"**{selected_layer} Technologies:**")
        for tech in tech_stack[selected_layer]:
            st.markdown(f"• {tech}")

def show_company_case_study(company):
    st.markdown("---")
    st.subheader(f"📋 Interactive Case Study: {company}")
    
    # Initialize database connection
    conn = create_company_database()
    
    if "Amazon" in company:
        st.markdown("""
        ### 🛒 Amazon's E-commerce Data Architecture
        **Scale:** Millions of products, billions of transactions daily  
        **Real-time Requirements:** Inventory, recommendations, fraud detection
        """)
        
        # Load Amazon data from SQLite
        df = pd.read_sql_query("SELECT * FROM amazon_sales LIMIT 1000", conn)
        
        st.markdown("#### 📊 Sales Analytics Dashboard")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Orders", f"{len(df):,}")
        with col2:
            st.metric("Total Revenue", f"${df['order_value'].sum():,.2f}")
        with col3:
            st.metric("Avg Order Value", f"${df['order_value'].mean():.2f}")
        with col4:
            st.metric("Prime Members", f"{(df['prime_member'].sum()/len(df)*100):.1f}%")
            
        # Interactive Charts
        tab1, tab2, tab3 = st.tabs(["📈 Sales Trends", "🏷️ Categories", "🚚 Shipping Analysis"])
        
        with tab1:
            # Sales over time
            daily_sales = df.groupby(df['order_date'].dt.date)['order_value'].agg(['sum', 'count']).reset_index()
            fig = px.line(daily_sales, x='order_date', y='sum', title='Daily Sales Revenue',
                         labels={'sum': 'Revenue ($)', 'order_date': 'Date'})
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            # Category analysis
            cat_analysis = df.groupby('product_category')['order_value'].agg(['sum', 'mean', 'count']).reset_index()
            fig = px.bar(cat_analysis, x='product_category', y='sum', title='Revenue by Category',
                        labels={'sum': 'Total Revenue ($)', 'product_category': 'Category'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Pie chart for order distribution
            fig_pie = px.pie(cat_analysis, values='count', names='product_category', 
                           title='Order Distribution by Category')
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with tab3:
            # Shipping analysis
            shipping_stats = df.groupby('shipping_speed')['delivery_days'].agg(['mean', 'count']).reset_index()
            fig = px.bar(shipping_stats, x='shipping_speed', y='mean', title='Average Delivery Days by Shipping Type')
            st.plotly_chart(fig, use_container_width=True)
            
        # Raw data preview
        with st.expander("🔍 View Raw Data Sample"):
            st.dataframe(df.head(100))
            
    elif "Netflix" in company:
        st.markdown("""
        ### 🎬 Netflix's Streaming Data Architecture
        **Scale:** 260M+ subscribers worldwide, petabytes of viewing data
        **Real-time Requirements:** Recommendations, content delivery, user experience
        """)
        
        # Load Netflix data from SQLite  
        df = pd.read_sql_query("SELECT * FROM netflix_viewership LIMIT 1000", conn)
        
        st.markdown("#### 🎭 Viewership Analytics Dashboard")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Views", f"{len(df):,}")
        with col2:
            st.metric("Avg Watch Time", f"{df['watch_duration_min'].mean():.0f} min")
        with col3:
            st.metric("Avg Completion", f"{df['completion_rate'].mean():.1%}")
        with col4:
            st.metric("Avg Rating", f"{df['rating'].mean():.1f}/5")
            
        # Interactive Charts
        tab1, tab2, tab3 = st.tabs(["📺 Content Performance", "🌍 Regional Insights", "📱 Device Analytics"])
        
        with tab1:
            # Most watched content
            content_stats = df.groupby('title')['watch_duration_min'].agg(['sum', 'mean', 'count']).reset_index()
            content_stats = content_stats.sort_values('sum', ascending=False).head(10)
            fig = px.bar(content_stats, x='title', y='sum', title='Top 10 Most Watched Shows (Total Minutes)')
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Genre popularity
            genre_stats = df.groupby('genre')['watch_duration_min'].sum().reset_index()
            fig = px.pie(genre_stats, values='watch_duration_min', names='genre', title='Content Consumption by Genre')
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            # Regional analysis
            region_stats = df.groupby('region')['watch_duration_min'].agg(['sum', 'mean']).reset_index()
            fig = px.bar(region_stats, x='region', y='sum', title='Total Watch Time by Region')
            st.plotly_chart(fig, use_container_width=True)
            
        with tab3:
            # Device preferences
            device_stats = df.groupby('device_type')['completion_rate'].mean().reset_index()
            fig = px.bar(device_stats, x='device_type', y='completion_rate', 
                        title='Average Completion Rate by Device Type')
            st.plotly_chart(fig, use_container_width=True)
            
        # Raw data preview
        with st.expander("🔍 View Raw Data Sample"):
            st.dataframe(df.head(100))
            
    elif "Uber" in company:
        st.markdown("""
        ### 🚗 Uber's Mobility Data Architecture
        **Scale:** 5B+ rides annually, real-time matching across 70+ countries
        **Real-time Requirements:** Driver-rider matching, dynamic pricing, ETA prediction
        """)
        
        # Load Uber data from SQLite
        df = pd.read_sql_query("SELECT * FROM uber_rides LIMIT 1000", conn)
        
        st.markdown("#### 🚕 Ride Analytics Dashboard")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Rides", f"{len(df):,}")
        with col2:
            st.metric("Total Revenue", f"${(df['fare_amount'] + df['tip_amount']).sum():,.2f}")
        with col3:
            st.metric("Avg Ride Distance", f"{df['distance_miles'].mean():.1f} mi")
        with col4:
            st.metric("Avg Driver Rating", f"{df['driver_rating'].mean():.1f}/5")
            
        # Interactive Charts
        tab1, tab2, tab3 = st.tabs(["🚗 Ride Patterns", "💰 Revenue Analysis", "⭐ Quality Metrics"])
        
        with tab1:
            # Ride type distribution
            ride_type_stats = df.groupby('ride_type')['fare_amount'].agg(['sum', 'count', 'mean']).reset_index()
            fig = px.bar(ride_type_stats, x='ride_type', y='count', title='Rides by Service Type')
            st.plotly_chart(fig, use_container_width=True)
            
            # City performance
            city_stats = df.groupby('city')['distance_miles'].agg(['mean', 'count']).reset_index()
            fig = px.scatter(city_stats, x='mean', y='count', size='count', text='city',
                           title='Average Distance vs Volume by City')
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            # Surge pricing impact
            surge_revenue = df.groupby('surge_multiplier')['fare_amount'].agg(['mean', 'count']).reset_index()
            fig = px.bar(surge_revenue, x='surge_multiplier', y='mean', title='Average Fare by Surge Multiplier')
            st.plotly_chart(fig, use_container_width=True)
            
        with tab3:
            # Rating distribution
            fig = px.histogram(df, x='rider_rating', title='Rider Rating Distribution')
            st.plotly_chart(fig, use_container_width=True)
            
        # Raw data preview
        with st.expander("🔍 View Raw Data Sample"):
            st.dataframe(df.head(100))
            
    elif "NYSE" in company:
        st.markdown("""
        ### 💰 NYSE Trading Data Architecture
        **Scale:** 2,400+ listed companies, billions in daily volume
        **Real-time Requirements:** Trade execution, price discovery, market surveillance
        """)
        
        # Load NYSE data from SQLite
        df = pd.read_sql_query("SELECT * FROM nyse_trades LIMIT 1000", conn)
        
        st.markdown("#### 📈 Market Analytics Dashboard")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Trades", f"{len(df):,}")
        with col2:
            st.metric("Total Volume", f"{df['volume'].sum():,}")
        with col3:
            st.metric("Avg Trade Price", f"${df['price'].mean():.2f}")
        with col4:
            st.metric("Market Cap", f"${df['market_cap_billion'].mean():.1f}B")
            
        # Interactive Charts
        tab1, tab2, tab3 = st.tabs(["📊 Market Overview", "🏢 Sector Analysis", "📈 Price Movements"])
        
        with tab1:
            # Top symbols by volume
            symbol_stats = df.groupby('symbol')['volume'].agg(['sum', 'mean']).reset_index()
            symbol_stats = symbol_stats.sort_values('sum', ascending=False).head(10)
            fig = px.bar(symbol_stats, x='symbol', y='sum', title='Top 10 Symbols by Total Volume')
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            # Sector performance
            sector_stats = df.groupby('sector')['price'].agg(['mean', 'count']).reset_index()
            fig = px.bar(sector_stats, x='sector', y='mean', title='Average Price by Sector')
            st.plotly_chart(fig, use_container_width=True)
            
        with tab3:
            # Price change distribution
            fig = px.histogram(df, x='day_change_pct', title='Daily Price Change Distribution (%)')
            st.plotly_chart(fig, use_container_width=True)
            
        # Raw data preview  
        with st.expander("🔍 View Raw Data Sample"):
            st.dataframe(df.head(100))
            
    else:
        st.info(f"Interactive case study for {company} coming soon!")
        
    # Close database connection
    conn.close()

def show_olap_vs_oltp():
    st.header("🔍 OLAP vs OLTP")
    st.markdown("Understanding the differences between analytical and transactional processing")
    
    # Main comparison
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: #E8F4FD; padding: 25px; border-radius: 15px; border-left: 5px solid #2B6CB0;">
            <h3 style="color: #2B6CB0; margin-top: 0;">🏦 OLTP - Online Transaction Processing</h3>
            <p><strong>Purpose:</strong> Handle day-to-day transactions</p>
            <p><strong>Focus:</strong> INSERT, UPDATE, DELETE operations</p>
            <p><strong>Response Time:</strong> Milliseconds</p>
            <p><strong>Data Volume:</strong> Current data, gigabytes</p>
            <p><strong>Users:</strong> Many concurrent users</p>
            <p><strong>Schema:</strong> Highly normalized (3NF)</p>
            <p><strong>Examples:</strong> Banking, E-commerce, CRM</p>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div style="background: #F0FDF4; padding: 25px; border-radius: 15px; border-left: 5px solid #16A34A;">
            <h3 style="color: #16A34A; margin-top: 0;">📊 OLAP - Online Analytical Processing</h3>
            <p><strong>Purpose:</strong> Support business intelligence and analytics</p>
            <p><strong>Focus:</strong> SELECT operations, complex queries</p>
            <p><strong>Response Time:</strong> Seconds to minutes</p>
            <p><strong>Data Volume:</strong> Historical data, terabytes</p>
            <p><strong>Users:</strong> Few concurrent users</p>
            <p><strong>Schema:</strong> Denormalized (star/snowflake)</p>
            <p><strong>Examples:</strong> Data warehousing, BI, reporting</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Interactive comparison table
    st.subheader("📋 Detailed Comparison")
    
    comparison_data = pd.DataFrame({
        'Aspect': ['Primary Function', 'Query Complexity', 'Data Freshness', 'Storage Optimization', 
                  'Typical Users', 'Performance Metric', 'Backup Strategy', 'Indexing Strategy'],
        'OLTP': ['Transaction Processing', 'Simple queries', 'Real-time/Current', 'Write-optimized', 
                'End users, Applications', 'Throughput (TPS)', 'Frequent, point-in-time', 'Selective indexing'],
        'OLAP': ['Data Analysis', 'Complex analytical queries', 'Historical/Batch updated', 'Read-optimized',
                'Analysts, Data Scientists', 'Query performance', 'Less frequent, full backups', 'Extensive indexing']
    })
    
    st.dataframe(comparison_data, use_container_width=True)
    
    # Real-world examples with interactive charts
    st.subheader("🏢 Real-World Implementation Examples")
    
    tab1, tab2, tab3 = st.tabs(["Banking System", "E-commerce Platform", "Healthcare System"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🏦 Banking OLTP")
            st.markdown("""
            - **Account transactions**: Deposits, withdrawals, transfers
            - **ATM operations**: Balance inquiries, cash withdrawals
            - **Online banking**: Bill payments, account management
            - **Credit card processing**: Authorization, settlement
            """)
            
            # Sample transaction volume chart
            banking_data = pd.DataFrame({
                'Hour': range(24),
                'Transactions': [120, 80, 60, 40, 35, 45, 180, 320, 450, 380, 
                               420, 480, 520, 500, 460, 520, 580, 640, 580, 480, 380, 280, 200, 160]
            })
            fig_banking = px.bar(banking_data, x='Hour', y='Transactions',
                               title='Daily Transaction Volume Pattern')
            st.plotly_chart(fig_banking, use_container_width=True)
        
        with col2:
            st.markdown("### 📊 Banking OLAP")
            st.markdown("""
            - **Risk analysis**: Credit scoring, fraud detection
            - **Customer analytics**: Behavior patterns, segmentation
            - **Regulatory reporting**: Compliance, audit reports
            - **Business intelligence**: KPIs, trends, forecasting
            """)
            
            # Sample analytical query performance
            query_data = pd.DataFrame({
                'Query Type': ['Risk Assessment', 'Customer Segmentation', 'Fraud Detection', 'Regulatory Report'],
                'Avg Response (s)': [15, 45, 8, 120],
                'Data Processed (GB)': [50, 200, 25, 500]
            })
            fig_queries = px.scatter(query_data, x='Avg Response (s)', y='Data Processed (GB)',
                                   size='Data Processed (GB)', color='Query Type',
                                   title='Analytical Query Performance')
            st.plotly_chart(fig_queries, use_container_width=True)
    
    with tab2:
        st.markdown("### 🛒 E-commerce System Architecture")
        
        # Netflix-style architecture for e-commerce
        st.markdown("""
        <div style="background: linear-gradient(135deg, #667eea, #764ba2); padding: 25px; border-radius: 15px; color: white;">
            <h4 style="text-align: center; margin-bottom: 20px;">E-COMMERCE DATA ARCHITECTURE</h4>
            
            <div style="display: flex; justify-content: space-between; margin: 20px 0;">
                <div style="text-align: center;">
                    <div style="background: #4299E1; padding: 15px; border-radius: 8px; margin: 10px;">
                        <div style="font-weight: bold;">OLTP Layer</div>
                        <div style="font-size: 24px; margin: 10px 0;">🏪</div>
                        <div style="font-size: 12px;">
                            • Order Processing<br>
                            • Inventory Management<br>
                            • User Authentication<br>
                            • Payment Processing
                        </div>
                    </div>
                </div>
                
                <div style="text-align: center;">
                    <div style="background: #48BB78; padding: 15px; border-radius: 8px; margin: 10px;">
                        <div style="font-weight: bold;">ETL Pipeline</div>
                        <div style="font-size: 24px; margin: 10px 0;">🔄</div>
                        <div style="font-size: 12px;">
                            • Data Extraction<br>
                            • Transformation<br>
                            • Data Quality<br>
                            • Loading
                        </div>
                    </div>
                </div>
                
                <div style="text-align: center;">
                    <div style="background: #ED8936; padding: 15px; border-radius: 8px; margin: 10px;">
                        <div style="font-weight: bold;">OLAP Layer</div>
                        <div style="font-size: 24px; margin: 10px 0;">📊</div>
                        <div style="font-size: 12px;">
                            • Sales Analytics<br>
                            • Customer Insights<br>
                            • Product Performance<br>
                            • Forecasting
                        </div>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with tab3:
        st.markdown("### 🏥 Healthcare System")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **OLTP Applications:**
            - Patient registration and scheduling
            - Electronic health records (EHR)
            - Prescription management
            - Billing and insurance processing
            """)
            
        with col2:
            st.markdown("""
            **OLAP Applications:**
            - Population health analytics
            - Treatment outcome analysis
            - Resource utilization reporting
            - Predictive health modeling
            """)
    
    # Performance optimization tips
    st.subheader("⚡ Performance Optimization")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🏦 OLTP Optimization
        - **Indexing**: Create selective indexes on frequently queried columns
        - **Normalization**: Use 3NF to reduce data redundancy
        - **Connection pooling**: Manage database connections efficiently
        - **Caching**: Implement application-level caching
        - **Partitioning**: Partition large tables by time or key ranges
        """)
        
    with col2:
        st.markdown("""
        ### 📊 OLAP Optimization
        - **Denormalization**: Use star/snowflake schemas for faster queries
        - **Materialized views**: Pre-compute common aggregations
        - **Columnar storage**: Use column-oriented databases
        - **Data compression**: Compress historical data
        - **Parallel processing**: Leverage MPP architectures
        """)

def show_data_science_analytics():
    st.header("🧠 Data Science & Analytics")
    st.markdown("Explore machine learning pipelines and advanced analytics use cases")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Use Cases", "🤖 ML Pipelines", "🔮 Predictive Analytics", "📊 Business Analytics"])
    
    with tab1:
        st.subheader("🎯 Data Science Applications")
        
        # Create interactive use case explorer
        use_case = st.selectbox("Choose a use case to explore:", [
            "Recommendation Systems",
            "Risk Analysis", 
            "Customer Churn Analysis",
            "Credit Risk Management",
            "Portfolio Management",
            "Fraud Detection",
            "Demand Forecasting"
        ])
        
        if use_case == "Recommendation Systems":
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("""
                ### 🎬 Netflix-Style Recommendation Engine
                
                **Architecture Components:**
                - **Data Collection**: User interactions, viewing history, ratings
                - **Feature Engineering**: User profiles, content features, context
                - **Model Training**: Collaborative filtering, content-based, hybrid
                - **Real-time Serving**: Low-latency prediction API
                - **A/B Testing**: Continuous experimentation and optimization
                """)
                
                # Recommendation performance metrics
                rec_metrics = pd.DataFrame({
                    'Algorithm': ['Collaborative Filtering', 'Content-Based', 'Matrix Factorization', 'Deep Learning'],
                    'Precision': [0.75, 0.68, 0.82, 0.88],
                    'Recall': [0.65, 0.72, 0.79, 0.85],
                    'Training Time (hrs)': [2, 1, 8, 24]
                })
                
                fig_rec = px.scatter(rec_metrics, x='Precision', y='Recall', 
                                   size='Training Time (hrs)', color='Algorithm',
                                   title='Recommendation Algorithm Performance')
                st.plotly_chart(fig_rec, use_container_width=True)
            
            with col2:
                st.markdown("""
                **Tech Stack:**
                - **Spark**: Batch processing
                - **Kafka**: Real-time events
                - **Redis**: Caching recommendations
                - **TensorFlow**: Deep learning models
                - **Kubernetes**: Model serving
                """)
                
                # Sample recommendation results
                st.markdown("**Sample Output:**")
                recommendations = pd.DataFrame({
                    'User': ['User_123', 'User_456'],
                    'Top_Recommendation': ['Stranger Things', 'The Crown'],
                    'Confidence': [0.92, 0.87]
                })
                st.dataframe(recommendations, use_container_width=True)
        
        elif use_case == "Customer Churn Analysis":
            st.markdown("""
            ### 📱 Telecom Customer Churn Prediction
            
            **Business Problem:** Predict which customers are likely to cancel their subscription
            
            **Data Sources:**
            - Customer demographics and account information
            - Usage patterns (call duration, data usage, SMS)
            - Service interactions and support tickets
            - Payment history and billing data
            """)
            
            # Churn analysis visualization
            churn_data = pd.DataFrame({
                'Month': pd.date_range('2024-01-01', periods=12, freq='M'),
                'Churn_Rate': [0.05, 0.04, 0.06, 0.08, 0.07, 0.09, 0.11, 0.10, 0.08, 0.07, 0.06, 0.05],
                'Predicted_Churn': [0.052, 0.045, 0.058, 0.075, 0.072, 0.085, 0.095, 0.098, 0.082, 0.071, 0.063, 0.054]
            })
            
            fig_churn = px.line(churn_data, x='Month', y=['Churn_Rate', 'Predicted_Churn'],
                              title='Actual vs Predicted Churn Rate')
            st.plotly_chart(fig_churn, use_container_width=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **Key Features:**
                - Contract length and type
                - Average monthly spending
                - Service usage patterns
                - Customer service interactions
                - Payment method and history
                """)
            
            with col2:
                st.markdown("""
                **Business Impact:**
                - 15% reduction in churn rate
                - $2.3M annual revenue retention
                - 85% model accuracy
                - 30% improvement in campaign targeting
                """)
        
        elif use_case == "Credit Risk Management":
            st.markdown("""
            ### 🏦 Banking Credit Risk Assessment
            
            **Objective:** Assess the probability of loan default and set appropriate interest rates
            """)
            
            # Credit risk visualization
            risk_data = pd.DataFrame({
                'Credit_Score': [300, 400, 500, 600, 700, 800, 850],
                'Default_Rate': [0.45, 0.25, 0.15, 0.08, 0.04, 0.02, 0.01],
                'Loan_Volume': [1000, 2500, 8000, 15000, 25000, 18000, 5000]
            })
            
            fig_risk = px.scatter(risk_data, x='Credit_Score', y='Default_Rate',
                                size='Loan_Volume', title='Credit Score vs Default Rate')
            st.plotly_chart(fig_risk, use_container_width=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **Risk Factors:**
                - Credit history and score
                - Debt-to-income ratio
                - Employment stability
                - Collateral value
                - Economic indicators
                """)
            
            with col2:
                st.markdown("""
                **Model Outputs:**
                - Probability of default (PD)
                - Loss given default (LGD)
                - Exposure at default (EAD)
                - Risk-adjusted pricing
                """)
    
    with tab2:
        st.subheader("🤖 Machine Learning Pipeline Architecture")
        
        # ML Pipeline visualization
        st.markdown("""
        <div style="background: linear-gradient(135deg, #ff6b6b, #ee5a24); padding: 25px; border-radius: 15px; color: white; margin: 20px 0;">
            <h4 style="text-align: center; margin-bottom: 20px;">ML PIPELINE ARCHITECTURE</h4>
            
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0;">
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Data Ingestion</div>
                        <div style="font-size: 20px; margin: 10px 0;">📥</div>
                        <div style="font-size: 12px;">Kafka, Kinesis</div>
                    </div>
                </div>
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Feature Store</div>
                        <div style="font-size: 20px; margin: 10px 0;">🗃️</div>
                        <div style="font-size: 12px;">Feast, Tecton</div>
                    </div>
                </div>
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Model Training</div>
                        <div style="font-size: 20px; margin: 10px 0;">🧠</div>
                        <div style="font-size: 12px;">MLflow, Kubeflow</div>
                    </div>
                </div>
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Model Serving</div>
                        <div style="font-size: 20px; margin: 10px 0;">🚀</div>
                        <div style="font-size: 12px;">Seldon, KFServing</div>
                    </div>
                </div>
            </div>
            
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-top: 20px;">
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Monitoring</div>
                        <div style="font-size: 20px; margin: 10px 0;">📊</div>
                        <div style="font-size: 12px;">Evidently, Grafana</div>
                    </div>
                </div>
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Experiment Tracking</div>
                        <div style="font-size: 20px; margin: 10px 0;">📝</div>
                        <div style="font-size: 12px;">MLflow, W&B</div>
                    </div>
                </div>
                <div style="text-align: center;">
                    <div style="background: rgba(255,255,255,0.2); padding: 15px; border-radius: 8px;">
                        <div style="font-weight: bold;">Model Registry</div>
                        <div style="font-size: 20px; margin: 10px 0;">🏛️</div>
                        <div style="font-size: 12px;">MLflow, DVC</div>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # MLOps maturity levels
        st.subheader("📈 MLOps Maturity Levels")
        
        maturity_levels = pd.DataFrame({
            'Level': ['Level 0: Manual', 'Level 1: ML Pipeline', 'Level 2: CI/CD Pipeline'],
            'Characteristics': [
                'Manual, script-driven process',
                'Automated training pipeline',
                'Automated CI/CD system'
            ],
            'Deployment': ['Manual', 'Automated training', 'Rapid and reliable'],
            'Monitoring': ['Limited', 'Basic metrics', 'Comprehensive']
        })
        
        st.dataframe(maturity_levels, use_container_width=True)
    
    with tab3:
        st.subheader("🔮 Predictive Analytics Use Cases")
        
        prediction_type = st.selectbox("Choose prediction type:", [
            "Demand Forecasting",
            "Price Optimization", 
            "Maintenance Prediction",
            "Market Trend Analysis"
        ])
        
        if prediction_type == "Demand Forecasting":
            st.markdown("""
            ### 📦 Supply Chain Demand Forecasting
            
            **Business Challenge:** Optimize inventory levels while minimizing stockouts
            """)
            
            # Generate demand forecasting data
            dates = pd.date_range('2024-01-01', periods=365, freq='D')
            base_demand = 1000
            seasonal = 200 * np.sin(2 * np.pi * np.arange(365) / 365)
            trend = np.arange(365) * 0.5
            noise = np.random.normal(0, 50, 365)
            
            demand_data = pd.DataFrame({
                'Date': dates,
                'Actual_Demand': base_demand + seasonal + trend + noise,
                'Predicted_Demand': base_demand + seasonal + trend + np.random.normal(0, 25, 365)
            })
            
            fig_demand = px.line(demand_data, x='Date', y=['Actual_Demand', 'Predicted_Demand'],
                               title='Demand Forecasting: Actual vs Predicted')
            st.plotly_chart(fig_demand, use_container_width=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **Features Used:**
                - Historical demand patterns
                - Seasonal trends
                - Economic indicators
                - Marketing campaigns
                - Weather data
                """)
            
            with col2:
                accuracy_metrics = pd.DataFrame({
                    'Metric': ['MAPE', 'RMSE', 'MAE'],
                    'Value': ['8.5%', '45.2', '32.1'],
                    'Target': ['<10%', '<50', '<35']
                })
                st.dataframe(accuracy_metrics, use_container_width=True)
    
    with tab4:
        st.subheader("📊 Business Analytics Dashboard")
        
        # Generate business metrics
        business_metrics = pd.DataFrame({
            'KPI': ['Customer Acquisition Cost', 'Customer Lifetime Value', 'Conversion Rate', 'Retention Rate'],
            'Current': [150, 1200, 3.2, 85.5],
            'Target': [120, 1500, 4.0, 90.0],
            'Unit': ['$', '$', '%', '%']
        })
        
        # Create metrics visualization
        fig_kpi = go.Figure()
        fig_kpi.add_trace(go.Bar(x=business_metrics['KPI'], y=business_metrics['Current'], 
                               name='Current', marker_color='lightblue'))
        fig_kpi.add_trace(go.Bar(x=business_metrics['KPI'], y=business_metrics['Target'], 
                               name='Target', marker_color='orange'))
        fig_kpi.update_layout(title='Key Performance Indicators', barmode='group')
        st.plotly_chart(fig_kpi, use_container_width=True)
        
        # Real-time metrics simulation
        st.markdown("### 📈 Real-time Analytics Simulation")
        if st.button("Generate Real-time Data"):
            metrics_placeholder = st.empty()
            for i in range(10):
                current_metrics = {
                    'Active Users': np.random.randint(8500, 12000),
                    'Revenue Today': np.random.randint(45000, 65000),
                    'Conversion Rate': round(np.random.uniform(2.8, 4.2), 2),
                    'Avg Session Duration': f"{np.random.randint(8, 15)}min {np.random.randint(10, 59)}s"
                }
                
                with metrics_placeholder.container():
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Active Users", current_metrics['Active Users'])
                    with col2:
                        st.metric("Revenue Today", f"${current_metrics['Revenue Today']:,}")
                    with col3:
                        st.metric("Conversion Rate", f"{current_metrics['Conversion Rate']}%")
                    with col4:
                        st.metric("Avg Session", current_metrics['Avg Session Duration'])
                
                import time
                time.sleep(1)

def show_control_and_logs():
    st.header("📊 Control and Logs")
    log_activity("INFO", "Control and Logs", "User accessed Control and Logs module")
    
    # Create tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs(["📊 System Status", "📝 Application Logs", "🎛️ Control Panel", "📈 Log Analytics"])
    
    with tab1:
        st.subheader("🖥️ System Status & Monitoring")
        
        # System metrics simulation
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("CPU Usage", f"{np.random.randint(15, 85)}%", delta=f"{np.random.randint(-5, 5)}%")
        with col2:
            st.metric("Memory Usage", f"{np.random.randint(40, 90)}%", delta=f"{np.random.randint(-3, 8)}%")
        with col3:
            st.metric("Active Sessions", np.random.randint(10, 50), delta=np.random.randint(-2, 5))
        with col4:
            st.metric("Database Connections", np.random.randint(5, 20), delta=np.random.randint(-1, 3))
        
        st.markdown("---")
        
        # System health chart
        st.subheader("📈 System Health Over Time")
        health_data = pd.DataFrame({
            'timestamp': pd.date_range(start=datetime.now()-timedelta(hours=24), end=datetime.now(), freq='H'),
            'cpu_usage': np.random.randint(10, 90, 25),
            'memory_usage': np.random.randint(30, 95, 25),
            'response_time': np.random.uniform(50, 500, 25)
        })
        
        fig = px.line(health_data, x='timestamp', y=['cpu_usage', 'memory_usage'], 
                     title="System Resource Usage (24 Hours)")
        st.plotly_chart(fig, use_container_width=True)
        
    with tab2:
        st.subheader("📝 Application Logs")
        
        # Controls for log filtering
        col1, col2, col3 = st.columns(3)
        with col1:
            log_level_filter = st.selectbox("Log Level:", ["ALL", "INFO", "WARNING", "ERROR", "DEBUG"])
        with col2:
            module_filter = st.selectbox("Module:", ["ALL", "Data Ingestion", "Data Storage", "ETL/ELT", "Control and Logs"])
        with col3:
            limit_logs = st.slider("Show last N logs:", 10, 1000, 100)
        
        # Fetch logs from database
        conn = sqlite3.connect('app_logs.db')
        query = "SELECT * FROM app_logs ORDER BY timestamp DESC LIMIT ?"
        params = [limit_logs]
        
        if log_level_filter != "ALL":
            query = query.replace("ORDER BY", "WHERE level = ? ORDER BY")
            params = [log_level_filter] + params
            
        logs_df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if not logs_df.empty:
            # Style the logs dataframe
            st.dataframe(logs_df, use_container_width=True, height=400)
            
            # Download logs option
            csv = logs_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Logs as CSV",
                data=csv,
                file_name=f"app_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No logs found matching the selected criteria.")
    
    with tab3:
        st.subheader("🎛️ Control Panel")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### System Controls")
            
            if st.button("🔄 Restart Application", type="secondary"):
                log_activity("WARNING", "Control Panel", "Application restart requested")
                st.success("Application restart initiated!")
                
            if st.button("🧹 Clear Cache", type="secondary"):
                st.cache_data.clear()
                log_activity("INFO", "Control Panel", "Cache cleared")
                st.success("Cache cleared successfully!")
                
            if st.button("🗑️ Clear Logs", type="secondary"):
                conn = sqlite3.connect('app_logs.db')
                cursor = conn.cursor()
                cursor.execute("DELETE FROM app_logs")
                conn.commit()
                conn.close()
                log_activity("WARNING", "Control Panel", "All logs cleared")
                st.success("All logs have been cleared!")
        
        with col2:
            st.markdown("#### Database Controls")
            
            if st.button("🔍 Test Database Connection"):
                try:
                    conn = sqlite3.connect('app_logs.db')
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM app_logs")
                    count = cursor.fetchone()[0]
                    conn.close()
                    st.success(f"✅ Database connected successfully! Total logs: {count}")
                    log_activity("INFO", "Control Panel", f"Database connection test successful, {count} logs found")
                except Exception as e:
                    st.error(f"❌ Database connection failed: {str(e)}")
                    log_activity("ERROR", "Control Panel", f"Database connection test failed: {str(e)}")
            
            if st.button("📊 Database Stats"):
                try:
                    conn = sqlite3.connect('app_logs.db')
                    stats_df = pd.read_sql_query("""
                        SELECT 
                            level,
                            COUNT(*) as count,
                            MIN(timestamp) as first_log,
                            MAX(timestamp) as last_log
                        FROM app_logs 
                        GROUP BY level
                    """, conn)
                    conn.close()
                    
                    if not stats_df.empty:
                        st.dataframe(stats_df, use_container_width=True)
                    else:
                        st.info("No log statistics available.")
                except Exception as e:
                    st.error(f"Error fetching database stats: {str(e)}")
    
    with tab4:
        st.subheader("📈 Log Analytics")
        
        try:
            conn = sqlite3.connect('app_logs.db')
            
            # Log level distribution
            level_stats = pd.read_sql_query("""
                SELECT level, COUNT(*) as count 
                FROM app_logs 
                GROUP BY level 
                ORDER BY count DESC
            """, conn)
            
            if not level_stats.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_pie = px.pie(level_stats, values='count', names='level', 
                                   title="Log Level Distribution")
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                with col2:
                    fig_bar = px.bar(level_stats, x='level', y='count', 
                                   title="Logs by Level")
                    st.plotly_chart(fig_bar, use_container_width=True)
            
            # Module activity over time
            module_stats = pd.read_sql_query("""
                SELECT 
                    module, 
                    COUNT(*) as activity_count,
                    DATE(timestamp) as date
                FROM app_logs 
                GROUP BY module, DATE(timestamp)
                ORDER BY date DESC
            """, conn)
            
            if not module_stats.empty:
                st.subheader("📊 Module Activity Over Time")
                fig_timeline = px.line(module_stats, x='date', y='activity_count', 
                                     color='module', title="Module Usage Timeline")
                st.plotly_chart(fig_timeline, use_container_width=True)
            
            conn.close()
            
        except Exception as e:
            st.error(f"Error generating analytics: {str(e)}")
            log_activity("ERROR", "Control Panel", f"Analytics generation failed: {str(e)}")

if __name__ == "__main__":
    main()