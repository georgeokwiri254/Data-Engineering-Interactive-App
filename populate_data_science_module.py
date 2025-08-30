
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random

# Function to initialize Module 7 database and create tables
def init_module7_database():
    conn = sqlite3.connect('module7_ml_features.db', check_same_thread=False)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Create feature tables for each company
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS features_uber_ride (
            ride_id TEXT PRIMARY KEY,
            pickup_hour INTEGER,
            is_peak INTEGER, -- 0 or 1
            driver_accept_rate REAL,
            predicted_fare_aed REAL,
            label_cancelled INTEGER -- 0 or 1
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS features_netflix_session (
            session_id TEXT PRIMARY KEY,
            user_avg_watch_7d REAL,
            content_popularity_rank INTEGER,
            device_type_enc INTEGER,
            label_churn_risk INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS features_amazon_order (
            order_id TEXT PRIMARY KEY,
            customer_ltv_aed REAL,
            items_count INTEGER,
            discount_pct REAL,
            label_returned INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS features_airbnb_booking (
            booking_id TEXT PRIMARY KEY,
            host_response_time_h REAL,
            guest_past_cancellations INTEGER,
            price_sensitivity_score REAL,
            label_cancelled INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS features_nyse_minute (
            minute_ts TEXT,
            ticker TEXT,
            price_momentum REAL,
            volatility_5min REAL,
            label_price_up_next_min INTEGER,
            PRIMARY KEY (minute_ts, ticker)
        )
    """)
    
    # Create model artifacts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS model_artifacts (
            model_id TEXT PRIMARY KEY,
            model_name TEXT,
            version TEXT,
            train_ts TEXT,
            hyperparameters TEXT, -- JSON
            metrics TEXT,         -- JSON
            artifact_path TEXT,
            split TEXT,
            seed INTEGER
        )
    """)
    
    conn.commit()
    return conn

# Data generation function for Uber ML features
def generate_uber_ml_features(n_records=500):
    np.random.seed(54)
    data = []
    
    for i in range(n_records):
        pickup_time = datetime.now() - timedelta(hours=np.random.randint(0, 720)) # Last 30 days
        
        data.append({
            'ride_id': f'ride_{i:06d}',
            'pickup_hour': pickup_time.hour,
            'is_peak': 1 if 7 <= pickup_time.hour <= 9 or 17 <= pickup_time.hour <= 19 else 0,
            'driver_accept_rate': round(np.random.uniform(0.7, 0.99), 2),
            'predicted_fare_aed': round(np.random.uniform(15, 150), 2),
            'label_cancelled': 1 if np.random.random() < 0.1 else 0 # 10% cancellation rate
        })
    return pd.DataFrame(data)

# Data generation function for model artifacts
def generate_model_artifacts(n_models=10, company_name='Uber'):
    np.random.seed(54)
    data = []
    model_names = [
        f'{company_name}_fare_prediction',
        f'{company_name}_cancellation_prediction',
        f'{company_name}_driver_matching',
        f'{company_name}_surge_prediction',
    ]
    splits = ['train', 'validation', 'test']
    
    for i in range(n_models):
        train_time = datetime.now() - timedelta(days=np.random.randint(0, 60))
        
        hyperparameters = {
            'learning_rate': round(np.random.uniform(0.001, 0.1), 3),
            'n_estimators': np.random.randint(100, 1000),
            'max_depth': np.random.randint(3, 10)
        }
        
        metrics = {
            'accuracy': round(np.random.uniform(0.8, 0.95), 3),
            'precision': round(np.random.uniform(0.7, 0.9), 3),
            'recall': round(np.random.uniform(0.6, 0.85), 3),
            'f1_score': round(np.random.uniform(0.65, 0.88), 3)
        }
        
        data.append({
            'model_id': f'model_{company_name}_{i:04d}',
            'model_name': np.random.choice(model_names),
            'version': f'v{np.random.randint(1, 5)}.{np.random.randint(0, 10)}',
            'train_ts': train_time.strftime('%Y-%m-%d %H:%M:%S'),
            'hyperparameters': json.dumps(hyperparameters),
            'metrics': json.dumps(metrics),
            'artifact_path': f's3://ml-artifacts/{company_name}/model_{i:04d}.pkl',
            'split': np.random.choice(splits),
            'seed': np.random.randint(1, 1000)
        })
    return pd.DataFrame(data)

# Netflix ML features generation
def generate_netflix_ml_features(n=400):
    np.random.seed(54)
    data = []
    for i in range(n):
        data.append({
            'session_id': f'netflix_session_{i:06d}',
            'user_avg_watch_7d': round(np.random.uniform(0.5, 8.0), 2),  # Average hours per day
            'content_popularity_rank': np.random.randint(1, 1000),
            'device_type_enc': np.random.randint(0, 5),  # Encoded device types
            'label_churn_risk': np.random.choice([0, 1], p=[0.85, 0.15])  # 15% churn risk
        })
    return pd.DataFrame(data)

# Amazon ML features generation
def generate_amazon_ml_features(n=350):
    np.random.seed(55)
    data = []
    for i in range(n):
        ltv = np.random.lognormal(mean=5, sigma=1)  # Customer lifetime value
        items_count = np.random.randint(1, 10)
        discount_pct = np.random.uniform(0, 0.5)
        
        data.append({
            'order_id': f'amazon_order_{i:06d}',
            'customer_ltv_aed': round(ltv, 2),
            'items_count': items_count,
            'discount_pct': round(discount_pct, 3),
            'label_returned': np.random.choice([0, 1], p=[0.9, 0.1])  # 10% return rate
        })
    return pd.DataFrame(data)

# Airbnb ML features generation
def generate_airbnb_ml_features(n=300):
    np.random.seed(56)
    data = []
    for i in range(n):
        data.append({
            'booking_id': f'airbnb_booking_{i:06d}',
            'host_response_time_h': round(np.random.exponential(scale=2), 1),  # Hours to respond
            'guest_past_cancellations': np.random.randint(0, 5),
            'price_sensitivity_score': round(np.random.uniform(0, 1), 3),
            'label_cancelled': np.random.choice([0, 1], p=[0.88, 0.12])  # 12% cancellation rate
        })
    return pd.DataFrame(data)

# NYSE ML features generation
def generate_nyse_ml_features(n=800):
    np.random.seed(57)
    data = []
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    base_time = datetime.now() - timedelta(days=7)  # Past week
    
    # Generate systematic time series to avoid duplicates
    for i, ticker in enumerate(tickers):
        for minute_offset in range(n // len(tickers)):
            minute_ts = base_time + timedelta(minutes=minute_offset * 5 + i)  # 5-minute intervals offset per ticker
            data.append({
                'minute_ts': minute_ts.strftime('%Y-%m-%d %H:%M:00'),
                'ticker': ticker,
                'price_momentum': round(np.random.uniform(-0.05, 0.05), 4),  # 5% momentum range
                'volatility_5min': round(np.random.uniform(0.001, 0.02), 5),  # Volatility measure
                'label_price_up_next_min': np.random.choice([0, 1], p=[0.5, 0.5])  # 50/50 price direction
            })
    
    return pd.DataFrame(data)

# Function to populate Module 7 database with synthetic data
def populate_module7_data(conn, company_name):
    cursor = conn.cursor()
    
    # Check if data already exists for this company
    # For ML features, we check a representative table
    table_map = {
        'Uber': 'features_uber_ride',
        'Netflix': 'features_netflix_session',
        'Amazon': 'features_amazon_order',
        'Airbnb': 'features_airbnb_booking',
        'NYSE': 'features_nyse_minute'
    }
    
    feature_table_name = table_map[company_name]
    cursor.execute(f"SELECT COUNT(*) FROM {feature_table_name}")
    feature_count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM model_artifacts WHERE model_name LIKE '{company_name}%'")
    artifact_count = cursor.fetchone()[0]
    
    if feature_count > 0 and artifact_count > 0:
        print(f"Data already exists for {company_name}. Skipping population.")
        return
        
    try:
        cursor.execute("BEGIN")
        
        if company_name == "Uber":
            features_data = generate_uber_ml_features()
            model_artifacts_data = generate_model_artifacts(company_name='Uber')
            
            features_data.to_sql('features_uber_ride', conn, if_exists='append', index=False)
            model_artifacts_data.to_sql('model_artifacts', conn, if_exists='append', index=False)
            
        elif company_name == "Netflix":
            features_data = generate_netflix_ml_features()
            model_artifacts_data = generate_model_artifacts(company_name='Netflix')
            
            features_data.to_sql('features_netflix_session', conn, if_exists='append', index=False)
            model_artifacts_data.to_sql('model_artifacts', conn, if_exists='append', index=False)
            
        elif company_name == "Amazon":
            features_data = generate_amazon_ml_features()
            model_artifacts_data = generate_model_artifacts(company_name='Amazon')
            
            features_data.to_sql('features_amazon_order', conn, if_exists='append', index=False)
            model_artifacts_data.to_sql('model_artifacts', conn, if_exists='append', index=False)
            
        elif company_name == "Airbnb":
            features_data = generate_airbnb_ml_features()
            model_artifacts_data = generate_model_artifacts(company_name='Airbnb')
            
            features_data.to_sql('features_airbnb_booking', conn, if_exists='append', index=False)
            model_artifacts_data.to_sql('model_artifacts', conn, if_exists='append', index=False)
            
        elif company_name == "NYSE":
            features_data = generate_nyse_ml_features()
            model_artifacts_data = generate_model_artifacts(company_name='NYSE')
            
            features_data.to_sql('features_nyse_minute', conn, if_exists='append', index=False)
            model_artifacts_data.to_sql('model_artifacts', conn, if_exists='append', index=False)
            
        else:
            print(f"Data generation for {company_name} is not implemented in this script.")
            return
            
        conn.commit()
        print(f"Populated {company_name} Data Science data.")
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Error populating Module 7 data for {company_name}: {str(e)}")
        raise e

# Main execution for Data Science module (all companies)
if __name__ == "__main__":
    print("Initializing Module 7 database and populating Data Science data for all companies...")
    conn = init_module7_database()
    
    companies = ["Uber", "Netflix", "Amazon", "Airbnb", "NYSE"]
    
    for company in companies:
        print(f"Populating data for {company}...")
        populate_module7_data(conn, company)
    
    conn.close()
    print("Data Science module data population complete for all companies.")
