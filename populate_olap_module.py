
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Function to initialize Module 5 database and create tables
def init_module5_database():
    conn = sqlite3.connect('module5_olap_aggregates.db', check_same_thread=False)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Create aggregate tables for each company
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_uber_daily_revenue (
            date TEXT,
            city TEXT,
            total_rides INTEGER,
            completed_rides INTEGER,
            gross_revenue_aed REAL,
            avg_fare_aed REAL,
            cancellation_rate REAL,
            PRIMARY KEY (date, city)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_netflix_hourly_engagement (
            date_hour TEXT PRIMARY KEY,
            content_id TEXT,
            views INTEGER,
            unique_viewers INTEGER,
            avg_watch_sec REAL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_amazon_daily_sales (
            date TEXT,
            category TEXT,
            orders INTEGER,
            units_sold INTEGER,
            gross_revenue_aed REAL,
            returns INTEGER,
            PRIMARY KEY (date, category)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_airbnb_occupancy (
            date TEXT,
            city TEXT,
            occupied_nights INTEGER,
            available_nights INTEGER,
            occupancy_rate REAL,
            revenue_aed REAL,
            PRIMARY KEY (date, city)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_nyse_minute_ohlc (
            ticker TEXT,
            minute_ts TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (ticker, minute_ts)
        )
    """)
    
    conn.commit()
    return conn

# Data generation function for Uber OLAP aggregates
def generate_uber_olap_daily_revenue(n_days=90):
    np.random.seed(53)
    data = []
    cities = ['Dubai', 'Abu Dhabi', 'Sharjah']
    
    for i in range(n_days):
        current_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        for city in cities:
            total_rides = np.random.randint(500, 5000)
            completed_rides = int(total_rides * np.random.uniform(0.85, 0.98))
            gross_revenue_aed = round(total_rides * np.random.uniform(20, 60), 2)
            avg_fare_aed = round(gross_revenue_aed / completed_rides if completed_rides > 0 else 0, 2)
            cancellation_rate = round(1 - (completed_rides / total_rides), 4)
            
            data.append({
                'date': current_date,
                'city': city,
                'total_rides': total_rides,
                'completed_rides': completed_rides,
                'gross_revenue_aed': gross_revenue_aed,
                'avg_fare_aed': avg_fare_aed,
                'cancellation_rate': cancellation_rate
            })
    return pd.DataFrame(data)

# Netflix OLAP data generation
def generate_netflix_olap_hourly_engagement():
    np.random.seed(44)
    data = []
    start_date = datetime.now() - timedelta(days=30)
    
    content_ids = [f"content_{i:05d}" for i in range(1000, 1020)]  # 20 popular contents
    
    for day in range(30):  # 30 days of data
        date = start_date + timedelta(days=day)
        for hour in range(24):  # Every hour
            date_hour = (date + timedelta(hours=hour)).strftime('%Y-%m-%d %H:00:00')
            content_id = np.random.choice(content_ids)
            
            # Peak hours have higher engagement (evening hours)
            if 18 <= hour <= 23:  # Evening peak
                base_views = np.random.randint(5000, 15000)
            elif 12 <= hour <= 17:  # Afternoon
                base_views = np.random.randint(2000, 8000)
            else:  # Off-peak
                base_views = np.random.randint(500, 3000)
            
            views = base_views
            unique_viewers = int(views * np.random.uniform(0.6, 0.9))  # Some users watch multiple times
            avg_watch_sec = np.random.randint(300, 3600)  # 5min to 1 hour average watch time
            
            data.append({
                'date_hour': date_hour,
                'content_id': content_id,
                'views': views,
                'unique_viewers': unique_viewers,
                'avg_watch_sec': avg_watch_sec
            })
    
    return pd.DataFrame(data)

# Amazon OLAP data generation
def generate_amazon_olap_daily_sales():
    np.random.seed(45)
    data = []
    start_date = datetime.now() - timedelta(days=90)
    
    categories = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports', 'Beauty', 'Automotive', 'Toys']
    
    for day in range(90):  # 90 days of data
        date = (start_date + timedelta(days=day)).strftime('%Y-%m-%d')
        
        for category in categories:
            # Weekend vs weekday patterns
            is_weekend = (start_date + timedelta(days=day)).weekday() >= 5
            multiplier = 1.3 if is_weekend else 1.0
            
            # Category-specific base volumes
            if category == 'Electronics':
                base_orders = int(np.random.randint(100, 500) * multiplier)
            elif category == 'Fashion':
                base_orders = int(np.random.randint(200, 800) * multiplier)
            else:
                base_orders = int(np.random.randint(50, 300) * multiplier)
            
            orders = base_orders
            units_sold = int(orders * np.random.uniform(1.2, 3.0))  # Multiple items per order
            avg_order_value = np.random.uniform(50, 300)  # AED per order
            gross_revenue_aed = orders * avg_order_value
            returns = int(orders * np.random.uniform(0.05, 0.15))  # 5-15% return rate
            
            data.append({
                'date': date,
                'category': category,
                'orders': orders,
                'units_sold': units_sold,
                'gross_revenue_aed': round(gross_revenue_aed, 2),
                'returns': returns
            })
    
    return pd.DataFrame(data)

# Airbnb OLAP data generation  
def generate_airbnb_olap_occupancy():
    np.random.seed(46)
    data = []
    start_date = datetime.now() - timedelta(days=60)
    
    cities = ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Ras Al Khaimah']
    
    for day in range(60):  # 60 days of data
        date = (start_date + timedelta(days=day)).strftime('%Y-%m-%d')
        
        for city in cities:
            # Dubai has higher occupancy than other cities
            if city == 'Dubai':
                available_nights = np.random.randint(8000, 12000)
                occupancy_rate = np.random.uniform(0.65, 0.85)
            elif city == 'Abu Dhabi':
                available_nights = np.random.randint(3000, 6000)
                occupancy_rate = np.random.uniform(0.55, 0.75)
            else:
                available_nights = np.random.randint(1000, 3000)
                occupancy_rate = np.random.uniform(0.45, 0.65)
            
            occupied_nights = int(available_nights * occupancy_rate)
            avg_price_per_night = np.random.uniform(150, 800)  # AED
            revenue_aed = occupied_nights * avg_price_per_night
            
            data.append({
                'date': date,
                'city': city,
                'occupied_nights': occupied_nights,
                'available_nights': available_nights,
                'occupancy_rate': round(occupancy_rate, 3),
                'revenue_aed': round(revenue_aed, 2)
            })
    
    return pd.DataFrame(data)

# NYSE OLAP data generation
def generate_nyse_olap_minute_ohlc():
    np.random.seed(47)
    data = []
    start_time = datetime.now() - timedelta(days=7)  # 1 week of minute data
    
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM']
    base_prices = {'AAPL': 150, 'MSFT': 300, 'GOOGL': 2500, 'AMZN': 3200, 
                   'TSLA': 800, 'META': 200, 'NVDA': 400, 'JPM': 140}
    
    # Generate minute-by-minute OHLC data for market hours only
    for day in range(7):
        date = start_time + timedelta(days=day)
        # Skip weekends
        if date.weekday() >= 5:
            continue
            
        # Market hours: 9:30 AM - 4:00 PM (6.5 hours * 60 = 390 minutes)
        market_start = date.replace(hour=9, minute=30, second=0, microsecond=0)
        
        for minute in range(390):  # 390 trading minutes per day
            minute_ts = market_start + timedelta(minutes=minute)
            
            for ticker in tickers:
                base_price = base_prices[ticker]
                
                # Simulate price movement with some volatility
                price_change = np.random.normal(0, base_price * 0.001)  # 0.1% volatility per minute
                open_price = base_price + price_change
                
                # OHLC within the minute
                minute_volatility = abs(np.random.normal(0, base_price * 0.0005))
                high = open_price + minute_volatility
                low = open_price - minute_volatility
                close_price = open_price + np.random.normal(0, minute_volatility)
                
                # Volume varies throughout the day
                if 60 <= minute <= 120 or 300 <= minute <= 390:  # Opening and closing hours
                    volume = np.random.randint(50000, 200000)
                else:
                    volume = np.random.randint(10000, 80000)
                
                data.append({
                    'ticker': ticker,
                    'minute_ts': minute_ts.strftime('%Y-%m-%d %H:%M:00'),
                    'open': round(open_price, 2),
                    'high': round(high, 2),
                    'low': round(low, 2),
                    'close': round(close_price, 2),
                    'volume': volume
                })
    
    return pd.DataFrame(data)

# Function to populate Module 5 database with synthetic data
def populate_module5_data(conn, company_name):
    cursor = conn.cursor()
    
    # Check if data already exists for this company
    table_map = {
        'Uber': 'agg_uber_daily_revenue',
        'Netflix': 'agg_netflix_hourly_engagement',
        'Amazon': 'agg_amazon_daily_sales',
        'Airbnb': 'agg_airbnb_occupancy',
        'NYSE': 'agg_nyse_minute_ohlc'
    }
    
    table_name = table_map[company_name]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    
    if count > 0:
        print(f"Data already exists for {company_name}. Skipping population.")
        return
        
    try:
        cursor.execute("BEGIN")
        
        if company_name == "Uber":
            data = generate_uber_olap_daily_revenue()
            data.to_sql('agg_uber_daily_revenue', conn, if_exists='append', index=False)
        elif company_name == "Netflix":
            data = generate_netflix_olap_hourly_engagement()
            data.to_sql('agg_netflix_hourly_engagement', conn, if_exists='append', index=False)
        elif company_name == "Amazon":
            data = generate_amazon_olap_daily_sales()
            data.to_sql('agg_amazon_daily_sales', conn, if_exists='append', index=False)
        elif company_name == "Airbnb":
            data = generate_airbnb_olap_occupancy()
            data.to_sql('agg_airbnb_occupancy', conn, if_exists='append', index=False)
        elif company_name == "NYSE":
            data = generate_nyse_olap_minute_ohlc()
            data.to_sql('agg_nyse_minute_ohlc', conn, if_exists='append', index=False)
        else:
            print(f"Data generation for {company_name} is not implemented in this script.")
            return
            
        conn.commit()
        print(f"Populated {company_name} OLAP data.")
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Error populating Module 5 data for {company_name}: {str(e)}")
        raise e

# Main execution for OLAP module (all companies)
if __name__ == "__main__":
    print("Initializing Module 5 database and populating OLAP data for all companies...")
    conn = init_module5_database()
    
    companies = ["Uber", "Netflix", "Amazon", "Airbnb", "NYSE"]
    
    for company in companies:
        print(f"Populating data for {company}...")
        populate_module5_data(conn, company)
    
    conn.close()
    print("OLAP module data population complete for all companies.")
