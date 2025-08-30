
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random

# Extracted from app.py - init_module4_database
def init_module4_database():
    """Initialize Module 4 SQLite database for OLTP (Transactional Schemas)"""
    conn = sqlite3.connect('module4_oltp.db', check_same_thread=False)
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Uber
    cursor.execute("CREATE TABLE IF NOT EXISTS uber_users (user_id TEXT PRIMARY KEY, name TEXT, signup_date TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS uber_drivers (driver_id TEXT PRIMARY KEY, name TEXT, rating REAL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS uber_rides (ride_id TEXT PRIMARY KEY, user_id TEXT, driver_id TEXT, status TEXT, FOREIGN KEY(user_id) REFERENCES uber_users(user_id), FOREIGN KEY(driver_id) REFERENCES uber_drivers(driver_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS uber_payments (payment_id TEXT PRIMARY KEY, ride_id TEXT, amount REAL, status TEXT, FOREIGN KEY(ride_id) REFERENCES uber_rides(ride_id))")

    # Netflix
    cursor.execute("CREATE TABLE IF NOT EXISTS netflix_users (user_id TEXT PRIMARY KEY, name TEXT, email TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS netflix_profiles (profile_id TEXT PRIMARY KEY, user_id TEXT, name TEXT, FOREIGN KEY(user_id) REFERENCES netflix_users(user_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS netflix_subscriptions (subscription_id TEXT PRIMARY KEY, user_id TEXT, plan TEXT, status TEXT, FOREIGN KEY(user_id) REFERENCES netflix_users(user_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS netflix_content_catalog (content_id TEXT PRIMARY KEY, title TEXT, type TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS netflix_views (view_id TEXT PRIMARY KEY, profile_id TEXT, content_id TEXT, view_date TEXT, FOREIGN KEY(profile_id) REFERENCES netflix_profiles(profile_id), FOREIGN KEY(content_id) REFERENCES netflix_content_catalog(content_id))")

    # Amazon
    cursor.execute("CREATE TABLE IF NOT EXISTS amazon_customers (customer_id TEXT PRIMARY KEY, name TEXT, join_date TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS amazon_products (product_id TEXT PRIMARY KEY, name TEXT, price REAL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS amazon_orders (order_id TEXT PRIMARY KEY, customer_id TEXT, order_date TEXT, status TEXT, FOREIGN KEY(customer_id) REFERENCES amazon_customers(customer_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS amazon_order_items (item_id TEXT PRIMARY KEY, order_id TEXT, product_id TEXT, quantity INTEGER, FOREIGN KEY(order_id) REFERENCES amazon_orders(order_id), FOREIGN KEY(product_id) REFERENCES amazon_products(product_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS amazon_shipments (shipment_id TEXT PRIMARY KEY, order_id TEXT, status TEXT, tracking_number TEXT, FOREIGN KEY(order_id) REFERENCES amazon_orders(order_id))")

    # Airbnb
    cursor.execute("CREATE TABLE IF NOT EXISTS airbnb_guests (guest_id TEXT PRIMARY KEY, name TEXT, member_since TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS airbnb_hosts (host_id TEXT PRIMARY KEY, name TEXT, is_superhost INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS airbnb_properties (property_id TEXT PRIMARY KEY, host_id TEXT, title TEXT, city TEXT, FOREIGN KEY(host_id) REFERENCES airbnb_hosts(host_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS airbnb_bookings (booking_id TEXT PRIMARY KEY, guest_id TEXT, property_id TEXT, checkin_date TEXT, checkout_date TEXT, FOREIGN KEY(guest_id) REFERENCES airbnb_guests(guest_id), FOREIGN KEY(property_id) REFERENCES airbnb_properties(property_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS airbnb_reviews (review_id TEXT PRIMARY KEY, booking_id TEXT, rating INTEGER, comment TEXT, FOREIGN KEY(booking_id) REFERENCES airbnb_bookings(booking_id))")

    # NYSE
    cursor.execute("CREATE TABLE IF NOT EXISTS nyse_accounts (account_id TEXT PRIMARY KEY, name TEXT, balance REAL)")
    cursor.execute("CREATE TABLE IF NOT EXISTS nyse_orders (order_id TEXT PRIMARY KEY, account_id TEXT, ticker TEXT, type TEXT, quantity INTEGER, price REAL, status TEXT, FOREIGN KEY(account_id) REFERENCES nyse_accounts(account_id))")
    cursor.execute("CREATE TABLE IF NOT EXISTS nyse_transactions (transaction_id TEXT PRIMARY KEY, order_id TEXT, transaction_time TEXT, FOREIGN KEY(order_id) REFERENCES nyse_orders(order_id))")

    conn.commit()
    return conn

# Extracted from app.py - generate_uber_oltp_users
def generate_uber_oltp_users(n_records=100):
    np.random.seed(48)
    data = []
    for i in range(n_records):
        data.append({
            'user_id': f'usr_{i:05d}',
            'name': f'Rider {i}',
            'signup_date': (datetime.now() - timedelta(days=np.random.randint(1, 730))).strftime('%Y-%m-%d')
        })
    return pd.DataFrame(data)

# Extracted from app.py - generate_uber_oltp_drivers
def generate_uber_oltp_drivers(n_records=50):
    np.random.seed(48)
    data = []
    for i in range(n_records):
        data.append({
            'driver_id': f'drv_{i:04d}',
            'name': f'Driver {i}',
            'rating': round(np.random.uniform(4.0, 5.0), 2)
        })
    return pd.DataFrame(data)

# Extracted from app.py - generate_uber_oltp_rides
def generate_uber_oltp_rides(n_records=200, user_ids=None, driver_ids=None):
    np.random.seed(48)
    data = []
    for i in range(n_records):
        data.append({
            'ride_id': f'ride_{i:06d}',
            'user_id': np.random.choice(user_ids) if user_ids else f'usr_{np.random.randint(0, 100):05d}',
            'driver_id': np.random.choice(driver_ids) if driver_ids else f'drv_{np.random.randint(0, 50):04d}',
            'status': np.random.choice(['completed', 'cancelled', 'ongoing'])
        })
    return pd.DataFrame(data)

# Extracted from app.py - generate_uber_oltp_payments
def generate_uber_oltp_payments(n_records=200, ride_ids=None):
    np.random.seed(48)
    data = []
    for i in range(n_records):
        data.append({
            'payment_id': f'pay_{i:06d}',
            'ride_id': np.random.choice(ride_ids) if ride_ids else f'ride_{np.random.randint(0, 200):06d}',
            'amount': round(np.random.uniform(10, 100), 2),
            'status': np.random.choice(['paid', 'pending', 'failed'])
        })
    return pd.DataFrame(data)

# Netflix OLTP data generation functions
def generate_netflix_oltp_users(n=100):
    np.random.seed(44)
    data = []
    for i in range(n):
        data.append({
            'user_id': f'netflix_user_{i:06d}',
            'name': f'User_{i:04d}',
            'email': f'user{i}@example.com'
        })
    return pd.DataFrame(data)

def generate_netflix_oltp_profiles(n=150, user_ids=None):
    np.random.seed(44)
    if user_ids is None:
        user_ids = [f'netflix_user_{i:06d}' for i in range(50)]
    
    data = []
    profile_names = ['Main', 'Kids', 'Personal', 'Work', 'Guest']
    for i in range(n):
        data.append({
            'profile_id': f'netflix_profile_{i:06d}',
            'user_id': np.random.choice(user_ids),
            'name': f"{np.random.choice(profile_names)}_{i}"
        })
    return pd.DataFrame(data)

def generate_netflix_oltp_subscriptions(n=100, user_ids=None):
    np.random.seed(44)
    if user_ids is None:
        user_ids = [f'netflix_user_{i:06d}' for i in range(50)]
    
    data = []
    plans = ['Basic', 'Standard', 'Premium']
    statuses = ['Active', 'Cancelled', 'Paused']
    for i in range(n):
        data.append({
            'subscription_id': f'netflix_sub_{i:06d}',
            'user_id': np.random.choice(user_ids),
            'plan': np.random.choice(plans),
            'status': np.random.choice(statuses, p=[0.85, 0.1, 0.05])
        })
    return pd.DataFrame(data)

def generate_netflix_oltp_content(n=50):
    np.random.seed(44)
    data = []
    types = ['Movie', 'Series', 'Documentary', 'Stand-up']
    for i in range(n):
        data.append({
            'content_id': f'netflix_content_{i:05d}',
            'title': f'Content Title {i}',
            'type': np.random.choice(types)
        })
    return pd.DataFrame(data)

def generate_netflix_oltp_views(n=300, profile_ids=None, content_ids=None):
    np.random.seed(44)
    if profile_ids is None:
        profile_ids = [f'netflix_profile_{i:06d}' for i in range(50)]
    if content_ids is None:
        content_ids = [f'netflix_content_{i:05d}' for i in range(25)]
    
    data = []
    for i in range(n):
        view_date = datetime.now() - timedelta(days=np.random.randint(0, 30))
        data.append({
            'view_id': f'netflix_view_{i:06d}',
            'profile_id': np.random.choice(profile_ids),
            'content_id': np.random.choice(content_ids),
            'view_date': view_date.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(data)

# Amazon OLTP data generation functions  
def generate_amazon_oltp_customers(n=100):
    np.random.seed(45)
    data = []
    for i in range(n):
        join_date = datetime.now() - timedelta(days=np.random.randint(30, 1095))  # 1 month to 3 years
        data.append({
            'customer_id': f'amazon_customer_{i:06d}',
            'name': f'Customer_{i:04d}',
            'join_date': join_date.strftime('%Y-%m-%d')
        })
    return pd.DataFrame(data)

def generate_amazon_oltp_products(n=200):
    np.random.seed(45)
    data = []
    categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports']
    for i in range(n):
        price = np.random.uniform(10, 500)  # AED
        data.append({
            'product_id': f'amazon_product_{i:06d}',
            'name': f'{np.random.choice(categories)} Product {i}',
            'price': round(price, 2)
        })
    return pd.DataFrame(data)

def generate_amazon_oltp_orders(n=150, customer_ids=None):
    np.random.seed(45)
    if customer_ids is None:
        customer_ids = [f'amazon_customer_{i:06d}' for i in range(50)]
    
    data = []
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    for i in range(n):
        order_date = datetime.now() - timedelta(days=np.random.randint(0, 90))
        data.append({
            'order_id': f'amazon_order_{i:06d}',
            'customer_id': np.random.choice(customer_ids),
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'status': np.random.choice(statuses, p=[0.1, 0.2, 0.3, 0.35, 0.05])
        })
    return pd.DataFrame(data)

def generate_amazon_oltp_order_items(n=300, order_ids=None, product_ids=None):
    np.random.seed(45)
    if order_ids is None:
        order_ids = [f'amazon_order_{i:06d}' for i in range(75)]
    if product_ids is None:
        product_ids = [f'amazon_product_{i:06d}' for i in range(100)]
    
    data = []
    for i in range(n):
        data.append({
            'item_id': f'amazon_item_{i:06d}',
            'order_id': np.random.choice(order_ids),
            'product_id': np.random.choice(product_ids),
            'quantity': np.random.randint(1, 5)
        })
    return pd.DataFrame(data)

def generate_amazon_oltp_shipments(n=120, order_ids=None):
    np.random.seed(45)
    if order_ids is None:
        order_ids = [f'amazon_order_{i:06d}' for i in range(60)]
    
    data = []
    statuses = ['Preparing', 'In Transit', 'Delivered', 'Exception']
    for i in range(n):
        data.append({
            'shipment_id': f'amazon_shipment_{i:06d}',
            'order_id': np.random.choice(order_ids),
            'status': np.random.choice(statuses, p=[0.2, 0.4, 0.35, 0.05]),
            'tracking_number': f'AMZ{i:010d}'
        })
    return pd.DataFrame(data)

# Airbnb OLTP data generation functions
def generate_airbnb_oltp_guests(n=100):
    np.random.seed(46)
    data = []
    for i in range(n):
        member_since = datetime.now() - timedelta(days=np.random.randint(30, 1825))  # 1 month to 5 years
        data.append({
            'guest_id': f'airbnb_guest_{i:06d}',
            'name': f'Guest_{i:04d}',
            'member_since': member_since.strftime('%Y-%m-%d')
        })
    return pd.DataFrame(data)

def generate_airbnb_oltp_hosts(n=50):
    np.random.seed(46)
    data = []
    for i in range(n):
        data.append({
            'host_id': f'airbnb_host_{i:06d}',
            'name': f'Host_{i:04d}',
            'is_superhost': np.random.choice([0, 1], p=[0.8, 0.2])
        })
    return pd.DataFrame(data)

def generate_airbnb_oltp_properties(n=80, host_ids=None):
    np.random.seed(46)
    if host_ids is None:
        host_ids = [f'airbnb_host_{i:06d}' for i in range(25)]
    
    data = []
    cities = ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Ras Al Khaimah']
    property_types = ['Apartment', 'Villa', 'Studio', 'Penthouse']
    
    for i in range(n):
        data.append({
            'property_id': f'airbnb_property_{i:06d}',
            'host_id': np.random.choice(host_ids),
            'title': f'{np.random.choice(property_types)} in {np.random.choice(cities)} {i}',
            'city': np.random.choice(cities)
        })
    return pd.DataFrame(data)

def generate_airbnb_oltp_bookings(n=120, guest_ids=None, property_ids=None):
    np.random.seed(46)
    if guest_ids is None:
        guest_ids = [f'airbnb_guest_{i:06d}' for i in range(50)]
    if property_ids is None:
        property_ids = [f'airbnb_property_{i:06d}' for i in range(40)]
    
    data = []
    for i in range(n):
        checkin_date = datetime.now() + timedelta(days=np.random.randint(-30, 90))
        nights = np.random.randint(1, 14)
        checkout_date = checkin_date + timedelta(days=nights)
        
        data.append({
            'booking_id': f'airbnb_booking_{i:06d}',
            'guest_id': np.random.choice(guest_ids),
            'property_id': np.random.choice(property_ids),
            'checkin_date': checkin_date.strftime('%Y-%m-%d'),
            'checkout_date': checkout_date.strftime('%Y-%m-%d')
        })
    return pd.DataFrame(data)

def generate_airbnb_oltp_reviews(n=80, booking_ids=None):
    np.random.seed(46)
    if booking_ids is None:
        booking_ids = [f'airbnb_booking_{i:06d}' for i in range(60)]
    
    data = []
    comments = ['Great place!', 'Clean and comfortable', 'Perfect location', 'Would stay again', 'Amazing host']
    
    for i in range(n):
        data.append({
            'review_id': f'airbnb_review_{i:06d}',
            'booking_id': np.random.choice(booking_ids),
            'rating': np.random.randint(3, 6),  # 3-5 stars
            'comment': np.random.choice(comments)
        })
    return pd.DataFrame(data)

# NYSE OLTP data generation functions
def generate_nyse_oltp_accounts(n=50):
    np.random.seed(47)
    data = []
    for i in range(n):
        balance = np.random.uniform(1000, 100000)  # USD
        data.append({
            'account_id': f'nyse_account_{i:06d}',
            'name': f'Account Holder {i:04d}',
            'balance': round(balance, 2)
        })
    return pd.DataFrame(data)

def generate_nyse_oltp_orders(n=200, account_ids=None):
    np.random.seed(47)
    if account_ids is None:
        account_ids = [f'nyse_account_{i:06d}' for i in range(25)]
    
    data = []
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM']
    order_types = ['BUY', 'SELL']
    statuses = ['Pending', 'Filled', 'Cancelled', 'Partial']
    
    for i in range(n):
        data.append({
            'order_id': f'nyse_order_{i:06d}',
            'account_id': np.random.choice(account_ids),
            'ticker': np.random.choice(tickers),
            'type': np.random.choice(order_types),
            'quantity': np.random.randint(10, 1000),
            'price': round(np.random.uniform(50, 500), 2),
            'status': np.random.choice(statuses, p=[0.2, 0.6, 0.1, 0.1])
        })
    return pd.DataFrame(data)

def generate_nyse_oltp_transactions(n=150, order_ids=None):
    np.random.seed(47)
    if order_ids is None:
        order_ids = [f'nyse_order_{i:06d}' for i in range(100)]
    
    data = []
    for i in range(n):
        transaction_time = datetime.now() - timedelta(hours=np.random.randint(0, 168))
        data.append({
            'transaction_id': f'nyse_transaction_{i:06d}',
            'order_id': np.random.choice(order_ids),
            'transaction_time': transaction_time.strftime('%Y-%m-%d %H:%M:%S')
        })
    return pd.DataFrame(data)

# Extracted from app.py - populate_module4_data
def populate_module4_data(conn, company_name):
    """Populate Module 4 database with synthetic OLTP data"""
    cursor = conn.cursor()

    # Check if data already exists for this company
    # For OLTP, we check a representative table like users
    table_map = {
        'Uber': 'uber_users',
        'Netflix': 'netflix_users',
        'Amazon': 'amazon_customers',
        'Airbnb': 'airbnb_guests',
        'NYSE': 'nyse_accounts'
    }
    
    table_name = table_map[company_name]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    
    if count > 0:
        print(f"Data already exists for {company_name}. Skipping population.")
        return  # Data already exists for this company

    try:
        cursor.execute("BEGIN")
        
        if company_name == "Uber":
            users = generate_uber_oltp_users(100)
            drivers = generate_uber_oltp_drivers(50)
            rides = generate_uber_oltp_rides(200, users['user_id'].tolist(), drivers['driver_id'].tolist())
            payments = generate_uber_oltp_payments(200, rides['ride_id'].tolist())
            
            users.to_sql('uber_users', conn, if_exists='append', index=False)
            drivers.to_sql('uber_drivers', conn, if_exists='append', index=False)
            rides.to_sql('uber_rides', conn, if_exists='append', index=False)
            payments.to_sql('uber_payments', conn, if_exists='append', index=False)
            
        elif company_name == "Netflix":
            users = generate_netflix_oltp_users(100)
            profiles = generate_netflix_oltp_profiles(150, users['user_id'].tolist())
            subscriptions = generate_netflix_oltp_subscriptions(100, users['user_id'].tolist())
            content = generate_netflix_oltp_content(50)
            views = generate_netflix_oltp_views(300, profiles['profile_id'].tolist(), content['content_id'].tolist())
            
            users.to_sql('netflix_users', conn, if_exists='append', index=False)
            profiles.to_sql('netflix_profiles', conn, if_exists='append', index=False)
            subscriptions.to_sql('netflix_subscriptions', conn, if_exists='append', index=False)
            content.to_sql('netflix_content_catalog', conn, if_exists='append', index=False)
            views.to_sql('netflix_views', conn, if_exists='append', index=False)

        elif company_name == "Amazon":
            customers = generate_amazon_oltp_customers(100)
            products = generate_amazon_oltp_products(200)
            orders = generate_amazon_oltp_orders(150, customers['customer_id'].tolist())
            order_items = generate_amazon_oltp_order_items(300, orders['order_id'].tolist(), products['product_id'].tolist())
            shipments = generate_amazon_oltp_shipments(120, orders['order_id'].tolist())
            
            customers.to_sql('amazon_customers', conn, if_exists='append', index=False)
            products.to_sql('amazon_products', conn, if_exists='append', index=False)
            orders.to_sql('amazon_orders', conn, if_exists='append', index=False)
            order_items.to_sql('amazon_order_items', conn, if_exists='append', index=False)
            shipments.to_sql('amazon_shipments', conn, if_exists='append', index=False)

        elif company_name == "Airbnb":
            guests = generate_airbnb_oltp_guests(100)
            hosts = generate_airbnb_oltp_hosts(50)
            properties = generate_airbnb_oltp_properties(80, hosts['host_id'].tolist())
            bookings = generate_airbnb_oltp_bookings(120, guests['guest_id'].tolist(), properties['property_id'].tolist())
            reviews = generate_airbnb_oltp_reviews(80, bookings['booking_id'].tolist())
            
            guests.to_sql('airbnb_guests', conn, if_exists='append', index=False)
            hosts.to_sql('airbnb_hosts', conn, if_exists='append', index=False)
            properties.to_sql('airbnb_properties', conn, if_exists='append', index=False)
            bookings.to_sql('airbnb_bookings', conn, if_exists='append', index=False)
            reviews.to_sql('airbnb_reviews', conn, if_exists='append', index=False)

        elif company_name == "NYSE":
            accounts = generate_nyse_oltp_accounts(50)
            orders = generate_nyse_oltp_orders(200, accounts['account_id'].tolist())
            transactions = generate_nyse_oltp_transactions(150, orders['order_id'].tolist())
            
            accounts.to_sql('nyse_accounts', conn, if_exists='append', index=False)
            orders.to_sql('nyse_orders', conn, if_exists='append', index=False)
            transactions.to_sql('nyse_transactions', conn, if_exists='append', index=False)
            
        conn.commit()
        print(f"Populated {company_name} OLTP data.")
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        print(f"Error populating Module 4 data for {company_name}: {str(e)}")
        raise e

# Main execution for OLTP module (all companies)
if __name__ == "__main__":
    print("Initializing Module 4 database and populating OLTP data for all companies...")
    conn = init_module4_database()
    
    companies = ["Uber", "Netflix", "Amazon", "Airbnb", "NYSE"]
    
    for company in companies:
        print(f"Populating data for {company}...")
        populate_module4_data(conn, company)
    
    conn.close()
    print("OLTP module data population complete for all companies.")
