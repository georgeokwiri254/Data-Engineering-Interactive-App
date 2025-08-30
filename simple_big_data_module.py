#!/usr/bin/env python3
"""
Simplified Big Data Module - Using Built-in Libraries Only
=========================================================

Creates database schemas and generates basic synthetic data using only
Python built-in libraries (no external dependencies).

Focus on:
- Database schema creation
- Basic synthetic data generation  
- OLTP vs OLAP structure demonstration
- Volume, velocity, variety concepts
"""

import sqlite3
import random
import json
import uuid
from datetime import datetime, timedelta
import math

class SimpleBigDataModule:
    """Simplified Big Data module using built-in libraries only"""
    
    def __init__(self, db_path="big_data_analytics.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # Set random seed for reproducibility
        random.seed(42)
        
        # Configuration data
        self.regions = ['UAE', 'KSA', 'Egypt', 'Qatar', 'Kuwait', 'Bahrain', 'Oman']
        self.countries = ['US', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'India', 'Brazil', 'Australia', 'UAE']
        self.cities = ['Dubai', 'Riyadh', 'Cairo', 'Doha', 'Kuwait City', 'Manama', 'Muscat', 'Abu Dhabi']
        self.nyse_tickers = [
            'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK.B', 'JNJ', 'UNH',
            'XOM', 'JPM', 'V', 'PG', 'HD', 'CVX', 'MA', 'BAC', 'ABBV', 'PFE'
        ]
        
        print(f"✅ Simple Big Data Module initialized: {db_path}")
        self.create_all_schemas()
    
    def create_all_schemas(self):
        """Create all database schemas"""
        print("🔧 Creating database schemas...")
        
        # Amazon E-commerce schemas
        self.create_amazon_schemas()
        
        # Netflix streaming schemas  
        self.create_netflix_schemas()
        
        # Uber ride-hailing schemas
        self.create_uber_schemas()
        
        # Airbnb marketplace schemas
        self.create_airbnb_schemas()
        
        # NYSE market data schemas
        self.create_nyse_schemas()
        
        # OLAP aggregate schemas
        self.create_olap_schemas()
        
        self.conn.commit()
        print("✅ All schemas created successfully")
    
    def create_amazon_schemas(self):
        """Create Amazon OLTP schemas"""
        
        # Customers (OLTP - normalized)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS amazon_customers (
                customer_id TEXT PRIMARY KEY,
                signup_date DATE,
                region TEXT,
                age_band TEXT,
                loyalty_tier TEXT,
                marketing_opt_in BOOLEAN,
                lifetime_value_aed DECIMAL(10,2)
            )
        """)
        
        # Products (OLTP - normalized)  
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS amazon_products (
                product_id TEXT PRIMARY KEY,
                sku TEXT UNIQUE,
                category_lvl1 TEXT,
                category_lvl2 TEXT,
                category_lvl3 TEXT,
                brand TEXT,
                price_aed DECIMAL(8,2),
                stock_qty INTEGER,
                launch_date DATE
            )
        """)
        
        # Orders (OLTP - transactional)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS amazon_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT,
                order_ts TIMESTAMP,
                channel TEXT,
                payment_method TEXT,
                order_status TEXT,
                total_aed DECIMAL(10,2),
                promo_code TEXT,
                FOREIGN KEY (customer_id) REFERENCES amazon_customers(customer_id)
            )
        """)
        
        # Order Items (OLTP - detail records)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS amazon_order_items (
                order_item_id TEXT PRIMARY KEY,
                order_id TEXT,
                product_id TEXT,
                quantity INTEGER,
                unit_price_aed DECIMAL(8,2),
                line_total_aed DECIMAL(10,2),
                FOREIGN KEY (order_id) REFERENCES amazon_orders(order_id),
                FOREIGN KEY (product_id) REFERENCES amazon_products(product_id)
            )
        """)
        
        # Order Events (Streaming/Event Log)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS amazon_order_events (
                event_id TEXT PRIMARY KEY,
                order_id TEXT,
                event_type TEXT,
                event_ts TIMESTAMP,
                channel TEXT,
                risk_score DECIMAL(5,3),
                FOREIGN KEY (order_id) REFERENCES amazon_orders(order_id)
            )
        """)
        
        print("  ✅ Amazon e-commerce schemas created")
    
    def create_netflix_schemas(self):
        """Create Netflix OLTP schemas"""
        
        # Users
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS netflix_users (
                user_id TEXT PRIMARY KEY,
                signup_date DATE,
                country TEXT,
                subscription_plan TEXT,
                billing_status TEXT,
                churn_risk_score DECIMAL(5,3)
            )
        """)
        
        # Content Catalog
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS netflix_content (
                content_id TEXT PRIMARY KEY,
                title TEXT,
                content_type TEXT,
                genre_primary TEXT,
                release_year INTEGER,
                runtime_minutes INTEGER,
                imdb_score DECIMAL(3,1)
            )
        """)
        
        # Viewing Events (High-volume streaming)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS netflix_viewing_events (
                event_id TEXT PRIMARY KEY,
                user_id TEXT,
                content_id TEXT,
                device_type TEXT,
                event_type TEXT,
                timestamp_ms TIMESTAMP,
                watch_duration_sec INTEGER,
                bitrate_kbps INTEGER,
                FOREIGN KEY (user_id) REFERENCES netflix_users(user_id),
                FOREIGN KEY (content_id) REFERENCES netflix_content(content_id)
            )
        """)
        
        print("  ✅ Netflix streaming schemas created")
    
    def create_uber_schemas(self):
        """Create Uber OLTP schemas"""
        
        # Drivers
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS uber_drivers (
                driver_id TEXT PRIMARY KEY,
                onboard_date DATE,
                home_city TEXT,
                vehicle_type TEXT,
                rating_avg DECIMAL(3,2),
                trips_completed INTEGER,
                status TEXT
            )
        """)
        
        # Riders
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS uber_riders (
                rider_id TEXT PRIMARY KEY,
                signup_date DATE,
                home_city TEXT,
                device_os TEXT,
                rating_avg DECIMAL(3,2)
            )
        """)
        
        # Rides (Location + Time sensitive)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS uber_rides (
                ride_id TEXT PRIMARY KEY,
                rider_id TEXT,
                driver_id TEXT,
                request_ts TIMESTAMP,
                pickup_ts TIMESTAMP,
                dropoff_ts TIMESTAMP,
                pickup_lat DECIMAL(10,7),
                pickup_lng DECIMAL(10,7),
                dropoff_lat DECIMAL(10,7),
                dropoff_lng DECIMAL(10,7),
                distance_km DECIMAL(8,3),
                fare_aed DECIMAL(8,2),
                surge_multiplier DECIMAL(4,2),
                ride_status TEXT,
                FOREIGN KEY (rider_id) REFERENCES uber_riders(rider_id),
                FOREIGN KEY (driver_id) REFERENCES uber_drivers(driver_id)
            )
        """)
        
        print("  ✅ Uber ride-hailing schemas created")
    
    def create_airbnb_schemas(self):
        """Create Airbnb OLTP schemas"""
        
        # Hosts
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS airbnb_hosts (
                host_id TEXT PRIMARY KEY,
                host_since DATE,
                superhost_flag BOOLEAN,
                response_rate_pct DECIMAL(5,2),
                cancellation_policy TEXT
            )
        """)
        
        # Properties
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS airbnb_properties (
                property_id TEXT PRIMARY KEY,
                host_id TEXT,
                city TEXT,
                property_type TEXT,
                bedrooms INTEGER,
                max_guests INTEGER,
                base_price_aed DECIMAL(8,2),
                amenities_json TEXT,
                instant_book BOOLEAN,
                FOREIGN KEY (host_id) REFERENCES airbnb_hosts(host_id)
            )
        """)
        
        # Bookings
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS airbnb_bookings (
                booking_id TEXT PRIMARY KEY,
                property_id TEXT,
                checkin_date DATE,
                checkout_date DATE,
                nights INTEGER,
                guests_count INTEGER,
                total_price_aed DECIMAL(10,2),
                booking_status TEXT,
                FOREIGN KEY (property_id) REFERENCES airbnb_properties(property_id)
            )
        """)
        
        print("  ✅ Airbnb marketplace schemas created")
    
    def create_nyse_schemas(self):
        """Create NYSE high-frequency schemas"""
        
        # Trade Ticks (Ultra-high frequency)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nyse_trade_ticks (
                tick_id TEXT PRIMARY KEY,
                ticker TEXT,
                trade_timestamp_ms TIMESTAMP,
                trade_price DECIMAL(12,4),
                trade_size INTEGER,
                trade_side TEXT,
                venue_code TEXT
            )
        """)
        
        # High-dimensional minute features (25+ columns)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nyse_features_minute (
                minute_timestamp TIMESTAMP,
                ticker TEXT,
                -- Price data (5 features)
                open_price DECIMAL(12,4),
                high_price DECIMAL(12,4),
                low_price DECIMAL(12,4),
                close_price DECIMAL(12,4),
                vwap DECIMAL(12,4),
                -- Returns (3 features)
                return_1m DECIMAL(10,6),
                return_5m DECIMAL(10,6),
                return_15m DECIMAL(10,6),
                -- Volume (5 features)
                volume_shares BIGINT,
                volume_notional_usd DECIMAL(15,2),
                trade_count INTEGER,
                buy_volume_ratio DECIMAL(6,4),
                volume_imbalance DECIMAL(8,6),
                -- Volatility (4 features)
                realized_volatility_5m DECIMAL(8,6),
                realized_volatility_15m DECIMAL(8,6),
                momentum_5m DECIMAL(8,6),
                momentum_15m DECIMAL(8,6),
                -- Microstructure (5 features)
                bid_ask_spread_bps DECIMAL(8,4),
                order_flow_imbalance DECIMAL(8,6),
                effective_spread_bps DECIMAL(8,4),
                rsi_14 DECIMAL(6,4),
                -- Prediction targets (3 features)
                return_next_1m DECIMAL(8,6),
                return_next_5m DECIMAL(8,6),
                volatility_next_15m DECIMAL(8,6),
                PRIMARY KEY (minute_timestamp, ticker)
            )
        """)
        
        print("  ✅ NYSE market data schemas created")
    
    def create_olap_schemas(self):
        """Create OLAP aggregate schemas (denormalized for analytics)"""
        
        # Amazon Daily Sales (OLAP - wide, denormalized)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS amazon_sales_daily_agg (
                date_key DATE,
                category_key TEXT,
                region_key TEXT,
                orders_count INTEGER,
                units_sold INTEGER,
                gross_revenue_aed DECIMAL(15,2),
                avg_order_value DECIMAL(10,2),
                unique_customers INTEGER,
                conversion_rate DECIMAL(6,4),
                PRIMARY KEY (date_key, category_key, region_key)
            )
        """)
        
        # Netflix Hourly Engagement (OLAP - time-series aggregates)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS netflix_engagement_hourly_agg (
                date_hour_key TIMESTAMP,
                content_key TEXT,
                country_key TEXT,
                device_key TEXT,
                unique_viewers INTEGER,
                total_watch_hours DECIMAL(12,2),
                completion_rate DECIMAL(6,4),
                avg_bitrate INTEGER,
                session_starts INTEGER,
                PRIMARY KEY (date_hour_key, content_key, country_key, device_key)
            )
        """)
        
        # Uber City Performance (OLAP - geographical aggregates)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS uber_city_hourly_agg (
                date_hour_key TIMESTAMP,
                city_key TEXT,
                total_requests INTEGER,
                completed_rides INTEGER,
                avg_wait_minutes DECIMAL(6,2),
                avg_fare_aed DECIMAL(8,2),
                surge_hours INTEGER,
                driver_utilization_rate DECIMAL(6,4),
                PRIMARY KEY (date_hour_key, city_key)
            )
        """)
        
        print("  ✅ OLAP aggregate schemas created")
    
    def generate_sample_data(self, sample_size="small"):
        """Generate sample synthetic data"""
        print(f"📊 Generating {sample_size} sample dataset...")
        
        if sample_size == "small":
            # Small dataset for demonstration
            self.generate_amazon_sample_data(1000, 100, 500)  # 1K customers, 100 products, 500 orders
            self.generate_netflix_sample_data(500, 50, 2000)   # 500 users, 50 content, 2K events
            self.generate_uber_sample_data(200, 300, 1000)     # 200 drivers, 300 riders, 1K rides
            self.generate_airbnb_sample_data(100, 200, 500)    # 100 hosts, 200 properties, 500 bookings
            self.generate_nyse_sample_data(10, 1000, 500)      # 10 tickers, 1K ticks, 500 features
        
        elif sample_size == "medium":
            # Medium dataset for analysis
            self.generate_amazon_sample_data(10000, 1000, 5000)
            self.generate_netflix_sample_data(5000, 500, 20000)
            self.generate_uber_sample_data(2000, 3000, 10000)
            self.generate_airbnb_sample_data(1000, 2000, 5000)
            self.generate_nyse_sample_data(20, 10000, 5000)
        
        # Generate OLAP aggregates
        self.generate_olap_sample_data()
        
        self.conn.commit()
        print("✅ Sample data generation completed")
    
    def generate_amazon_sample_data(self, n_customers, n_products, n_orders):
        """Generate Amazon sample data"""
        print(f"  🛒 Generating Amazon data: {n_customers} customers, {n_products} products, {n_orders} orders")
        
        # Customers
        customers_data = []
        for i in range(n_customers):
            customer_id = f"CUST_{str(i+1).zfill(6)}"
            signup_date = self.random_date_between(-1095, -30)  # 3 years to 1 month ago
            region = random.choice(self.regions)
            age_band = random.choices(['18-25', '26-35', '36-45', '46-55', '56+'], weights=[20, 35, 25, 15, 5])[0]
            loyalty_tier = random.choices(['Bronze', 'Silver', 'Gold', 'Prime'], weights=[40, 30, 20, 10])[0]
            marketing_opt_in = random.choice([True, False])
            lifetime_value_aed = round(random.uniform(100, 5000), 2)
            
            customers_data.append((customer_id, signup_date, region, age_band, loyalty_tier, marketing_opt_in, lifetime_value_aed))
        
        self.conn.executemany("""
            INSERT INTO amazon_customers VALUES (?, ?, ?, ?, ?, ?, ?)
        """, customers_data)
        
        # Products
        categories_l1 = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports', 'Beauty', 'Automotive']
        products_data = []
        for i in range(n_products):
            product_id = f"PROD_{str(i+1).zfill(7)}"
            sku = f"SKU{random.randint(100000, 999999)}"
            category_l1 = random.choice(categories_l1)
            category_l2 = f"{category_l1}_{random.choice(['A', 'B', 'C'])}"
            category_l3 = f"{category_l2}_{random.randint(1, 5)}"
            brand = f"Brand_{random.randint(1, 50)}"
            price_aed = round(random.uniform(10, 2000), 2)
            stock_qty = random.randint(0, 100)
            launch_date = self.random_date_between(-730, 0)  # 2 years ago to today
            
            products_data.append((product_id, sku, category_l1, category_l2, category_l3, brand, price_aed, stock_qty, launch_date))
        
        self.conn.executemany("""
            INSERT INTO amazon_products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, products_data)
        
        # Orders and Order Items
        customer_ids = [f"CUST_{str(i+1).zfill(6)}" for i in range(n_customers)]
        product_ids = [f"PROD_{str(i+1).zfill(7)}" for i in range(n_products)]
        
        orders_data = []
        order_items_data = []
        
        for i in range(n_orders):
            order_id = f"ORDER_{str(i+1).zfill(8)}"
            customer_id = random.choice(customer_ids)
            order_ts = self.random_datetime_between(-365, 0)  # Last year
            channel = random.choices(['web', 'mobile', 'app'], weights=[45, 35, 20])[0]
            payment_method = random.choices(['credit_card', 'debit_card', 'wallet'], weights=[50, 30, 20])[0]
            order_status = random.choices(['completed', 'cancelled', 'pending'], weights=[85, 10, 5])[0]
            
            # Generate 1-4 items per order
            n_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
            selected_products = random.sample(product_ids, n_items)
            
            total_aed = 0
            for j, product_id in enumerate(selected_products):
                order_item_id = f"{order_id}_ITEM_{j+1}"
                quantity = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
                unit_price_aed = round(random.uniform(10, 500), 2)
                line_total_aed = quantity * unit_price_aed
                total_aed += line_total_aed
                
                order_items_data.append((order_item_id, order_id, product_id, quantity, unit_price_aed, line_total_aed))
            
            promo_code = f"PROMO{random.randint(1, 10)}" if random.random() < 0.2 else None
            orders_data.append((order_id, customer_id, order_ts, channel, payment_method, order_status, total_aed, promo_code))
        
        self.conn.executemany("""
            INSERT INTO amazon_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, orders_data)
        
        self.conn.executemany("""
            INSERT INTO amazon_order_items VALUES (?, ?, ?, ?, ?, ?)
        """, order_items_data)
        
        # Order Events (3-5 events per order)
        order_events_data = []
        event_types = ['created', 'paid', 'shipped', 'delivered', 'cancelled']
        
        for order_data in orders_data[:min(100, len(orders_data))]:  # Limit events to first 100 orders
            order_id = order_data[0]
            base_ts = order_data[2]
            
            n_events = random.randint(3, 5)
            for i in range(n_events):
                event_id = f"EVT_{uuid.uuid4().hex[:8].upper()}"
                event_type = event_types[min(i, len(event_types)-1)]
                event_ts = base_ts + timedelta(hours=i*4 + random.randint(0, 4))
                channel = random.choice(['web', 'mobile', 'system'])
                risk_score = round(random.uniform(0, 1), 3)
                
                order_events_data.append((event_id, order_id, event_type, event_ts, channel, risk_score))
        
        self.conn.executemany("""
            INSERT INTO amazon_order_events VALUES (?, ?, ?, ?, ?, ?)
        """, order_events_data)
    
    def generate_netflix_sample_data(self, n_users, n_content, n_events):
        """Generate Netflix sample data"""
        print(f"  🎬 Generating Netflix data: {n_users} users, {n_content} content, {n_events} events")
        
        # Users
        users_data = []
        for i in range(n_users):
            user_id = f"USER_{str(i+1).zfill(6)}"
            signup_date = self.random_date_between(-1095, -30)
            country = random.choice(self.countries)
            subscription_plan = random.choices(['Basic', 'Standard', 'Premium'], weights=[40, 35, 25])[0]
            billing_status = random.choices(['active', 'past_due', 'cancelled'], weights=[85, 10, 5])[0]
            churn_risk_score = round(random.uniform(0, 1), 3)
            
            users_data.append((user_id, signup_date, country, subscription_plan, billing_status, churn_risk_score))
        
        self.conn.executemany("""
            INSERT INTO netflix_users VALUES (?, ?, ?, ?, ?, ?)
        """, users_data)
        
        # Content
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 'Animation']
        content_data = []
        for i in range(n_content):
            content_id = f"CONTENT_{str(i+1).zfill(5)}"
            title = f"Title_{i+1}"
            content_type = random.choices(['Movie', 'Series', 'Documentary'], weights=[60, 30, 10])[0]
            genre_primary = random.choice(genres)
            release_year = random.randint(2000, 2024)
            runtime_minutes = random.randint(90, 180) if content_type == 'Movie' else random.randint(45, 90)
            imdb_score = round(random.uniform(5.0, 9.5), 1)
            
            content_data.append((content_id, title, content_type, genre_primary, release_year, runtime_minutes, imdb_score))
        
        self.conn.executemany("""
            INSERT INTO netflix_content VALUES (?, ?, ?, ?, ?, ?, ?)
        """, content_data)
        
        # Viewing Events
        user_ids = [f"USER_{str(i+1).zfill(6)}" for i in range(n_users)]
        content_ids = [f"CONTENT_{str(i+1).zfill(5)}" for i in range(n_content)]
        
        viewing_events_data = []
        for i in range(n_events):
            event_id = f"VIEW_{str(i+1).zfill(8)}"
            user_id = random.choice(user_ids)
            content_id = random.choice(content_ids)
            device_type = random.choices(['TV', 'Mobile', 'Desktop', 'Tablet'], weights=[45, 30, 20, 5])[0]
            event_type = random.choices(['play', 'pause', 'stop', 'complete'], weights=[40, 25, 25, 10])[0]
            timestamp_ms = self.random_datetime_between(-180, 0)  # Last 6 months
            watch_duration_sec = random.randint(300, 7200)  # 5 min to 2 hours
            bitrate_kbps = random.choice([480, 720, 1080, 1440])
            
            viewing_events_data.append((event_id, user_id, content_id, device_type, event_type, timestamp_ms, watch_duration_sec, bitrate_kbps))
        
        self.conn.executemany("""
            INSERT INTO netflix_viewing_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, viewing_events_data)
    
    def generate_uber_sample_data(self, n_drivers, n_riders, n_rides):
        """Generate Uber sample data"""
        print(f"  🚗 Generating Uber data: {n_drivers} drivers, {n_riders} riders, {n_rides} rides")
        
        # Drivers
        drivers_data = []
        for i in range(n_drivers):
            driver_id = f"DRV_{str(i+1).zfill(6)}"
            onboard_date = self.random_date_between(-730, -30)
            home_city = random.choice(self.cities)
            vehicle_type = random.choices(['Economy', 'Comfort', 'Premium'], weights=[60, 30, 10])[0]
            rating_avg = round(random.uniform(4.0, 5.0), 2)
            trips_completed = random.randint(50, 2000)
            status = random.choices(['Active', 'Inactive'], weights=[80, 20])[0]
            
            drivers_data.append((driver_id, onboard_date, home_city, vehicle_type, rating_avg, trips_completed, status))
        
        self.conn.executemany("""
            INSERT INTO uber_drivers VALUES (?, ?, ?, ?, ?, ?, ?)
        """, drivers_data)
        
        # Riders
        riders_data = []
        for i in range(n_riders):
            rider_id = f"RDR_{str(i+1).zfill(6)}"
            signup_date = self.random_date_between(-730, -1)
            home_city = random.choice(self.cities)
            device_os = random.choices(['iOS', 'Android'], weights=[45, 55])[0]
            rating_avg = round(random.uniform(4.5, 5.0), 2)
            
            riders_data.append((rider_id, signup_date, home_city, device_os, rating_avg))
        
        self.conn.executemany("""
            INSERT INTO uber_riders VALUES (?, ?, ?, ?, ?)
        """, riders_data)
        
        # Rides
        driver_ids = [f"DRV_{str(i+1).zfill(6)}" for i in range(n_drivers)]
        rider_ids = [f"RDR_{str(i+1).zfill(6)}" for i in range(n_riders)]
        
        rides_data = []
        for i in range(n_rides):
            ride_id = f"RIDE_{str(i+1).zfill(8)}"
            rider_id = random.choice(rider_ids)
            driver_id = random.choice(driver_ids)
            
            request_ts = self.random_datetime_between(-90, 0)  # Last 3 months
            pickup_ts = request_ts + timedelta(minutes=random.randint(2, 15))
            dropoff_ts = pickup_ts + timedelta(minutes=random.randint(5, 60))
            
            # Dubai coordinates as base
            pickup_lat = round(25.2048 + random.uniform(-0.2, 0.2), 7)
            pickup_lng = round(55.2708 + random.uniform(-0.2, 0.2), 7)
            dropoff_lat = round(pickup_lat + random.uniform(-0.1, 0.1), 7)
            dropoff_lng = round(pickup_lng + random.uniform(-0.1, 0.1), 7)
            
            distance_km = round(random.uniform(1.0, 50.0), 3)
            fare_aed = round(5.0 + (distance_km * 2.5) + random.uniform(0, 10), 2)
            surge_multiplier = round(random.choices([1.0, 1.2, 1.5, 2.0], weights=[70, 20, 8, 2])[0], 2)
            ride_status = random.choices(['completed', 'cancelled'], weights=[85, 15])[0]
            
            rides_data.append((ride_id, rider_id, driver_id, request_ts, pickup_ts, dropoff_ts,
                             pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, distance_km, 
                             fare_aed, surge_multiplier, ride_status))
        
        self.conn.executemany("""
            INSERT INTO uber_rides VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rides_data)
    
    def generate_airbnb_sample_data(self, n_hosts, n_properties, n_bookings):
        """Generate Airbnb sample data"""
        print(f"  🏠 Generating Airbnb data: {n_hosts} hosts, {n_properties} properties, {n_bookings} bookings")
        
        # Hosts
        hosts_data = []
        for i in range(n_hosts):
            host_id = f"HOST_{str(i+1).zfill(5)}"
            host_since = self.random_date_between(-1460, -180)  # 4 years to 6 months ago
            superhost_flag = random.choices([True, False], weights=[20, 80])[0]
            response_rate_pct = round(random.uniform(70, 100), 2)
            cancellation_policy = random.choices(['Flexible', 'Moderate', 'Strict'], weights=[40, 40, 20])[0]
            
            hosts_data.append((host_id, host_since, superhost_flag, response_rate_pct, cancellation_policy))
        
        self.conn.executemany("""
            INSERT INTO airbnb_hosts VALUES (?, ?, ?, ?, ?)
        """, hosts_data)
        
        # Properties
        host_ids = [f"HOST_{str(i+1).zfill(5)}" for i in range(n_hosts)]
        property_types = ['Apartment', 'House', 'Villa', 'Studio']
        
        properties_data = []
        for i in range(n_properties):
            property_id = f"PROP_{str(i+1).zfill(6)}"
            host_id = random.choice(host_ids)
            city = random.choice(self.cities)
            property_type = random.choice(property_types)
            bedrooms = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
            max_guests = bedrooms * 2 + random.randint(0, 2)
            base_price_aed = round(random.uniform(100, 1000), 2)
            amenities = random.sample(['WiFi', 'AC', 'Kitchen', 'Parking', 'Pool', 'Gym'], random.randint(3, 6))
            amenities_json = json.dumps(amenities)
            instant_book = random.choice([True, False])
            
            properties_data.append((property_id, host_id, city, property_type, bedrooms, max_guests,
                                  base_price_aed, amenities_json, instant_book))
        
        self.conn.executemany("""
            INSERT INTO airbnb_properties VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, properties_data)
        
        # Bookings
        property_ids = [f"PROP_{str(i+1).zfill(6)}" for i in range(n_properties)]
        
        bookings_data = []
        for i in range(n_bookings):
            booking_id = f"BOOK_{str(i+1).zfill(7)}"
            property_id = random.choice(property_ids)
            checkin_date = self.random_date_between(-180, 30)  # 6 months ago to 1 month future
            nights = random.choices([1, 2, 3, 4, 5, 7, 14], weights=[20, 25, 20, 15, 10, 7, 3])[0]
            checkout_date = checkin_date + timedelta(days=nights)
            guests_count = random.randint(1, 4)
            total_price_aed = round(random.uniform(200, 3000), 2)
            booking_status = random.choices(['confirmed', 'cancelled', 'completed'], weights=[70, 15, 15])[0]
            
            bookings_data.append((booking_id, property_id, checkin_date, checkout_date, nights,
                                guests_count, total_price_aed, booking_status))
        
        self.conn.executemany("""
            INSERT INTO airbnb_bookings VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, bookings_data)
    
    def generate_nyse_sample_data(self, n_tickers, n_ticks, n_features):
        """Generate NYSE sample data"""
        print(f"  📈 Generating NYSE data: {n_tickers} tickers, {n_ticks} ticks, {n_features} features")
        
        # Trade Ticks
        selected_tickers = random.sample(self.nyse_tickers, min(n_tickers, len(self.nyse_tickers)))
        venues = ['NYSE', 'ARCA', 'BATS', 'IEX']
        
        trade_ticks_data = []
        for i in range(n_ticks):
            tick_id = f"TICK_{str(i+1).zfill(8)}"
            ticker = random.choice(selected_tickers)
            trade_timestamp_ms = self.random_datetime_between(-30, 0)  # Last 30 days
            trade_price = round(random.uniform(50, 500), 4)
            trade_size = random.choice([100, 200, 500, 1000, 2000])
            trade_side = random.choice(['buy', 'sell'])
            venue_code = random.choice(venues)
            
            trade_ticks_data.append((tick_id, ticker, trade_timestamp_ms, trade_price, trade_size, trade_side, venue_code))
        
        self.conn.executemany("""
            INSERT INTO nyse_trade_ticks VALUES (?, ?, ?, ?, ?, ?, ?)
        """, trade_ticks_data)
        
        # High-dimensional features
        features_data = []
        for i in range(n_features):
            ticker = random.choice(selected_tickers)
            minute_timestamp = self.random_datetime_between(-30, 0)
            minute_timestamp = minute_timestamp.replace(second=0, microsecond=0)
            
            # Price data (OHLCV)
            base_price = random.uniform(50, 500)
            open_price = round(base_price, 4)
            high_price = round(base_price * random.uniform(1.0, 1.02), 4)
            low_price = round(base_price * random.uniform(0.98, 1.0), 4)
            close_price = round(base_price * random.uniform(0.99, 1.01), 4)
            vwap = round((high_price + low_price + close_price) / 3, 4)
            
            # Returns
            return_1m = round(random.uniform(-0.01, 0.01), 6)
            return_5m = round(return_1m * 2.2 + random.uniform(-0.002, 0.002), 6)
            return_15m = round(return_5m * 3.0 + random.uniform(-0.004, 0.004), 6)
            
            # Volume
            volume_shares = random.randint(10000, 500000)
            volume_notional_usd = round(volume_shares * vwap, 2)
            trade_count = random.randint(50, 500)
            buy_volume_ratio = round(random.uniform(0.3, 0.7), 4)
            volume_imbalance = round(random.uniform(-0.5, 0.5), 6)
            
            # Volatility
            realized_volatility_5m = round(abs(random.uniform(0.01, 0.05)), 6)
            realized_volatility_15m = round(realized_volatility_5m * 1.7, 6)
            momentum_5m = round(random.uniform(-0.02, 0.02), 6)
            momentum_15m = round(momentum_5m * 2.8, 6)
            
            # Microstructure
            bid_ask_spread_bps = round(random.uniform(1, 20), 4)
            order_flow_imbalance = round(random.uniform(-0.3, 0.3), 6)
            effective_spread_bps = round(bid_ask_spread_bps * random.uniform(0.5, 0.9), 4)
            rsi_14 = round(random.uniform(20, 80), 4)
            
            # Prediction targets
            return_next_1m = round(random.uniform(-0.01, 0.01), 6)
            return_next_5m = round(return_next_1m * 2.1 + random.uniform(-0.002, 0.002), 6)
            volatility_next_15m = round(abs(random.uniform(0.01, 0.03)), 6)
            
            features_data.append((minute_timestamp, ticker, open_price, high_price, low_price, close_price, vwap,
                                return_1m, return_5m, return_15m, volume_shares, volume_notional_usd, trade_count,
                                buy_volume_ratio, volume_imbalance, realized_volatility_5m, realized_volatility_15m,
                                momentum_5m, momentum_15m, bid_ask_spread_bps, order_flow_imbalance,
                                effective_spread_bps, rsi_14, return_next_1m, return_next_5m, volatility_next_15m))
        
        self.conn.executemany("""
            INSERT INTO nyse_features_minute VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, features_data)
    
    def generate_olap_sample_data(self):
        """Generate OLAP aggregate sample data"""
        print("  📊 Generating OLAP aggregate data...")
        
        # Amazon daily sales aggregates
        categories = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports']
        regions = ['UAE', 'KSA', 'Egypt', 'Qatar']
        
        amazon_agg_data = []
        used_combinations = set()
        
        attempts = 0
        while len(amazon_agg_data) < 100 and attempts < 500:  # Reduce to 100 records to avoid duplicates
            date_key = self.random_date_between(-365, 0)
            category_key = random.choice(categories)
            region_key = random.choice(regions)
            
            # Check for duplicate combination
            combo_key = (date_key, category_key, region_key)
            if combo_key in used_combinations:
                attempts += 1
                continue
                
            used_combinations.add(combo_key)
            orders_count = random.randint(50, 500)
            units_sold = orders_count * random.randint(1, 3)
            gross_revenue_aed = round(units_sold * random.uniform(50, 200), 2)
            avg_order_value = round(gross_revenue_aed / orders_count, 2)
            unique_customers = random.randint(30, orders_count)
            conversion_rate = round(random.uniform(0.02, 0.08), 4)
            
            amazon_agg_data.append((date_key, category_key, region_key, orders_count, units_sold,
                                  gross_revenue_aed, avg_order_value, unique_customers, conversion_rate))
            attempts += 1
        
        self.conn.executemany("""
            INSERT INTO amazon_sales_daily_agg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, amazon_agg_data)
    
    def random_date_between(self, start_days, end_days):
        """Generate random date between start_days and end_days from today"""
        start_date = datetime.now() + timedelta(days=start_days)
        end_date = datetime.now() + timedelta(days=end_days)
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        return (start_date + timedelta(days=random_days)).date()
    
    def random_datetime_between(self, start_days, end_days):
        """Generate random datetime between start_days and end_days from now"""
        start_date = datetime.now() + timedelta(days=start_days)
        end_date = datetime.now() + timedelta(days=end_days)
        time_between = end_date - start_date
        random_seconds = random.randint(0, int(time_between.total_seconds()))
        return start_date + timedelta(seconds=random_seconds)
    
    def show_data_summary(self):
        """Display summary of generated data"""
        print("\n📊 BIG DATA MODULE - DATA SUMMARY")
        print("=" * 60)
        
        tables = [
            ('amazon_customers', 'Amazon Customers'),
            ('amazon_products', 'Amazon Products'),
            ('amazon_orders', 'Amazon Orders'),
            ('amazon_order_items', 'Amazon Order Items'),
            ('amazon_order_events', 'Amazon Order Events'),
            ('netflix_users', 'Netflix Users'),
            ('netflix_content', 'Netflix Content'),
            ('netflix_viewing_events', 'Netflix Viewing Events'),
            ('uber_drivers', 'Uber Drivers'),
            ('uber_riders', 'Uber Riders'),
            ('uber_rides', 'Uber Rides'),
            ('airbnb_hosts', 'Airbnb Hosts'),
            ('airbnb_properties', 'Airbnb Properties'),
            ('airbnb_bookings', 'Airbnb Bookings'),
            ('nyse_trade_ticks', 'NYSE Trade Ticks'),
            ('nyse_features_minute', 'NYSE Minute Features'),
            ('amazon_sales_daily_agg', 'Amazon Daily Aggregates (OLAP)'),
        ]
        
        total_records = 0
        for table_name, display_name in tables:
            try:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"  {display_name:<35}: {count:>8,} records")
                total_records += count
            except Exception as e:
                print(f"  {display_name:<35}: {'Error':>8}")
        
        print(f"\n  {'Total Records':<35}: {total_records:>8,}")
        
        # Database size
        try:
            cursor = self.conn.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            db_size = cursor.fetchone()[0]
            print(f"  {'Database Size':<35}: {db_size/(1024*1024):>8.2f} MB")
        except:
            print(f"  {'Database Size':<35}: {'Unknown':>8}")
    
    def show_big_data_concepts(self):
        """Display big data concepts demonstration"""
        print("\n🎯 BIG DATA CONCEPTS DEMONSTRATION")
        print("=" * 60)
        
        print("\n📈 VOLUME (Scale)")
        print("  • Amazon: 1K customers × 100 products = 100K potential combinations")
        print("  • Netflix: 500 users × 50 content × 2K events = High interaction volume")
        print("  • NYSE: 10 tickers × 1K ticks = Ultra-high frequency data")
        print("  • Scalable to millions with same structure")
        
        print("\n⚡ VELOCITY (Speed)")
        print("  • Netflix viewing events: Real-time streaming data")
        print("  • NYSE trade ticks: Microsecond precision timestamps") 
        print("  • Uber ride events: GPS location updates every second")
        print("  • Amazon order events: Multi-step transaction lifecycle")
        
        print("\n🌟 VARIETY (Types)")
        print("  • Structured: OLTP tables (normalized, relational)")
        print("  • Semi-structured: JSON fields (amenities, cast, depth levels)")
        print("  • Time-series: NYSE minute features (25+ dimensions)")
        print("  • Geospatial: Uber pickup/dropoff coordinates")
        print("  • Event streams: Order lifecycle, viewing sessions")
        
        print("\n⚖️ OLTP vs OLAP COMPARISON")
        print("  OLTP (Transactional):")
        print("    • Normalized tables (3NF)")
        print("    • Point lookups (customer orders, product details)")
        print("    • High concurrency, low latency")
        print("    • Examples: amazon_customers, uber_rides")
        
        print("  OLAP (Analytical):")
        print("    • Denormalized aggregates (star schema)")
        print("    • Complex queries (GROUP BY, time windows)")
        print("    • Read-optimized, high throughput")
        print("    • Examples: amazon_sales_daily_agg, netflix_engagement_hourly_agg")
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        print(f"📂 Database connection closed: {self.db_path}")

def main():
    """Main function to demonstrate Big Data module"""
    print("🚀 BIG DATA & SCALING MODULE")
    print("Using built-in Python libraries only")
    print("=" * 60)
    
    # Initialize module
    module = SimpleBigDataModule()
    
    # Generate sample data
    module.generate_sample_data("small")
    
    # Show summary
    module.show_data_summary()
    
    # Show concepts
    module.show_big_data_concepts()
    
    # Close
    module.close()
    
    print("\n✅ Big Data Module implementation completed!")
    print("🔧 Database ready for EDA analysis and OLAP/OLTP demonstrations")

if __name__ == "__main__":
    main()