#!/usr/bin/env python3
"""
Synthetic Data Generator for Big Data Module
===========================================

Generates realistic synthetic data for:
- Amazon E-commerce (50K customers, 100K products, 500K orders)
- Netflix Streaming (25K users, 5K content, 2M viewing events)
- Uber Rides (10K drivers, 15K riders, 200K rides)
- Airbnb Marketplace (5K hosts, 10K properties, 50K bookings)
- NYSE Market Data (50 tickers, 100K trade ticks, 10K features)

Focus: Volume, velocity, variety for big data scenarios
"""

import sqlite3
import pandas as pd
import numpy as np
import random
import json
from datetime import datetime, timedelta
from faker import Faker
import uuid
import math

# Set seeds for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

class SyntheticDataGenerator:
    """Generates synthetic data for all companies"""
    
    def __init__(self, db_path="big_data_analytics.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # UAE/MENA focused regions for Amazon
        self.regions = ['UAE', 'KSA', 'Egypt', 'Qatar', 'Kuwait', 'Bahrain', 'Oman']
        self.region_weights = [0.30, 0.25, 0.20, 0.10, 0.06, 0.05, 0.04]
        
        # Global countries for Netflix
        self.countries = ['US', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'India', 'Brazil', 'Australia', 'UAE']
        
        # Cities for Uber and Airbnb
        self.cities = ['Dubai', 'Riyadh', 'Cairo', 'Doha', 'Kuwait City', 'Manama', 'Muscat', 'Abu Dhabi']
        
        # NYSE tickers (50 liquid stocks)
        self.nyse_tickers = [
            'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK.B', 'JNJ', 'UNH',
            'XOM', 'JPM', 'V', 'PG', 'HD', 'CVX', 'MA', 'BAC', 'ABBV', 'PFE',
            'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'DIS', 'ABT', 'ACN', 'MRK', 'NFLX',
            'CRM', 'VZ', 'ADBE', 'DHR', 'CMCSA', 'NKE', 'TXN', 'NEE', 'RTX', 'QCOM',
            'HON', 'AMGN', 'UPS', 'LOW', 'IBM', 'SPGI', 'GE', 'CAT', 'MDT', 'INTU'
        ]
        
        print(f"🔧 Synthetic Data Generator initialized with database: {db_path}")
    
    def generate_all_data(self):
        """Generate synthetic data for all companies"""
        print("🏭 Generating synthetic data for all companies...")
        print("=" * 60)
        
        # Generate Amazon data
        print("🛒 Generating Amazon E-commerce data...")
        self.generate_amazon_data()
        
        # Generate Netflix data  
        print("🎬 Generating Netflix streaming data...")
        self.generate_netflix_data()
        
        # Generate Uber data
        print("🚗 Generating Uber ride data...")
        self.generate_uber_data()
        
        # Generate Airbnb data
        print("🏠 Generating Airbnb marketplace data...")
        self.generate_airbnb_data()
        
        # Generate NYSE data
        print("📈 Generating NYSE market data...")
        self.generate_nyse_data()
        
        # Generate OLAP aggregates
        print("📊 Generating OLAP aggregate data...")
        self.generate_olap_aggregates()
        
        self.conn.commit()
        print("✅ All synthetic data generated successfully!")
    
    def generate_amazon_data(self):
        """Generate Amazon e-commerce synthetic data"""
        
        # Generate customers (50K records)
        customers_data = []
        for i in range(50000):
            customer_id = f"CUST_{str(i+1).zfill(6)}"
            signup_date = fake.date_between(start_date='-3y', end_date='today')
            region = np.random.choice(self.regions, p=self.region_weights)
            age_band = np.random.choice(['18-25', '26-35', '36-45', '46-55', '56+'], 
                                      p=[0.20, 0.35, 0.25, 0.15, 0.05])
            loyalty_tier = np.random.choice(['Bronze', 'Silver', 'Gold', 'Prime'], 
                                          p=[0.40, 0.30, 0.20, 0.10])
            marketing_opt_in = np.random.choice([True, False], p=[0.65, 0.35])
            lifetime_value_aed = np.random.lognormal(mean=6.0, sigma=1.2)
            address_hash = fake.sha256()[:16]
            
            customers_data.append((customer_id, signup_date, region, age_band, 
                                 loyalty_tier, marketing_opt_in, lifetime_value_aed, address_hash))
        
        self.conn.executemany("""
            INSERT INTO amazon_customers VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, customers_data)
        print(f"  ✅ Generated {len(customers_data):,} customers")
        
        # Generate products (100K records)
        categories_l1 = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports', 'Beauty', 'Automotive', 
                        'Toys', 'Health', 'Garden', 'Tools', 'Grocery', 'Baby', 'Pet Supplies', 'Office']
        
        products_data = []
        for i in range(100000):
            product_id = f"PROD_{str(i+1).zfill(7)}"
            sku = f"SKU{fake.random_int(min=100000, max=999999)}"
            category_l1 = np.random.choice(categories_l1)
            category_l2 = f"{category_l1}_{np.random.choice(['A', 'B', 'C', 'D'])}"
            category_l3 = f"{category_l2}_{np.random.randint(1, 10)}"
            brand = fake.company()
            price_aed = np.random.lognormal(mean=4.0, sigma=1.5)
            cost_aed = price_aed * np.random.uniform(0.4, 0.8)
            stock_qty = np.random.poisson(50)
            weight_g = np.random.randint(10, 5000)
            dimensions_cm = f"{np.random.randint(1,50)}x{np.random.randint(1,30)}x{np.random.randint(1,20)}"
            supplier_id = f"SUP_{np.random.randint(1, 1000)}"
            launch_date = fake.date_between(start_date='-2y', end_date='today')
            
            products_data.append((product_id, sku, category_l1, category_l2, category_l3, brand,
                                price_aed, cost_aed, stock_qty, weight_g, dimensions_cm, 
                                supplier_id, launch_date))
        
        self.conn.executemany("""
            INSERT INTO amazon_products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, products_data)
        print(f"  ✅ Generated {len(products_data):,} products")
        
        # Generate orders (500K records)
        customer_ids = [f"CUST_{str(i+1).zfill(6)}" for i in range(50000)]
        product_ids = [f"PROD_{str(i+1).zfill(7)}" for i in range(100000)]
        
        orders_data = []
        order_items_data = []
        
        for i in range(500000):
            order_id = f"ORDER_{str(i+1).zfill(8)}"
            customer_id = np.random.choice(customer_ids)
            
            # Seasonal patterns (higher in Nov-Dec)
            base_date = fake.date_between(start_date='-1y', end_date='today')
            if base_date.month in [11, 12]:  # Black Friday / Holiday season
                order_ts = fake.date_time_between(start_date=base_date, end_date=base_date + timedelta(days=1))
            else:
                order_ts = fake.date_time_between(start_date=base_date, end_date=base_date + timedelta(days=1))
            
            channel = np.random.choice(['web', 'mobile', 'app'], p=[0.45, 0.35, 0.20])
            payment_method = np.random.choice(['credit_card', 'debit_card', 'wallet', 'cod'], 
                                            p=[0.50, 0.25, 0.15, 0.10])
            order_status = np.random.choice(['completed', 'cancelled', 'returned', 'pending'], 
                                          p=[0.80, 0.10, 0.08, 0.02])
            
            # Generate order items (1-6 items per order)
            n_items = np.random.choice([1, 2, 3, 4, 5, 6], p=[0.50, 0.25, 0.15, 0.06, 0.03, 0.01])
            selected_products = np.random.choice(product_ids, size=n_items, replace=False)
            
            total_aed = 0
            for j, product_id in enumerate(selected_products):
                order_item_id = f"{order_id}_ITEM_{j+1}"
                quantity = np.random.choice([1, 2, 3], p=[0.80, 0.15, 0.05])
                unit_price_aed = np.random.lognormal(mean=4.0, sigma=1.5)
                discount_pct = np.random.choice([0, 5, 10, 15, 20, 25], p=[0.60, 0.15, 0.10, 0.08, 0.05, 0.02])
                line_total_aed = quantity * unit_price_aed * (1 - discount_pct/100)
                tax_aed = line_total_aed * 0.05  # 5% VAT
                total_aed += line_total_aed + tax_aed
                
                order_items_data.append((order_item_id, order_id, product_id, quantity, 
                                       unit_price_aed, discount_pct, tax_aed, line_total_aed))
            
            tax_aed = total_aed * 0.05
            shipping_aed = 15.0 if total_aed < 200 else 0  # Free shipping over 200 AED
            promo_code = fake.word().upper() if np.random.random() < 0.15 else None
            warehouse_id = f"WH_{np.random.randint(1, 20)}"
            estimated_delivery = order_ts.date() + timedelta(days=np.random.randint(1, 7))
            
            orders_data.append((order_id, customer_id, order_ts, channel, payment_method, order_status,
                              total_aed, tax_aed, shipping_aed, promo_code, warehouse_id, estimated_delivery))
        
        self.conn.executemany("""
            INSERT INTO amazon_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, orders_data)
        print(f"  ✅ Generated {len(orders_data):,} orders")
        
        self.conn.executemany("""
            INSERT INTO amazon_order_items VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, order_items_data)
        print(f"  ✅ Generated {len(order_items_data):,} order items")
        
        # Generate order events (2M events - 4 events per order average)
        order_events_data = []
        event_types = ['created', 'paid', 'shipped', 'delivered', 'cancelled', 'returned']
        
        for order_data in orders_data[:100000]:  # Limit to 100K orders for events
            order_id = order_data[0]
            base_ts = order_data[2]
            
            # Generate lifecycle events
            n_events = np.random.choice([3, 4, 5, 6], p=[0.20, 0.50, 0.25, 0.05])
            for i in range(n_events):
                event_id = f"EVT_{uuid.uuid4().hex[:8].upper()}"
                event_type = event_types[min(i, len(event_types)-1)]
                actor = np.random.choice(['customer', 'system', 'warehouse'], p=[0.30, 0.60, 0.10])
                event_ts = base_ts + timedelta(hours=i*6 + np.random.randint(0, 6))
                channel = np.random.choice(['web', 'mobile', 'app', 'api'])
                risk_score = np.random.beta(2, 5)  # Most orders low risk
                geo_location = f"{np.random.uniform(24, 26):.4f},{np.random.uniform(54, 56):.4f}"
                session_id = fake.uuid4()
                user_agent = fake.user_agent()
                
                order_events_data.append((event_id, order_id, event_type, actor, event_ts,
                                        channel, risk_score, geo_location, session_id, user_agent))
        
        self.conn.executemany("""
            INSERT INTO amazon_order_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, order_events_data)
        print(f"  ✅ Generated {len(order_events_data):,} order events")
    
    def generate_netflix_data(self):
        """Generate Netflix streaming synthetic data"""
        
        # Generate users (25K records)
        users_data = []
        for i in range(25000):
            user_id = f"USER_{str(i+1).zfill(6)}"
            signup_date = fake.date_between(start_date='-3y', end_date='today')
            country = np.random.choice(self.countries, p=[0.40, 0.10, 0.08, 0.07, 0.06, 0.05, 0.05, 0.04, 0.04, 0.11])
            subscription_plan = np.random.choice(['Basic', 'Standard', 'Premium'], p=[0.40, 0.35, 0.25])
            billing_status = np.random.choice(['active', 'past_due', 'cancelled'], p=[0.85, 0.10, 0.05])
            payment_method = np.random.choice(['credit_card', 'debit_card', 'paypal', 'gift_card'], p=[0.50, 0.25, 0.20, 0.05])
            trial_end_date = signup_date + timedelta(days=30) if np.random.random() < 0.80 else None
            churn_risk_score = np.random.beta(2, 8)  # Most users low churn risk
            
            users_data.append((user_id, signup_date, country, subscription_plan, billing_status, 
                             payment_method, trial_end_date, churn_risk_score))
        
        self.conn.executemany("""
            INSERT INTO netflix_users VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, users_data)
        print(f"  ✅ Generated {len(users_data):,} users")
        
        # Generate profiles (60K records - 2.4 per user average)
        user_ids = [f"USER_{str(i+1).zfill(6)}" for i in range(25000)]
        profiles_data = []
        
        for user_id in user_ids:
            n_profiles = np.random.choice([1, 2, 3, 4, 5], p=[0.20, 0.40, 0.25, 0.10, 0.05])
            for i in range(n_profiles):
                profile_id = f"{user_id}_PROF_{i+1}"
                age_rating = np.random.choice(['Kids', 'Teen', 'Adult'], p=[0.25, 0.25, 0.50])
                language_pref = np.random.choice(['en', 'ar', 'es', 'fr', 'de', 'ja'], p=[0.60, 0.15, 0.10, 0.05, 0.05, 0.05])
                device_types = json.dumps(np.random.choice(['TV', 'Mobile', 'Desktop', 'Tablet'], 
                                                        size=np.random.randint(1, 4), replace=False).tolist())
                viewing_restrictions = 'Parental Control' if age_rating == 'Kids' else 'None'
                profile_type = 'Primary' if i == 0 else 'Secondary'
                
                profiles_data.append((profile_id, user_id, age_rating, language_pref, 
                                    device_types, viewing_restrictions, profile_type))
        
        self.conn.executemany("""
            INSERT INTO netflix_profiles VALUES (?, ?, ?, ?, ?, ?, ?)
        """, profiles_data)
        print(f"  ✅ Generated {len(profiles_data):,} profiles")
        
        # Generate content catalog (5K titles)
        content_data = []
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 
                 'Animation', 'Thriller', 'Crime', 'Fantasy', 'Adventure', 'Mystery', 'War', 'Western']
        
        for i in range(5000):
            content_id = f"CONTENT_{str(i+1).zfill(5)}"
            title = fake.catch_phrase()
            content_type = np.random.choice(['Movie', 'Series', 'Documentary'], p=[0.60, 0.30, 0.10])
            genre_primary = np.random.choice(genres)
            genre_secondary = np.random.choice([g for g in genres if g != genre_primary]) if np.random.random() < 0.70 else None
            release_year = np.random.randint(1990, 2024)
            runtime_minutes = np.random.randint(80, 180) if content_type == 'Movie' else np.random.randint(30, 90)
            maturity_rating = np.random.choice(['G', 'PG', 'PG-13', 'R', 'NC-17'], p=[0.20, 0.25, 0.30, 0.20, 0.05])
            production_country = np.random.choice(['US', 'UK', 'Canada', 'France', 'Germany', 'Japan', 'India'])
            director = fake.name()
            cast_json = json.dumps([fake.name() for _ in range(np.random.randint(3, 8))])
            imdb_score = np.random.normal(7.0, 1.5)
            imdb_score = max(1.0, min(10.0, imdb_score))  # Clamp between 1-10
            awards_count = np.random.poisson(2)
            
            content_data.append((content_id, title, content_type, genre_primary, genre_secondary, 
                               release_year, runtime_minutes, maturity_rating, production_country,
                               director, cast_json, imdb_score, awards_count))
        
        self.conn.executemany("""
            INSERT INTO netflix_content_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, content_data)
        print(f"  ✅ Generated {len(content_data):,} content items")
        
        # Generate viewing events (2M events)
        profile_ids = [row[0] for row in profiles_data]
        content_ids = [f"CONTENT_{str(i+1).zfill(5)}" for i in range(5000)]
        
        viewing_events_data = []
        for i in range(2000000):
            event_id = f"VIEW_EVT_{str(i+1).zfill(8)}"
            profile_id = np.random.choice(profile_ids)
            content_id = np.random.choice(content_ids)
            device_type = np.random.choice(['TV', 'Mobile', 'Desktop', 'Tablet'], p=[0.45, 0.30, 0.20, 0.05])
            event_type = np.random.choice(['play', 'pause', 'stop', 'seek', 'complete'], p=[0.40, 0.20, 0.15, 0.15, 0.10])
            
            # Peak viewing hours: 7-11 PM
            hour_weights = [0.02] * 6 + [0.03] * 3 + [0.04] * 8 + [0.15] * 4 + [0.08] * 3  # 24 hours
            viewing_hour = np.random.choice(range(24), p=hour_weights)
            
            timestamp_ms = fake.date_time_between(start_date='-6m', end_date='today').replace(hour=viewing_hour)
            watch_duration_sec = np.random.exponential(1800)  # Average 30 min
            bitrate_kbps = np.random.choice([480, 720, 1080, 1440, 2160], p=[0.10, 0.30, 0.40, 0.15, 0.05])
            buffer_events = np.random.poisson(2) if np.random.random() < 0.20 else 0
            cdn_pop = f"CDN_{np.random.choice(['US-East', 'US-West', 'EU-West', 'APAC', 'ME'])}"
            app_version = f"v{np.random.randint(8, 12)}.{np.random.randint(0, 5)}.{np.random.randint(0, 10)}"
            seek_events = np.random.poisson(3) if event_type == 'seek' else 0
            subtitle_lang = np.random.choice(['en', 'ar', 'es', 'fr', None], p=[0.40, 0.20, 0.10, 0.10, 0.20])
            
            viewing_events_data.append((event_id, profile_id, content_id, device_type, event_type,
                                      timestamp_ms, watch_duration_sec, bitrate_kbps, buffer_events,
                                      cdn_pop, app_version, seek_events, subtitle_lang))
        
        self.conn.executemany("""
            INSERT INTO netflix_viewing_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, viewing_events_data)
        print(f"  ✅ Generated {len(viewing_events_data):,} viewing events")
    
    def generate_uber_data(self):
        """Generate Uber ride-hailing synthetic data"""
        
        # Generate drivers (10K records)
        drivers_data = []
        for i in range(10000):
            driver_id = f"DRV_{str(i+1).zfill(6)}"
            onboard_date = fake.date_between(start_date='-2y', end_date='-30d')
            home_city = np.random.choice(self.cities)
            license_expiry = fake.date_between(start_date='today', end_date='+3y')
            vehicle_type = np.random.choice(['Economy', 'Comfort', 'Premium', 'SUV'], p=[0.60, 0.25, 0.10, 0.05])
            vehicle_year = np.random.randint(2015, 2024)
            rating_avg = np.random.normal(4.3, 0.4)
            rating_avg = max(1.0, min(5.0, rating_avg))
            trips_completed = np.random.poisson(500)
            acceptance_rate = np.random.normal(85.0, 15.0)
            acceptance_rate = max(0.0, min(100.0, acceptance_rate))
            cancellation_rate = np.random.exponential(5.0)
            cancellation_rate = min(30.0, cancellation_rate)
            earnings_ytd_aed = np.random.lognormal(mean=9.0, sigma=0.8)
            status = np.random.choice(['Active', 'Inactive', 'Suspended'], p=[0.80, 0.15, 0.05])
            
            drivers_data.append((driver_id, onboard_date, home_city, license_expiry, vehicle_type, 
                               vehicle_year, rating_avg, trips_completed, acceptance_rate, 
                               cancellation_rate, earnings_ytd_aed, status))
        
        self.conn.executemany("""
            INSERT INTO uber_drivers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, drivers_data)
        print(f"  ✅ Generated {len(drivers_data):,} drivers")
        
        # Generate riders (15K records)
        riders_data = []
        for i in range(15000):
            rider_id = f"RDR_{str(i+1).zfill(6)}"
            signup_date = fake.date_between(start_date='-2y', end_date='today')
            home_city = np.random.choice(self.cities)
            device_os = np.random.choice(['iOS', 'Android'], p=[0.45, 0.55])
            wallet_balance_aed = np.random.exponential(100.0)
            rating_avg = np.random.normal(4.7, 0.3)
            rating_avg = max(1.0, min(5.0, rating_avg))
            
            riders_data.append((rider_id, signup_date, home_city, device_os, wallet_balance_aed, rating_avg))
        
        self.conn.executemany("""
            INSERT INTO uber_riders VALUES (?, ?, ?, ?, ?, ?)
        """, riders_data)
        print(f"  ✅ Generated {len(riders_data):,} riders")
        
        # Generate rides (200K records)
        driver_ids = [f"DRV_{str(i+1).zfill(6)}" for i in range(10000)]
        rider_ids = [f"RDR_{str(i+1).zfill(6)}" for i in range(15000)]
        
        rides_data = []
        for i in range(200000):
            ride_id = f"RIDE_{str(i+1).zfill(8)}"
            rider_id = np.random.choice(rider_ids)
            driver_id = np.random.choice(driver_ids)
            
            # Peak hours: 7-9 AM, 5-8 PM
            hour_weights = [0.02] * 7 + [0.12, 0.15, 0.08] + [0.04] * 7 + [0.10, 0.12, 0.15, 0.08] + [0.04] * 4
            ride_hour = np.random.choice(range(24), p=hour_weights)
            
            request_ts = fake.date_time_between(start_date='-3m', end_date='today').replace(hour=ride_hour)
            accept_ts = request_ts + timedelta(seconds=np.random.exponential(120))
            pickup_ts = accept_ts + timedelta(seconds=np.random.exponential(300))
            
            # Dubai coordinates as base
            pickup_lat = 25.2048 + np.random.normal(0, 0.1)
            pickup_lng = 55.2708 + np.random.normal(0, 0.1)
            
            # Distance and direction
            distance_km = np.random.exponential(8.0)
            angle = np.random.uniform(0, 2 * math.pi)
            
            dropoff_lat = pickup_lat + (distance_km / 111.0) * math.cos(angle)
            dropoff_lng = pickup_lng + (distance_km / (111.0 * math.cos(math.radians(pickup_lat)))) * math.sin(angle)
            
            duration_sec = distance_km * 180 + np.random.exponential(300)  # ~3 min per km + traffic
            dropoff_ts = pickup_ts + timedelta(seconds=duration_sec)
            
            # Pricing
            base_fare = 5.0  # Base fare in AED
            fare_base_aed = base_fare + (distance_km * 2.5) + (duration_sec / 60 * 0.5)
            
            # Surge pricing during peak hours
            surge_multiplier = 1.0
            if ride_hour in [7, 8, 17, 18, 19]:
                surge_multiplier = np.random.choice([1.0, 1.2, 1.5, 2.0], p=[0.50, 0.30, 0.15, 0.05])
            
            fare_base_aed *= surge_multiplier
            
            tips_aed = np.random.exponential(5.0) if np.random.random() < 0.25 else 0
            tolls_aed = np.random.choice([0, 4, 8], p=[0.70, 0.20, 0.10])  # Salik toll
            final_fare_aed = fare_base_aed + tips_aed + tolls_aed
            
            rating_rider = np.random.choice([3, 4, 5], p=[0.05, 0.25, 0.70]) if np.random.random() < 0.80 else None
            rating_driver = np.random.choice([3, 4, 5], p=[0.05, 0.20, 0.75]) if np.random.random() < 0.85 else None
            
            ride_status = np.random.choice(['completed', 'cancelled_rider', 'cancelled_driver', 'no_show'], 
                                         p=[0.85, 0.08, 0.05, 0.02])
            
            rides_data.append((ride_id, rider_id, driver_id, request_ts, accept_ts, pickup_ts, dropoff_ts,
                             pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, distance_km, duration_sec,
                             fare_base_aed, surge_multiplier, tips_aed, tolls_aed, final_fare_aed,
                             rating_rider, rating_driver, ride_status))
        
        self.conn.executemany("""
            INSERT INTO uber_rides VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rides_data)
        print(f"  ✅ Generated {len(rides_data):,} rides")
    
    def generate_airbnb_data(self):
        """Generate Airbnb marketplace synthetic data"""
        
        # Generate hosts (5K records)
        hosts_data = []
        for i in range(5000):
            host_id = f"HOST_{str(i+1).zfill(5)}"
            host_since = fake.date_between(start_date='-5y', end_date='-6m')
            superhost_flag = np.random.choice([True, False], p=[0.20, 0.80])
            response_time_hours = np.random.exponential(2.0) if superhost_flag else np.random.exponential(8.0)
            response_rate_pct = np.random.normal(95.0, 10.0) if superhost_flag else np.random.normal(85.0, 20.0)
            response_rate_pct = max(0.0, min(100.0, response_rate_pct))
            cancellation_policy = np.random.choice(['Flexible', 'Moderate', 'Strict', 'Super Strict'], 
                                                 p=[0.40, 0.35, 0.20, 0.05])
            
            hosts_data.append((host_id, host_since, superhost_flag, response_time_hours, 
                             response_rate_pct, cancellation_policy))
        
        self.conn.executemany("""
            INSERT INTO airbnb_hosts VALUES (?, ?, ?, ?, ?, ?)
        """, hosts_data)
        print(f"  ✅ Generated {len(hosts_data):,} hosts")
        
        # Generate guests (15K records)
        guests_data = []
        for i in range(15000):
            guest_id = f"GUEST_{str(i+1).zfill(6)}"
            signup_date = fake.date_between(start_date='-3y', end_date='today')
            country = np.random.choice(self.countries, p=[0.25, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05, 0.05, 0.05, 0.15])
            n_prior_bookings = np.random.poisson(3)
            avg_review_score = np.random.normal(4.5, 0.5)
            avg_review_score = max(1.0, min(5.0, avg_review_score))
            cancel_history_cnt = np.random.poisson(0.5)
            
            guests_data.append((guest_id, signup_date, country, n_prior_bookings, 
                              avg_review_score, cancel_history_cnt))
        
        self.conn.executemany("""
            INSERT INTO airbnb_guests VALUES (?, ?, ?, ?, ?, ?)
        """, guests_data)
        print(f"  ✅ Generated {len(guests_data):,} guests")
        
        # Generate properties (10K records)
        host_ids = [f"HOST_{str(i+1).zfill(5)}" for i in range(5000)]
        property_types = ['Apartment', 'House', 'Townhouse', 'Villa', 'Studio', 'Loft', 'Condo']
        room_types = ['Entire place', 'Private room', 'Shared room', 'Hotel room']
        
        properties_data = []
        for i in range(10000):
            property_id = f"PROP_{str(i+1).zfill(6)}"
            host_id = np.random.choice(host_ids)
            city = np.random.choice(self.cities)
            neighborhood = f"{city}_{np.random.choice(['Downtown', 'Marina', 'Beach', 'Airport', 'Mall', 'Business'])}"
            property_type = np.random.choice(property_types, p=[0.40, 0.20, 0.15, 0.10, 0.08, 0.05, 0.02])
            room_type = np.random.choice(room_types, p=[0.70, 0.25, 0.03, 0.02])
            bedrooms = np.random.choice([0, 1, 2, 3, 4, 5], p=[0.15, 0.35, 0.30, 0.15, 0.04, 0.01])
            bathrooms = bedrooms * 0.8 + np.random.choice([0, 0.5, 1.0], p=[0.10, 0.20, 0.70])
            max_guests = max(1, bedrooms * 2 + np.random.randint(0, 3))
            
            # Amenities
            all_amenities = ['WiFi', 'AC', 'Kitchen', 'Parking', 'Pool', 'Gym', 'Balcony', 'Sea View', 'Pet Friendly']
            n_amenities = np.random.randint(3, 8)
            amenities = np.random.choice(all_amenities, size=n_amenities, replace=False).tolist()
            amenities_json = json.dumps(amenities)
            
            # Pricing (higher for Dubai, lower for other cities)
            base_multiplier = 1.5 if city == 'Dubai' else 1.0
            base_price_aed = base_multiplier * (50 + bedrooms * 100 + len(amenities) * 20 + np.random.exponential(100))
            cleaning_fee_aed = base_price_aed * 0.15 + np.random.uniform(20, 100)
            security_deposit_aed = base_price_aed * 2.0 if np.random.random() < 0.60 else 0
            
            minimum_nights = np.random.choice([1, 2, 3, 7, 30], p=[0.50, 0.25, 0.15, 0.08, 0.02])
            maximum_nights = np.random.choice([30, 90, 365, 1000], p=[0.30, 0.40, 0.25, 0.05])
            instant_book = np.random.choice([True, False], p=[0.35, 0.65])
            cancellation_policy = np.random.choice(['Flexible', 'Moderate', 'Strict'], p=[0.40, 0.40, 0.20])
            
            listing_date = fake.date_between(start_date='-2y', end_date='-1m')
            last_updated = fake.date_time_between(start_date=listing_date, end_date='today')
            
            properties_data.append((property_id, host_id, city, neighborhood, property_type, room_type,
                                  bedrooms, bathrooms, max_guests, amenities_json, base_price_aed,
                                  cleaning_fee_aed, security_deposit_aed, minimum_nights, maximum_nights,
                                  instant_book, cancellation_policy, listing_date, last_updated))
        
        self.conn.executemany("""
            INSERT INTO airbnb_properties VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, properties_data)
        print(f"  ✅ Generated {len(properties_data):,} properties")
        
        # Generate bookings (50K records)
        guest_ids = [f"GUEST_{str(i+1).zfill(6)}" for i in range(15000)]
        property_ids = [f"PROP_{str(i+1).zfill(6)}" for i in range(10000)]
        
        bookings_data = []
        for i in range(50000):
            booking_id = f"BOOK_{str(i+1).zfill(7)}"
            guest_id = np.random.choice(guest_ids)
            property_id = np.random.choice(property_ids)
            
            # Seasonal booking patterns
            checkin_date = fake.date_between(start_date='-1y', end_date='+3m')
            
            # Length of stay
            nights = np.random.choice([1, 2, 3, 4, 5, 6, 7, 14, 30], 
                                    p=[0.20, 0.25, 0.20, 0.15, 0.08, 0.05, 0.03, 0.03, 0.01])
            checkout_date = checkin_date + timedelta(days=nights)
            
            guests_count = np.random.randint(1, 5)
            booking_status = np.random.choice(['confirmed', 'cancelled', 'pending', 'completed'], 
                                            p=[0.70, 0.15, 0.05, 0.10])
            
            # Get base price (simplified - using average)
            base_price_per_night = np.random.lognormal(mean=5.0, sigma=0.8)
            
            # Dynamic pricing adjustments
            if checkin_date.month in [11, 12, 1, 2]:  # High season
                base_price_per_night *= 1.3
            elif checkin_date.weekday() >= 4:  # Weekend
                base_price_per_night *= 1.2
            
            total_price_aed = base_price_per_night * nights
            host_fee_aed = total_price_aed * 0.03  # 3% host fee
            service_fee_aed = total_price_aed * 0.12  # 12% Airbnb fee
            taxes_aed = total_price_aed * 0.10  # 10% tourism tax
            
            applied_discounts_aed = 0
            if nights >= 7:  # Weekly discount
                applied_discounts_aed = total_price_aed * 0.10
            elif nights >= 28:  # Monthly discount  
                applied_discounts_aed = total_price_aed * 0.20
            
            total_price_aed = total_price_aed - applied_discounts_aed + host_fee_aed + service_fee_aed + taxes_aed
            
            booking_channel = np.random.choice(['web', 'mobile', 'app', 'partner'], p=[0.45, 0.35, 0.15, 0.05])
            special_requests_text = fake.text(max_nb_chars=200) if np.random.random() < 0.30 else None
            
            bookings_data.append((booking_id, guest_id, property_id, checkin_date, checkout_date, nights,
                                guests_count, booking_status, total_price_aed, host_fee_aed, service_fee_aed,
                                taxes_aed, applied_discounts_aed, booking_channel, special_requests_text))
        
        self.conn.executemany("""
            INSERT INTO airbnb_bookings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, bookings_data)
        print(f"  ✅ Generated {len(bookings_data):,} bookings")
    
    def generate_nyse_data(self):
        """Generate NYSE market data with high dimensionality"""
        
        # Generate trade ticks (100K records for 50 tickers)
        trade_ticks_data = []
        venues = ['NYSE', 'ARCA', 'BATS', 'EDGX', 'IEX', 'NASDAQ']
        
        for i in range(100000):
            tick_id = f"TICK_{str(i+1).zfill(8)}"
            ticker = np.random.choice(self.nyse_tickers)
            
            # Trading hours: 9:30 AM - 4:00 PM EST
            trading_day = fake.date_between(start_date='-30d', end_date='today')
            trading_hour = np.random.randint(9, 16)
            trading_minute = np.random.randint(0, 60) if trading_hour != 9 else np.random.randint(30, 60)
            trading_second = np.random.randint(0, 60)
            trading_ms = np.random.randint(0, 1000)
            
            trade_timestamp_ms = trading_day.replace(hour=trading_hour, minute=trading_minute, 
                                                   second=trading_second, microsecond=trading_ms*1000)
            
            # Stock price simulation (simplified random walk)
            base_price = np.random.uniform(50, 500)  # Base stock price
            trade_price = base_price * (1 + np.random.normal(0, 0.02))
            
            trade_size = np.random.choice([100, 200, 500, 1000, 2000, 5000], 
                                        p=[0.40, 0.25, 0.15, 0.10, 0.06, 0.04])
            trade_side = np.random.choice(['buy', 'sell'], p=[0.52, 0.48])  # Slight buy bias
            venue_code = np.random.choice(venues)
            liquidity_flag = np.random.choice(['add', 'remove'], p=[0.45, 0.55])
            trade_condition = np.random.choice(['normal', 'opening', 'closing', 'odd_lot'], p=[0.85, 0.05, 0.05, 0.05])
            order_id = f"ORD_{fake.uuid4().hex[:8].upper()}"
            execution_latency_microsec = np.random.exponential(50)
            
            trade_ticks_data.append((tick_id, ticker, trade_timestamp_ms, trade_price, trade_size,
                                   trade_side, venue_code, liquidity_flag, trade_condition, 
                                   order_id, execution_latency_microsec))
        
        self.conn.executemany("""
            INSERT INTO nyse_trade_ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, trade_ticks_data)
        print(f"  ✅ Generated {len(trade_ticks_data):,} trade ticks")
        
        # Generate minute-level features (10K records)
        features_data = []
        for ticker in self.nyse_tickers[:10]:  # Limit to 10 tickers for features
            for i in range(1000):  # 1000 minutes per ticker
                base_time = fake.date_time_between(start_date='-30d', end_date='today')
                minute_timestamp = base_time.replace(second=0, microsecond=0)
                
                # Price data (OHLCV)
                base_price = np.random.uniform(50, 500)
                open_price = base_price
                high_price = open_price * (1 + abs(np.random.normal(0, 0.01)))
                low_price = open_price * (1 - abs(np.random.normal(0, 0.01)))
                close_price = open_price * (1 + np.random.normal(0, 0.005))
                vwap = (high_price + low_price + close_price) / 3
                
                # Returns
                return_1m = np.random.normal(0, 0.001)
                return_5m = return_1m * 2.2 + np.random.normal(0, 0.002)
                return_15m = return_5m * 3.0 + np.random.normal(0, 0.004)
                
                # Volume data
                volume_shares = np.random.poisson(50000)
                volume_notional_usd = volume_shares * vwap
                trade_count = np.random.poisson(100)
                avg_trade_size = volume_shares / max(1, trade_count)
                
                # Market microstructure features
                volume_imbalance = np.random.normal(0, 0.2)
                signed_volume = volume_shares * np.random.choice([-1, 1], p=[0.48, 0.52])
                buy_volume_ratio = np.random.beta(5, 5)  # Centered around 0.5
                bid_ask_spread_bps = np.random.exponential(5)
                
                # Volatility and momentum
                realized_volatility_5m = abs(np.random.normal(0.02, 0.01))
                realized_volatility_15m = realized_volatility_5m * 1.7
                momentum_5m = np.random.normal(0, 0.01)
                momentum_15m = momentum_5m * 2.8
                rsi_14 = np.random.beta(2, 2) * 100  # RSI between 0-100
                
                # Advanced microstructure
                order_flow_imbalance = np.random.normal(0, 0.3)
                effective_spread_bps = bid_ask_spread_bps * np.random.uniform(0.3, 0.8)
                price_improvement_bps = effective_spread_bps * np.random.uniform(0.1, 0.4)
                
                # Prediction targets
                return_next_1m = np.random.normal(0, 0.001)
                return_next_5m = return_next_1m * 2.1 + np.random.normal(0, 0.002)
                volatility_next_15m = abs(np.random.normal(0.02, 0.005))
                
                features_data.append((minute_timestamp, ticker, open_price, high_price, low_price, close_price,
                                    vwap, return_1m, return_5m, return_15m, volume_shares, volume_notional_usd,
                                    trade_count, avg_trade_size, volume_imbalance, signed_volume, buy_volume_ratio,
                                    bid_ask_spread_bps, realized_volatility_5m, realized_volatility_15m,
                                    momentum_5m, momentum_15m, rsi_14, order_flow_imbalance, effective_spread_bps,
                                    price_improvement_bps, return_next_1m, return_next_5m, volatility_next_15m))
        
        self.conn.executemany("""
            INSERT INTO nyse_features_minute VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, features_data)
        print(f"  ✅ Generated {len(features_data):,} minute-level features")
    
    def generate_olap_aggregates(self):
        """Generate OLAP aggregate data"""
        print("📊 Generating OLAP aggregates...")
        
        # Amazon daily sales aggregates (sample data)
        amazon_agg_data = []
        categories = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports']
        regions = ['UAE', 'KSA', 'Egypt', 'Qatar']
        
        for i in range(1000):
            date_key = fake.date_between(start_date='-1y', end_date='today')
            category_key = np.random.choice(categories)
            region_key = np.random.choice(regions)
            orders_count = np.random.poisson(100)
            units_sold = orders_count * np.random.poisson(2)
            gross_revenue_aed = units_sold * np.random.lognormal(mean=4.0, sigma=1.0)
            discounts_aed = gross_revenue_aed * np.random.uniform(0.05, 0.20)
            returns_aed = gross_revenue_aed * np.random.uniform(0.02, 0.08)
            avg_order_value = gross_revenue_aed / max(1, orders_count)
            conversion_rate = np.random.uniform(0.02, 0.08)
            customer_acquisition_cost = np.random.uniform(20, 100)
            
            amazon_agg_data.append((date_key, category_key, region_key, orders_count, units_sold,
                                  gross_revenue_aed, discounts_aed, returns_aed, avg_order_value,
                                  conversion_rate, customer_acquisition_cost))
        
        self.conn.executemany("""
            INSERT INTO amazon_daily_sales_agg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, amazon_agg_data)
        print(f"  ✅ Generated {len(amazon_agg_data):,} Amazon daily aggregates")
    
    def get_data_summary(self):
        """Get summary of generated data"""
        print("\n📊 DATA GENERATION SUMMARY")
        print("=" * 60)
        
        tables = [
            'amazon_customers', 'amazon_products', 'amazon_orders', 'amazon_order_items', 'amazon_order_events',
            'netflix_users', 'netflix_profiles', 'netflix_content_catalog', 'netflix_viewing_events',
            'uber_drivers', 'uber_riders', 'uber_rides',
            'airbnb_hosts', 'airbnb_guests', 'airbnb_properties', 'airbnb_bookings',
            'nyse_trade_ticks', 'nyse_features_minute',
            'amazon_daily_sales_agg'
        ]
        
        for table in tables:
            try:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:<35}: {count:>10,} records")
            except sqlite3.OperationalError:
                print(f"  {table:<35}: {'Table not found':>15}")
        
        # Database size
        cursor = self.conn.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
        db_size = cursor.fetchone()[0]
        print(f"\n📂 Database size: {db_size / (1024*1024):.2f} MB")
        
        self.conn.close()

def main():
    """Main function to generate all synthetic data"""
    print("🚀 Big Data Synthetic Data Generator")
    print("=" * 60)
    
    generator = SyntheticDataGenerator()
    generator.generate_all_data()
    generator.get_data_summary()
    
    print("\n✅ Synthetic data generation completed!")
    print("🔧 Ready for EDA analysis and visualization")

if __name__ == "__main__":
    main()