# Data Architecture Engineering: Big Data & Scaling with Synthetic Datasets

## Table of Contents
- [Module 1: Big Data Fundamentals & Use Cases](#module-1-big-data-fundamentals--use-cases)
- [Module 2: Company-Specific Datasets](#module-2-company-specific-datasets)
- [Module 3: Data Science Projects & ML Techniques](#module-3-data-science-projects--ml-techniques)
- [Module 4: OLTP vs OLAP Systems](#module-4-oltp-vs-olap-systems)
- [Module 5: NYSE High-Dimensional Analytics](#module-5-nyse-high-dimensional-analytics)

---

## Module 1: Big Data Fundamentals & Use Cases

### 1.1 Big Data Characteristics & Scaling Scenarios

#### Volume, Velocity & Variety Challenges

| **Company** | **Volume Challenge** | **Velocity Challenge** | **Variety Challenge** |
|-------------|---------------------|------------------------|----------------------|
| **Amazon** | 12B+ orders annually | 500K+ orders/hour peak | Product catalogs, reviews, logistics |
| **Netflix** | 1B+ hours watched daily | 10M+ concurrent streams | Video metadata, user behavior, CDN metrics |
| **Uber** | 20M+ rides daily | 50K+ ride requests/minute | GPS coordinates, payments, driver data |
| **Airbnb** | 4M+ hosts, 1B+ guest arrivals | Booking surges during events | Property amenities, reviews, pricing |
| **NYSE** | 5B+ messages daily | 100K+ messages/second | Trade data, order books, market events |

#### Scaling Patterns & Solutions

| **Scaling Pattern** | **Use Case** | **Implementation** | **Benefits** |
|--------------------|--------------|-------------------|-------------|
| **Horizontal Partitioning** | Amazon orders by region | Shard key: `customer_region` | Reduced query latency |
| **Time-based Partitioning** | Netflix viewing events | Partition: `date(event_timestamp)` | Efficient data pruning |
| **Streaming Ingestion** | Uber ride events | Apache Kafka + Spark Streaming | Real-time analytics |
| **Columnar Storage** | NYSE tick data | Parquet with compression | 10x query performance |
| **Caching Layers** | Airbnb search results | Redis with 15-min TTL | Sub-second response times |

---

## Module 2: Company-Specific Datasets

### 2.1 Amazon E-Commerce Platform

#### 2.1.1 OLTP Schema (Transactional)

**Entity: Customers**
```
Table: customers
Columns: customer_id (PK), signup_date, region, age_band, loyalty_tier, marketing_opt_in, lifetime_value_aed, address_hash
Sample Size: 50M records
Partitioning: Hash on customer_id (16 partitions)
```

**Entity: Products**
```
Table: products
Columns: product_id (PK), sku, category_lvl1, category_lvl2, category_lvl3, brand, price_aed, cost_aed, stock_qty, weight_g, dimensions_cm, supplier_id, launch_date
Sample Size: 500M records
Partitioning: Range on category_lvl1
Indexing: category_lvl1, brand, price_aed
```

**Entity: Orders**
```
Table: orders
Columns: order_id (PK), customer_id (FK), order_ts, channel, payment_method, order_status, total_aed, tax_aed, shipping_aed, promo_code, warehouse_id, estimated_delivery
Sample Size: 2B records annually
Partitioning: Time-based (daily)
```

#### 2.1.2 Event Streaming (Real-time)

**Stream: Order Events**
```
Topic: order-lifecycle-events
Schema: event_id, order_id, event_type, actor, event_ts, channel, risk_score, geo_location, session_id, user_agent
Throughput: 50K events/second
Retention: 30 days
```

#### 2.1.3 OLAP Schema (Analytics)

**Fact: Daily Sales Aggregates**
```
Table: fact_daily_sales
Columns: date_key, category_key, region_key, orders_count, units_sold, gross_revenue_aed, discounts_aed, returns_aed, avg_order_value, conversion_rate, customer_acquisition_cost
Grain: Daily × Category × Region
Size: 365 × 50 × 12 = 219K rows/year
```

### 2.2 Netflix Streaming Platform

#### 2.2.1 OLTP Schema (Transactional)

**Entity: Users & Profiles**
```
Table: users
Columns: user_id (PK), signup_date, country, subscription_plan, billing_status, payment_method, trial_end_date, churn_risk_score
Sample Size: 250M records

Table: profiles
Columns: profile_id (PK), user_id (FK), age_rating, language_pref, device_types, viewing_restrictions, profile_type
Sample Size: 600M records (2.4 profiles/user avg)
```

**Entity: Content Catalog**
```
Table: content_catalog
Columns: content_id (PK), title, content_type, genre_primary, genre_secondary, release_year, runtime_minutes, maturity_rating, production_country, director, cast_json, imdb_score, awards_count
Sample Size: 50K titles
Dimensions: Genre (25), Country (195), Rating (7)
```

#### 2.2.2 Event Streaming (High Volume)

**Stream: Viewing Events**
```
Topic: view-events-firehose
Schema: event_id, profile_id, content_id, device_type, event_type, timestamp_ms, watch_duration_sec, bitrate_kbps, buffer_events, cdn_pop, app_version, seek_events, subtitle_lang
Throughput: 2M events/second peak
Retention: 90 days
Compression: Snappy (30% reduction)
```

#### 2.2.3 OLAP Schema (Analytics)

**Fact: Hourly Engagement**
```
Table: fact_hourly_engagement
Columns: date_hour_key, content_key, country_key, device_key, unique_viewers, total_watch_hours, completion_rate, avg_bitrate, rebuffer_ratio, session_starts, user_ratings_avg
Grain: Hourly × Content × Country × Device
Size: 8760 × 50K × 195 × 10 = 855B rows/year
```

### 2.3 Uber Ride-Hailing Platform

#### 2.3.1 OLTP Schema (Transactional)

**Entity: Drivers & Vehicles**
```
Table: drivers
Columns: driver_id (PK), onboard_date, home_city, license_expiry, vehicle_type, vehicle_year, rating_avg, trips_completed, acceptance_rate, cancellation_rate, earnings_ytd_aed, status
Sample Size: 5M drivers globally
```

**Entity: Rides Lifecycle**
```
Table: rides
Columns: ride_id (PK), rider_id (FK), driver_id (FK), request_ts, accept_ts, pickup_ts, dropoff_ts, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, distance_km, duration_sec, fare_base_aed, surge_multiplier, tips_aed, tolls_aed, final_fare_aed, rating_rider, rating_driver, ride_status
Sample Size: 20M rides/day
Partitioning: Time (hourly) + City hash
```

#### 2.3.2 Event Streaming (Location-based)

**Stream: Ride Events**
```
Topic: ride-state-changes
Schema: event_id, ride_id, event_type, timestamp_ms, lat, lng, surge_zone, eta_seconds, driver_heading, speed_kmh, battery_level, app_version, network_type
Throughput: 100K events/second
Geospatial: Indexed by H3 hexagons (resolution 7)
```

#### 2.3.3 OLAP Schema (Analytics)

**Fact: City Performance**
```
Table: fact_city_hourly_metrics
Columns: date_hour_key, city_key, weather_key, total_requests, fulfilled_rides, avg_wait_minutes, avg_fare_aed, avg_rating, surge_hours, driver_utilization_rate, cancellation_rate, completed_trips_per_driver
Grain: Hourly × City × Weather
Size: 8760 × 500 × 20 = 87.6M rows/year
```

### 2.4 Airbnb Marketplace Platform

#### 2.4.1 OLTP Schema (Transactional)

**Entity: Properties & Amenities**
```
Table: properties
Columns: property_id (PK), host_id (FK), city, neighborhood, property_type, room_type, bedrooms, bathrooms, max_guests, amenities_json, base_price_aed, cleaning_fee_aed, security_deposit_aed, minimum_nights, maximum_nights, instant_book, cancellation_policy, listing_date, last_updated
Sample Size: 7M active listings
Indexing: city, property_type, price_range
```

**Entity: Bookings & Reviews**
```
Table: bookings
Columns: booking_id (PK), guest_id (FK), property_id (FK), checkin_date, checkout_date, nights, guests_count, booking_status, total_price_aed, host_fee_aed, service_fee_aed, taxes_aed, applied_discounts_aed, booking_channel, special_requests_text
Sample Size: 300M bookings/year

Table: reviews
Columns: review_id (PK), booking_id (FK), reviewer_type, overall_rating, cleanliness_rating, communication_rating, checkin_rating, accuracy_rating, location_rating, value_rating, review_text, review_date, response_text, response_date
Sample Size: 200M reviews (67% response rate)
```

#### 2.4.2 Event Streaming (Booking Lifecycle)

**Stream: Booking Events**
```
Topic: booking-lifecycle
Schema: event_id, booking_id, event_type, timestamp_ms, property_id, guest_id, price_change_aed, availability_change, search_rank_position, competitor_pricing_delta, seasonal_demand_index
Throughput: 25K events/second
```

#### 2.4.3 OLAP Schema (Analytics)

**Fact: Market Performance**
```
Table: fact_market_daily_performance
Columns: date_key, city_key, property_type_key, season_key, available_listings, booked_nights, occupancy_rate, avg_daily_rate_aed, revenue_per_available_night_aed, avg_length_of_stay, new_listings, cancelled_bookings_rate, host_response_rate, guest_satisfaction_score
Grain: Daily × City × Property Type × Season
Size: 365 × 1000 × 15 × 4 = 21.9M rows/year
```

---

## Module 3: Data Science Projects & ML Techniques

### 3.1 Churn Analysis & Prediction

#### 3.1.1 Feature Engineering Framework

| **Feature Category** | **Amazon Example** | **Netflix Example** | **Uber Example** | **Airbnb Example** |
|---------------------|-------------------|------------------|-----------------|------------------|
| **Recency** | `days_since_last_order` | `days_since_last_view` | `days_since_last_ride` | `days_since_last_booking` |
| **Frequency** | `orders_per_month` | `viewing_sessions_per_week` | `rides_per_month` | `bookings_per_year` |
| **Monetary** | `total_spent_6m` | `subscription_value` | `total_fare_paid_3m` | `total_booking_value_ytd` |
| **Engagement** | `product_categories_explored` | `content_genres_watched` | `unique_routes_taken` | `property_types_searched` |
| **Support** | `support_tickets_3m` | `technical_issues_reported` | `ride_complaints_filed` | `host_disputes_count` |

#### 3.1.2 Normalization & Preprocessing

```
Feature Preprocessing Pipeline:

1. Monetary Features:
   - Log transformation: log(1 + amount_aed)
   - Robust scaling: (x - median) / IQR
   - Outlier capping: 95th percentile

2. Categorical Features:
   - High cardinality (>50): Target encoding with K-fold CV
   - Low cardinality: One-hot encoding
   - Rare categories: Grouped into "Other"

3. Temporal Features:
   - Seasonality: sin/cos encoding for cyclical patterns
   - Trends: Rolling averages (7, 30, 90 days)
   - Time since events: Square root transformation

4. Missing Values:
   - Numerical: Median imputation + missingness indicator
   - Categorical: Mode imputation or "Unknown" category
```

#### 3.1.3 ML Techniques & Libraries

| **Algorithm** | **Use Case** | **Library** | **Hyperparameters** |
|---------------|--------------|-------------|-------------------|
| **Logistic Regression** | Baseline interpretable model | `scikit-learn` | `C=[0.1, 1, 10], penalty=['l1','l2']` |
| **XGBoost** | Tree-based ensemble | `xgboost` | `max_depth=[3,6], learning_rate=[0.01,0.1], n_estimators=[100,500]` |
| **LightGBM** | Gradient boosting optimized | `lightgbm` | `num_leaves=[31,127], feature_fraction=[0.8,1.0]` |
| **Random Forest** | Robust ensemble | `scikit-learn` | `n_estimators=[100,300], max_features=['sqrt','log2']` |

**Evaluation Framework:**
```
Metrics:
- Primary: AUC-ROC, Precision-Recall AUC
- Business: Precision@10%, Lift@20%
- Calibration: Brier Score, Reliability Diagram

Cross-validation:
- Time-based splits (not random)
- Training: Months 1-18
- Validation: Month 19-20
- Test: Month 21-24
```

### 3.2 Recommendation Systems

#### 3.2.1 Collaborative Filtering

**Matrix Factorization (Netflix, Amazon)**
```
Approach: Alternating Least Squares (ALS)
Library: implicit, spark.ml
Features: user_id, item_id, rating/interaction_strength
Hyperparameters: factors=[50,100,200], regularization=[0.01,0.1], iterations=[10,20]

Evaluation:
- Precision@K, Recall@K (K=5,10,20)
- Mean Average Precision (MAP)
- Normalized Discounted Cumulative Gain (NDCG)
```

#### 3.2.2 Content-Based Filtering

**Feature Engineering for Items:**
```
Netflix Content Features:
- Genre embeddings (Word2Vec on genre combinations)
- Director/Actor popularity scores
- Content age and runtime buckets
- IMDb ratings and review sentiment

Amazon Product Features:
- Category hierarchy embeddings
- Brand popularity in category
- Price percentile within category
- Review sentiment and rating distribution
```

#### 3.2.3 Hybrid & Deep Learning Approaches

| **Model Type** | **Architecture** | **Library** | **Use Case** |
|----------------|-----------------|-------------|-------------|
| **Two-Tower** | Separate encoders for users/items | `TensorFlow Recommenders` | Large-scale retrieval |
| **Wide & Deep** | Linear + DNN components | `TensorFlow` | Click prediction |
| **DeepFM** | FM + Deep layers | `PyTorch` | CTR prediction |
| **Neural CF** | Generalized MF with MLPs | `PyTorch Lightning` | Rating prediction |

### 3.3 Demand Forecasting

#### 3.3.1 Time Series Features

**Feature Categories:**
```
Lag Features:
- Values at t-1, t-7, t-30 (recent, weekly, monthly patterns)
- Rolling means: 3-day, 7-day, 30-day windows
- Rolling std: Volatility measures

External Features:
- Holiday indicators (national, religious, local events)
- Weather indices (temperature, precipitation, seasonal)
- Economic indicators (unemployment, GDP growth, inflation)
- Marketing spend and campaign flags

Cyclical Features:
- Hour of day (sin/cos encoding)
- Day of week (ordinal + cyclical)
- Month of year (seasonal trends)
- Day of month (pay period effects)
```

#### 3.3.2 Forecasting Models by Use Case

| **Company** | **Target Variable** | **Model** | **Library** | **Features** |
|-------------|-------------------|----------|-------------|-------------|
| **Uber** | Hourly ride demand by city | LightGBM | `lightgbm` | Weather, events, historical lags |
| **Airbnb** | Daily occupancy rate | Prophet | `prophet` | Seasonality, holidays, price changes |
| **Amazon** | Weekly product demand | N-BEATS | `darts` | Promotions, competitor pricing |
| **NYSE** | Intraday volume | LSTM | `PyTorch` | Market microstructure, news events |

#### 3.3.3 Model Evaluation & Validation

```
Validation Strategy:
- Walk-forward validation (expand window)
- Multiple forecast horizons (1-day, 7-day, 30-day)
- Business day vs. calendar day evaluation

Metrics:
- MAPE (Mean Absolute Percentage Error)
- WMAPE (Weighted MAPE for aggregated forecasts)
- sMAPE (Symmetric MAPE for zero-handling)
- Directional accuracy (forecast sign correctness)

Business Metrics:
- Inventory cost reduction
- Service level maintenance (95th percentile)
- Revenue impact from improved planning
```

### 3.4 Sentiment Analysis

#### 3.4.1 Text Preprocessing Pipeline

```
Text Cleaning Steps:
1. Language detection (langdetect library)
2. HTML/URL removal and normalization
3. Tokenization with spaCy (multi-language support)
4. Stopword removal (customized by domain)
5. Lemmatization and stemming
6. N-gram extraction (1-gram to 3-gram)

Feature Engineering:
- TF-IDF vectors (max_features=10K, min_df=5, max_df=0.95)
- Word embeddings: Word2Vec, GloVe, FastText
- Sentence embeddings: Universal Sentence Encoder
- Linguistic features: sentence length, punctuation, emoji count
```

#### 3.4.2 Model Approaches

| **Approach** | **Algorithm** | **Library** | **Use Case** |
|--------------|--------------|-------------|-------------|
| **Classical** | TF-IDF + Logistic Regression | `scikit-learn` | Baseline, interpretable |
| **Classical** | TF-IDF + SVM | `scikit-learn` | High-dimensional text |
| **Deep Learning** | BERT fine-tuning | `transformers` | High accuracy, multiple languages |
| **Deep Learning** | DistilBERT | `transformers` | Faster inference, good accuracy |
| **Ensemble** | Voting classifier | `scikit-learn` | Combining multiple models |

#### 3.4.3 Domain-Specific Applications

**Airbnb Review Sentiment:**
```
Labels: Positive/Negative/Neutral
Features: review_text, rating_overall, host_response_present
Metrics: F1-score by sentiment class, confusion matrix
Challenge: Sarcasm detection, cultural differences
```

**Amazon Product Review Analysis:**
```
Multi-aspect sentiment: Product quality, shipping, customer service
Approach: Multi-task learning with shared representations
Output: Aspect-level sentiment scores (1-5 scale)
Business value: Product improvement prioritization
```

---

## Module 4: OLTP vs OLAP Systems

### 4.1 OLTP System Design (Transactional)

#### 4.1.1 Characteristics & Requirements

| **Aspect** | **Requirements** | **Example (Amazon Orders)** |
|------------|-----------------|---------------------------|
| **Consistency** | ACID compliance | Order total = sum(line items) |
| **Normalization** | 3NF, minimal redundancy | Separate customers, products, orders tables |
| **Concurrency** | High write throughput | 10K orders/second during Black Friday |
| **Latency** | Sub-100ms response | Order confirmation within 50ms |
| **Indexing** | Primary keys, foreign keys | B+ trees on order_id, customer_id |

#### 4.1.2 Schema Design Pattern

**Normalized Structure (Amazon):**
```
customers (50M rows)
├── customer_id (PK)
├── email_hash
└── registration_date

products (500M rows)
├── product_id (PK)
├── category_id (FK)
└── current_price_aed

orders (2B rows/year)
├── order_id (PK)
├── customer_id (FK)
├── order_timestamp
└── status

order_items (8B rows/year)
├── order_item_id (PK)
├── order_id (FK)
├── product_id (FK)
├── quantity
└── unit_price_aed
```

### 4.2 OLAP System Design (Analytics)

#### 4.2.1 Characteristics & Requirements

| **Aspect** | **Requirements** | **Example (Sales Analytics)** |
|------------|-----------------|------------------------------|
| **Denormalization** | Star/snowflake schema | Wide fact tables with dimensions |
| **Read-optimized** | Columnar storage | Parquet files with compression |
| **Aggregation** | Pre-computed summaries | Daily/monthly sales rollups |
| **Historical** | Long retention periods | 5+ years of transaction history |
| **Batch processing** | ETL pipelines | Nightly aggregation jobs |

#### 4.2.2 Star Schema Design (Netflix Analytics)

**Fact Table: Content Engagement**
```sql
fact_content_engagement (100B rows)
├── date_key (FK to dim_date)
├── content_key (FK to dim_content)  
├── user_key (FK to dim_user)
├── device_key (FK to dim_device)
├── country_key (FK to dim_geography)
├── watch_time_minutes (MEASURE)
├── completion_rate (MEASURE)
├── quality_score (MEASURE)
└── revenue_attributed_aed (MEASURE)
```

**Dimension Tables:**
```sql
dim_content (50K rows)
├── content_key (PK)
├── title
├── genre_primary
├── release_year
└── content_type

dim_user (250M rows)
├── user_key (PK)
├── signup_cohort
├── subscription_tier
└── churn_risk_segment

dim_date (10K rows for 30 years)
├── date_key (PK)
├── calendar_date
├── day_of_week
├── month_name
├── quarter
├── is_holiday
└── is_weekend
```

### 4.3 Query Performance Comparison

#### 4.3.1 OLTP Query Examples (Point Lookups)

```sql
-- Customer's recent orders (OLTP - optimized for single customer)
-- Expected: <10ms response time
SELECT o.order_id, o.order_date, o.total_aed, o.status
FROM orders o 
WHERE o.customer_id = 'CUST123' 
  AND o.order_date >= '2024-01-01'
ORDER BY o.order_date DESC
LIMIT 10;

-- Product inventory check (OLTP - real-time stock)
-- Expected: <5ms response time  
SELECT product_id, current_stock, reserved_stock
FROM products 
WHERE product_id = 'PROD456';
```

#### 4.3.2 OLAP Query Examples (Aggregations)

```sql
-- Monthly sales by category (OLAP - analytical)
-- Expected: 2-10 seconds for large datasets
SELECT 
    d.month_name,
    p.category_lvl1,
    SUM(f.gross_revenue_aed) as total_revenue,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    AVG(f.order_value_aed) as avg_order_value
FROM fact_daily_sales f
JOIN dim_date d ON f.date_key = d.date_key  
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.year = 2024
GROUP BY d.month_name, p.category_lvl1
ORDER BY total_revenue DESC;

-- Cohort retention analysis (OLAP - complex analytical)
-- Expected: 30-60 seconds
WITH user_cohorts AS (
    SELECT 
        u.user_key,
        DATE_TRUNC('month', u.signup_date) as cohort_month
    FROM dim_user u
),
engagement_by_month AS (
    SELECT 
        uc.cohort_month,
        uc.user_key,
        d.month_key,
        CASE WHEN f.watch_time_minutes > 0 THEN 1 ELSE 0 END as active
    FROM user_cohorts uc
    CROSS JOIN dim_date d
    LEFT JOIN fact_content_engagement f ON uc.user_key = f.user_key AND d.date_key = f.date_key
    WHERE d.month_key BETWEEN '2024-01' AND '2024-12'
)
SELECT 
    cohort_month,
    month_key,
    COUNT(DISTINCT user_key) as cohort_size,
    SUM(active) as active_users,
    100.0 * SUM(active) / COUNT(DISTINCT user_key) as retention_rate
FROM engagement_by_month
GROUP BY cohort_month, month_key
ORDER BY cohort_month, month_key;
```

---

## Module 5: NYSE High-Dimensional Analytics

### 5.1 Market Microstructure Data

#### 5.1.1 Ultra-High Frequency Schema

**Tick Data (Primary Events)**
```
Table: trade_ticks
Columns: tick_id, ticker, trade_timestamp_ms, trade_price, trade_size, trade_side, venue_code, liquidity_flag, trade_condition, order_id, buyer_id, seller_id, execution_latency_microsec
Volume: 5B messages/day
Partitioning: Time (hourly) + Ticker hash
Compression: Delta encoding for prices, LZ4 for text
```

**Order Book Snapshots (L2 Market Data)**
```
Table: order_book_snapshots  
Columns: snapshot_id, ticker, snapshot_timestamp_ms, best_bid_price, best_ask_price, bid_size_level1, ask_size_level1, bid_ask_spread_bps, market_depth_levels_json, order_imbalance_ratio, quote_count, micro_price
Volume: 50M snapshots/day per ticker (100 tickers = 5B/day)
Storage: JSON arrays for depth levels 1-10
Indexing: ticker + timestamp range queries
```

#### 5.1.2 High-Dimensional Feature Engineering

**Technical Indicators (Per Minute)**
```sql
CREATE TABLE features_minute_high_dim (
    -- Time & Identifier
    minute_timestamp TIMESTAMP,
    ticker VARCHAR(10),
    
    -- Price & Returns (8 features)
    open_price DECIMAL(10,4),
    high_price DECIMAL(10,4), 
    low_price DECIMAL(10,4),
    close_price DECIMAL(10,4),
    vwap DECIMAL(10,4),
    return_1m DECIMAL(8,6),
    return_5m DECIMAL(8,6),
    return_15m DECIMAL(8,6),
    
    -- Volume & Liquidity (12 features)  
    volume_shares BIGINT,
    volume_notional_usd DECIMAL(15,2),
    trade_count INTEGER,
    avg_trade_size DECIMAL(10,2),
    volume_imbalance DECIMAL(8,6),
    signed_volume BIGINT,
    buy_volume_ratio DECIMAL(6,4),
    large_trade_ratio DECIMAL(6,4),
    vpin DECIMAL(8,6),  -- Volume-synchronized PIN
    kyle_lambda DECIMAL(8,6),  -- Price impact measure
    amihud_illiq DECIMAL(12,6),  -- Amihud illiquidity
    bid_ask_spread_bps DECIMAL(6,2),
    
    -- Volatility & Momentum (15 features)
    realized_volatility_5m DECIMAL(8,6),
    realized_volatility_15m DECIMAL(8,6), 
    realized_volatility_60m DECIMAL(8,6),
    parkinson_vol_5m DECIMAL(8,6),  -- High-low volatility estimator
    garman_klass_vol_5m DECIMAL(8,6),  -- OHLC volatility estimator
    momentum_5m DECIMAL(8,6),
    momentum_15m DECIMAL(8,6),
    momentum_60m DECIMAL(8,6),
    rsi_14 DECIMAL(6,4),  -- Relative strength index
    williams_r_14 DECIMAL(6,4),  -- Williams %R
    stoch_k_14 DECIMAL(6,4),  -- Stochastic oscillator
    macd_signal DECIMAL(8,6),  -- MACD signal line
    bollinger_position DECIMAL(6,4),  -- Position in Bollinger bands
    atr_14 DECIMAL(8,6),  -- Average True Range
    adx_14 DECIMAL(6,4),  -- Average Directional Index
    
    -- Market Microstructure (20 features)
    order_flow_imbalance DECIMAL(8,6),
    tick_rule_buy_ratio DECIMAL(6,4),
    effective_spread_bps DECIMAL(6,2),
    realized_spread_bps DECIMAL(6,2), 
    price_improvement_bps DECIMAL(6,2),
    quote_slope DECIMAL(8,6),  -- Best bid/ask price trend
    depth_imbalance_l1 DECIMAL(8,6),  -- Level 1 size imbalance
    depth_imbalance_l5 DECIMAL(8,6),  -- Level 1-5 size imbalance  
    weighted_spread_l5 DECIMAL(6,2),  -- Depth-weighted spread
    order_arrival_rate DECIMAL(8,4),  -- Orders per second
    cancellation_rate DECIMAL(6,4),  -- Cancel/order ratio
    hidden_liquidity_ratio DECIMAL(6,4),  -- Iceberg indicator
    venue_fragmentation_index DECIMAL(6,4),  -- Multi-venue measure
    latency_arbitrage_signal DECIMAL(8,6),  -- Cross-venue speed edge
    news_sentiment_score DECIMAL(6,4),  -- Real-time news sentiment
    options_put_call_ratio DECIMAL(8,4),  -- Options flow signal
    vix_term_structure_slope DECIMAL(8,6),  -- Volatility curve
    credit_spread_change_bps DECIMAL(6,2),  -- Corporate bond signal
    sector_relative_strength DECIMAL(8,6),  -- Sector momentum
    earnings_surprise_factor DECIMAL(8,6),  -- Fundamental surprise
    
    -- Prediction Targets (5 features)
    return_next_1m DECIMAL(8,6),  -- Forward 1-minute return
    return_next_5m DECIMAL(8,6),  -- Forward 5-minute return  
    volatility_next_15m DECIMAL(8,6),  -- Forward volatility
    volume_next_1m_above_avg BOOLEAN,  -- Volume surge indicator
    direction_next_1m INTEGER  -- -1/0/+1 for down/flat/up
);
```

### 5.2 OLAP Analytics for Trading

#### 5.2.1 Market Event Impact Analysis

**Fact Table: Event Reactions**
```sql
fact_event_reactions (10M rows/year)
├── event_timestamp_key (FK)
├── ticker_key (FK)  
├── event_type_key (FK)
├── venue_key (FK)
├── pre_event_price DECIMAL(10,4)
├── post_event_price DECIMAL(10,4)  
├── abnormal_return_bps DECIMAL(8,4)
├── volume_uplift_ratio DECIMAL(8,4)
├── spread_change_bps DECIMAL(6,2)
├── volatility_increase DECIMAL(8,6)
└── recovery_time_seconds INTEGER

-- Example analytical query
SELECT 
    et.event_category,
    AVG(f.abnormal_return_bps) as avg_price_impact,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.volume_uplift_ratio) as volume_impact_90pct,
    AVG(f.recovery_time_seconds) as avg_recovery_time
FROM fact_event_reactions f
JOIN dim_event_type et ON f.event_type_key = et.event_type_key
WHERE f.event_timestamp_key >= '2024-01-01'
GROUP BY et.event_category
ORDER BY avg_price_impact DESC;
```

#### 5.2.2 Cross-Asset Correlation Analysis

**Correlation Matrix Updates (Real-time)**
```sql
-- Rolling correlation computation (vectorized)
WITH price_changes AS (
    SELECT 
        ticker,
        minute_timestamp,
        return_1m,
        LAG(return_1m, 1) OVER (PARTITION BY ticker ORDER BY minute_timestamp) as lag1_return,
        LAG(return_1m, 5) OVER (PARTITION BY ticker ORDER BY minute_timestamp) as lag5_return
    FROM features_minute_high_dim
    WHERE minute_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
),
correlation_pairs AS (
    SELECT 
        a.ticker as ticker_a,
        b.ticker as ticker_b,
        CORR(a.return_1m, b.return_1m) as correlation_1m,
        CORR(a.return_1m, b.lag1_return) as lead_lag_correlation,
        COUNT(*) as observation_count
    FROM price_changes a
    JOIN price_changes b ON a.minute_timestamp = b.minute_timestamp
    WHERE a.ticker < b.ticker  -- Avoid duplicate pairs
      AND a.return_1m IS NOT NULL 
      AND b.return_1m IS NOT NULL
    GROUP BY a.ticker, b.ticker
    HAVING COUNT(*) >= 30  -- Minimum observations
)
SELECT * FROM correlation_pairs 
WHERE ABS(correlation_1m) > 0.3  -- Significant correlations only
ORDER BY ABS(correlation_1m) DESC;
```

### 5.3 Performance Optimization Strategies

#### 5.3.1 Storage & Compression

| **Data Type** | **Storage Format** | **Compression** | **Reduction** |
|---------------|-------------------|----------------|---------------|
| **Tick Prices** | Delta encoding + Parquet | ZSTD Level 3 | 85% reduction |
| **Timestamps** | Millisecond offsets | Delta + RLE | 90% reduction |
| **Venue/Ticker** | Dictionary encoding | LZ4 | 95% reduction |
| **JSON Depth** | MessagePack binary | Snappy | 60% reduction |

#### 5.3.2 Query Performance Tuning

**Partitioning Strategy:**
```sql
-- Time-based partitioning (hourly)
PARTITION BY RANGE (EXTRACT(epoch FROM trade_timestamp_ms)/3600)

-- Secondary partitioning by ticker hash  
SUBPARTITION BY HASH (ticker) SUBPARTITIONS 16

-- Covering indexes for common queries
CREATE INDEX idx_ticker_time_covering 
ON trade_ticks (ticker, trade_timestamp_ms) 
INCLUDE (trade_price, trade_size, venue_code);

-- Partial indexes for active trading hours
CREATE INDEX idx_active_hours 
ON trade_ticks (ticker, trade_timestamp_ms)
WHERE EXTRACT(hour FROM trade_timestamp_ms) BETWEEN 9 AND 16;
```

**Query Optimization Example:**
```sql
-- Optimized TWAP calculation (Time-Weighted Average Price)
WITH trades_with_weights AS (
    SELECT 
        ticker,
        trade_price,
        trade_size,
        trade_timestamp_ms,
        -- Time weight = seconds until next trade
        COALESCE(
            EXTRACT(epoch FROM LEAD(trade_timestamp_ms) OVER w) - 
            EXTRACT(epoch FROM trade_timestamp_ms),
            60  -- Default 60 seconds for last trade
        ) as time_weight
    FROM trade_ticks 
    WHERE ticker IN ('AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA')
      AND trade_timestamp_ms >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    WINDOW w AS (PARTITION BY ticker ORDER BY trade_timestamp_ms)
)
SELECT 
    ticker,
    SUM(trade_price * time_weight) / SUM(time_weight) as twap_price,
    SUM(trade_size) as total_volume,
    COUNT(*) as trade_count,
    MIN(trade_timestamp_ms) as start_time,
    MAX(trade_timestamp_ms) as end_time
FROM trades_with_weights
GROUP BY ticker
ORDER BY total_volume DESC;
```

### 5.4 Real-time Analytics Pipeline

#### 5.4.1 Streaming Architecture

```
Data Flow:
Market Data Feed → Kafka (partitioned by ticker) → 
Spark Streaming (micro-batches) → 
Feature Store (Redis) → 
ML Models (online inference) → 
Trading Signals → Position Management
```

**Kafka Topics Configuration:**
```
market-data-raw:
  partitions: 50 (by ticker hash)
  replication-factor: 3
  retention: 24 hours
  compression: snappy

features-computed:
  partitions: 20  
  replication-factor: 3
  retention: 7 days
  compression: lz4

trading-signals:
  partitions: 10
  replication-factor: 3  
  retention: 30 days
  compression: gzip
```

#### 5.4.2 Feature Store Design

**Redis Cluster for Low-Latency Features:**
```python
# Feature key pattern: {ticker}:{feature_type}:{window}
# Example keys:
AAPL:price:1m → {"price": 150.25, "volume": 1000, "timestamp": 1634567890}
AAPL:volatility:5m → {"realized_vol": 0.025, "gk_vol": 0.028}
AAPL:order_flow:1m → {"imbalance": 0.15, "pressure": 0.82}

# TTL: 1 hour (features auto-expire)
# Memory usage: ~50GB for 100 tickers × 50 features × 60 minutes
# Access pattern: 99.9% of reads within 1ms
```

---

## Implementation Guidance

### Dataset Generation Prompts

#### Amazon E-Commerce Prompt
```
Generate 12 months of synthetic Amazon e-commerce data with realistic distributions:

OLTP Tables:
- customers: 50M records with region distribution (UAE 30%, KSA 25%, Egypt 20%, other MENA 25%)
- products: 500M records across 15 top-level categories, price range 10-10000 AED
- orders: 2B records with seasonal patterns (20% higher in Nov-Dec), 2.4 items/order average
- order_items: 8B records with quantity distribution (80% qty=1, 15% qty=2-3, 5% qty>3)

Event Stream:  
- order_events: 50K events/second peak, lifecycle states (created→paid→shipped→delivered)
- Include 15% cancellation rate, 8% return rate

OLAP Aggregates:
- Daily sales by category×region with AOV, conversion rates, fulfillment SLAs
- Currency: AED throughout
```

#### Netflix Streaming Prompt  
```
Generate 12 months of Netflix streaming data with global scale:

OLTP Tables:
- users: 250M global users, subscription tiers (Basic 40%, Standard 35%, Premium 25%) 
- content_catalog: 50K titles, genre distribution based on Netflix public data
- profiles: 600M profiles (2.4 per user average), age rating preferences

Event Stream:
- view_events: 2M events/second peak (US evening), include rebuffering, bitrate adaptation
- Device distribution: Mobile 45%, TV 30%, Desktop 20%, Tablet 5%

OLAP Aggregates:  
- Hourly engagement by content×country×device
- Churn signals: days since last view, viewing diversity, plan changes
```

#### NYSE Market Data Prompt
```
Generate 6 months of NYSE high-frequency data with realistic microstructure:

Streaming Data:
- trade_ticks: 100 liquid tickers, 5B messages/day total
- order_book_snapshots: L2 depth data, 10 levels, millisecond timestamps  
- 50 features per minute: price/volume/volatility/microstructure indicators

Market Events:
- Earnings announcements, Fed speeches, economic releases
- Include abnormal return/volume reactions with realistic decay patterns

Storage:
- Parquet with delta encoding for prices
- JSON for order book depth arrays
- Partition by hour + ticker hash for parallel processing
```

### EDA & Analysis Prompts

#### Cross-Platform Analysis
```
Perform comparative analysis across all datasets:

Volume Patterns:
- Compare peak hour distributions (Amazon orders vs Netflix views vs Uber rides)
- Seasonality analysis: holiday effects, weekend patterns, regional differences

User Behavior:
- Engagement metrics: session duration, return frequency, churn patterns
- Cross-platform correlation: does Amazon Prime affect Netflix usage?

Technical Performance:
- Query latency comparison: OLTP point lookups vs OLAP aggregations  
- Storage efficiency: compression ratios, index effectiveness
- Scaling bottlenecks: identify performance degradation points
```

### ML Pipeline Prompts

#### Feature Engineering Validation
```
Validate feature quality across all ML projects:

Statistical Tests:
- Feature stability: PSI (Population Stability Index) month-over-month
- Correlation analysis: identify highly correlated features (>0.95)
- Information value: rank features by predictive power

Data Quality:
- Missing value patterns: time-based missing data analysis
- Outlier detection: statistical and business rule validation
- Label leakage: temporal validation of features vs targets

Model Performance:
- Cross-validation with time-aware splits
- Backtesting with realistic holdout periods  
- A/B test simulation for business impact estimation
```