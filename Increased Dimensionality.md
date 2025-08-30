# 1. **Overview & Goals**

* Build **realistic, reproducible synthetic datasets** for **Amazon, Netflix, Uber, Airbnb, NYSE** to practice **big-data scaling, ingestion, OLTP/OLAP design, and data science workflows**.
* No code here—only **clearly labeled prompts** and **column schemas** you can give to any LLM/agent to generate data and guidance for EDA/modeling.
* Emphasize **volume, velocity, and variety**; include **high-dimensional** variants (esp. for NYSE) for performance tests.

# 2. **Big-Data & Scaling Scenarios**

* **Throughput & Burstiness (Streaming):**

  * Netflix view events (millions/day), Uber ride events (bursty commuting peaks), NYSE ticks (thousands/sec).
* **Wide Tables & Joins (Variety):**

  * Amazon multi-item orders; Airbnb multi-amenity properties; NYSE with rich market microstructure features.
* **Historical Depth (Volume):**

  * 12–36 months of data for Amazon/Netflix/Uber/Airbnb; millisecond-level NYSE ticks for select tickers.
* **Partitioning & Aggregation:**

  * Time-based partitions (hour/day) for OLAP; sharding keys (customer\_id, region, ticker) for OLTP scaling.

# 3. **Amazon — Synthetic Dataset (E-Commerce)**

**Use Case Prompts:** simulate **orders, carts, returns, delivery SLAs, and promotions** across regions; stress-test OLTP order writes and OLAP category sales.
**Entity Tables (OLTP prompts):**

* `customers` — `customer_id`, `signup_date`, `region`, `age_band`, `loyalty_tier`, `marketing_opt_in`
* `products` — `product_id`, `sku`, `category_lvl1`, `category_lvl2`, `brand`, `price_aed`, `stock_qty`, `weight_g`, `dimensions_cm`
* `orders` — `order_id`, `customer_id`, `order_ts`, `channel(web/mobile)`, `payment_method`, `order_status`, `currency=AED`, `total_aed`, `promo_code`
* `order_items` — `order_item_id`, `order_id`, `product_id`, `qty`, `unit_price_aed`, `discount_pct`, `tax_aed`, `line_total_aed`
* `shipments` — `shipment_id`, `order_id`, `carrier`, `ship_ts`, `delivery_ts`, `sla_hours`, `delivery_status`, `warehouse_id`
  **Event Streams (Ingestion prompts):**
* `order_events` — `event_id`, `order_id`, `event_type(created/paid/shipped/delivered/returned)`, `actor(system/customer)`, `event_ts`, `channel`, `risk_score`
  **Analytics (OLAP prompts):**
* `agg_daily_sales` — `date`, `category_lvl1`, `orders`, `units`, `gross_revenue_aed`, `discounts_aed`, `returns_aed`, `avg_order_value_aed`, `conversion_rate`
* `agg_fulfillment_sla` — `date`, `warehouse_id`, `orders`, `on_time_pct`, `avg_delivery_days`, `return_rate_pct`

# 4. **Netflix — Synthetic Dataset (Streaming Media)**

**Use Case Prompts:** simulate **playback events, quality metrics, content catalog, and subscription churn**.
**Entity Tables (OLTP prompts):**

* `users` — `user_id`, `signup_date`, `country`, `plan(basic/standard/premium)`, `tenure_days`, `payment_status(active/past_due)`
* `profiles` — `profile_id`, `user_id`, `age_rating_pref`, `language_pref`, `device_favorites(list)`
* `content_catalog` — `content_id`, `title`, `type(series/film)`, `genre`, `release_year`, `run_time_min`, `maturity_rating`
* `subscriptions` — `sub_id`, `user_id`, `start_date`, `end_date`, `status`, `billing_cycle_days`, `price_aed`
  **Event Streams (Ingestion prompts):**
* `view_events` — `event_id`, `profile_id`, `content_id`, `device_type`, `event_type(play/pause/stop/seek)`, `ts`, `watch_sec`, `bitrate_kbps`, `buffer_events`, `cdn_pop`, `app_version`
  **Analytics (OLAP prompts):**
* `agg_hourly_engagement` — `date_hour`, `content_id`, `views`, `unique_profiles`, `avg_watch_sec`, `rebuffer_ratio`, `completion_rate`
* `agg_churn_signals_daily` — `date`, `country`, `plan`, `logins`, `days_since_last_view`, `downgrade_rate`, `cancel_rate`

# 5. **Uber — Synthetic Dataset (Ride-Hailing)**

**Use Case Prompts:** simulate **ride lifecycle, driver supply, pricing, cancellations, and ETA accuracy**.
**Entity Tables (OLTP prompts):**

* `riders` — `rider_id`, `signup_date`, `home_city`, `device_os`, `wallet_balance_aed`, `rating_rider`
* `drivers` — `driver_id`, `onboard_date`, `vehicle_type`, `vehicle_year`, `rating_driver`, `accept_rate_pct`, `cancel_rate_pct`
* `rides` — `ride_id`, `rider_id`, `driver_id`, `request_ts`, `pickup_ts`, `dropoff_ts`, `pickup_lat`, `pickup_lng`, `dropoff_lat`, `dropoff_lng`, `distance_km`, `fare_aed`, `tip_aed`, `status`
* `payments` — `payment_id`, `ride_id`, `method`, `charged_ts`, `net_driver_payout_aed`, `platform_fee_aed`, `tax_aed`
  **Event Streams (Ingestion prompts):**
* `ride_events` — `event_id`, `ride_id`, `event_type(request/accept/start/end/cancel)`, `ts`, `surge_multiplier`, `estimated_ETA_sec`, `gps_fix_quality`, `app_version`
  **Analytics (OLAP prompts):**
* `agg_city_daily` — `date`, `city`, `total_rides`, `completed_rides`, `avg_wait_sec`, `avg_fare_aed`, `cancellation_rate_pct`, `surge_ride_pct`
* `agg_driver_perf_weekly` — `week`, `driver_id`, `rides`, `utilization_hours`, `earnings_aed`, `accept_rate_pct`, `cancel_rate_pct`

# 6. **Airbnb — Synthetic Dataset (Marketplace Hospitality)**

**Use Case Prompts:** simulate **bookings, pricing seasonality, cancellations, reviews, and host response behavior**.
**Entity Tables (OLTP prompts):**

* `guests` — `guest_id`, `signup_date`, `country`, `n_prior_bookings`, `avg_review_score`, `cancel_history_cnt`
* `hosts` — `host_id`, `host_since`, `superhost_flag`, `response_time_hours`, `response_rate_pct`, `cancellation_policy`
* `properties` — `property_id`, `host_id`, `city`, `neighborhood`, `property_type`, `room_type`, `n_bedrooms`, `amenities(list)`, `base_price_aed`, `cleaning_fee_aed`
* `bookings` — `booking_id`, `guest_id`, `property_id`, `checkin_date`, `checkout_date`, `nights`, `status`, `price_per_night_aed`, `total_price_aed`, `applied_discount_pct`, `channel`
* `reviews` — `review_id`, `booking_id`, `guest_id`, `property_id`, `overall_score`, `cleanliness`, `communication`, `location`, `comments_text`
  **Event Streams (Ingestion prompts):**
* `booking_events` — `event_id`, `booking_id`, `event_type(request/confirm/cancel/modify)`, `ts`, `price_change_pct`, `inventory_status`, `search_rank_position`
  **Analytics (OLAP prompts):**
* `agg_city_occupancy_daily` — `date`, `city`, `available_listings`, `occupied_nights`, `occupancy_pct`, `avg_price_aed`, `cancel_rate_pct`
* `agg_host_performance_monthly` — `month`, `host_id`, `bookings`, `rev_aed`, `response_time_avg_h`, `rating_avg`

# 7. **NYSE — High-Dimensional Synthetic Dataset (Market Microstructure)**

**Use Case Prompts:** simulate **ultra-high-frequency ticks, order book depth, venues, liquidity, latency, and derived factors**.
**Tick/Trade (Streaming) prompts:**

* `trade_ticks` — `tick_id`, `ticker`, `trade_ts_ms`, `price`, `size`, `trade_side(buy/sell)`, `venue`, `liquidity_flag`, `trade_cond`
* **High-Dimensional Add-ons:**

  * `order_book_snapshots` — `snapshot_id`, `ticker`, `ts_ms`, `best_bid`, `best_ask`, `bid_size`, `ask_size`, `spread_bps`, `depth_level1_5_json`, `imbalance_ratio`
  * `market_events` — `event_id`, `ticker`, `ts_ms`, `event_type(auction/open/close/vol_halt/news)`, `venue`, `latency_ms`
  * `features_minute` — `minute_ts`, `ticker`, `ret_1m`, `vol_1m`, `mom_5m`, `rv_30m`, `bid_ask_spread_bps`, `order_imbalance`, `signed_volume`, `vpIN`, `micro_price_deviation`, `label_ret_next_1m_pos`
    **Analytics (OLAP prompts):**
* `agg_minute_ohlc` — `ticker`, `minute_ts`, `open`, `high`, `low`, `close`, `volume`, `vwap`, `spreads_avg_bps`
* `agg_event_reactions` — `event_type`, `window`, `avg_abnormal_return_bps`, `avg_spread_change_bps`, `avg_volume_uplift_pct`
  **Scaling Guidance:** include **multi-ticker** (e.g., 50–200 tickers), **millisecond timestamps**, and **nested depth levels** (stored as JSON) to increase dimensionality.

# 8. **OLTP vs OLAP — Prompts & Dataset Shapes**

**OLTP Prompts (row-level transactions):**

* Ensure **narrow, normalized tables**, strict keys, frequent updates.
* Example prompts:

  * “Generate 5M **Amazon `orders`** with realistic **`order_items`** cardinality (1–6 items), skewed by category and region.”
  * “Create **Uber `rides`** with lifecycle states; enforce FK integrity (**drivers**, **riders**). Include realistic clock skew and retries.”
    **OLAP Prompts (read-optimized aggregates):**
* Ensure **wide, denormalized** facts with time partitions and surrogate keys.
* Example prompts:

  * “Build **`agg_daily_sales`** for Amazon with **category and region** dimensions; compute **AOV, discount %, return %**.”
  * “Produce **`agg_minute_ohlc`** for NYSE and add **derived factors** (rolling volatility, momentum) for backtests.”

# 9. **Data Science Projects — Features, Normalization & Techniques**

**A) Churn Analysis (Netflix, Amazon, Airbnb, Uber)**

* **Feature Prompts:** `tenure_days`, `days_since_last_activity`, `avg_weekly_engagement`, `plan_changes_90d`, `refunds_6m`, `support_tickets`, `late_deliveries`, `ride_cancellations_pct`, `travel_seasonality_index`, `host_response_time_h`
* **Targets:** `churn_30d (0/1)`, `downgrade (0/1)`
* **Normalization/Prep:** robust scaling for skewed monetary fields; frequency encoding for high-cardinality categories; time-based leakage prevention.
* **Techniques/Libraries:** Logistic Regression, XGBoost/LightGBM, Random Forests; calibration (Platt/Isotonic); SHAP for explainability; libraries: `scikit-learn`, `xgboost`, `lightgbm`, `category_encoders`, `shap`.

**B) Recommendation (Netflix content, Amazon products, Airbnb listings)**

* **Feature Prompts:** user/content/item embeddings (genre/brand/amenities), co-watch/co-buy counts, recency/frequency/monetary (RFM), session context (device, time-of-day).
* **Approaches:** matrix factorization (ALS/BPR), gradient-boosted ranking (LambdaMART), two-tower deep retrieval, re-rankers with context.
* **Libraries:** `implicit`, `lightfm`, `xgboost` (ranking), `tensorflow-recommenders`, `pytorch-lightning`.

**C) Demand Forecasting (Uber rides, Airbnb occupancy, Amazon orders, NYSE volume)**

* **Feature Prompts:** holiday flags, weather indices (synthetic), promotions/surge, lag features (t-1, t-7), rolling means/volatility, event calendar.
* **Models:** SARIMAX/Prophet for baselines; Gradient Boosted Trees; Temporal Fusion Transformer; N-BEATS; LightGBM with lag features.
* **Libraries:** `prophet`, `statsmodels`, `lightgbm`, `pytorch-forecasting`, `darts`.

**D) Sentiment Analysis (Airbnb reviews, Amazon product reviews, Uber rider feedback, Netflix comments)**

* **Feature Prompts:** `review_text`, `language`, `stars`, `review_length`, `sentiment_score`, `topic_labels`, `toxicity_score`.
* **Approaches:** classical TF-IDF + Linear SVM; transformer fine-tuning (e.g., `bert-base-multilingual-cased`); weak supervision via rules + data programming.
* **Libraries:** `scikit-learn`, `transformers`, `spaCy`, `nltk`, `snorkel`.

**General Feature Hygiene:**

* Train/validation/test **temporal splits**; **min-max/standard/robust scaling** as needed; handle **class imbalance** (SMOTE, class weights); **target encoding** with K-folds to avoid leakage; **adversarial validation** for distribution shift.

# 10. **Reproducibility, EDA & Agent Prompts**

**Reproducibility Rules:**

* Fix random seeds; document `seed`, `rows`, `date_range`, `categorical_domains`.
* Keep currency fields in **AED** where monetary amounts are present for consistency.
* Store nested structures (e.g., order items, order book depth) as **JSON text** when simulating unstructured variety.

**EDA Prompts:**

* “Compute **null/outlier reports** by table; flag negative prices, impossible timestamps, duplicates.”
* “Profile **skew & hotspots**: top 10 categories/tickers by volume; weekday/weekend effects; peak hours.”
* “Benchmark **group-by performance** on OLAP facts for 1/7/30-day windows; test partition pruning logic (by date).”
* “Compare **OLTP vs OLAP** query latency for common questions (e.g., last order status vs. 30-day category sales).”
* “Generate **feature importance** and **drift reports** (PSI) month over month.”

---

## **Copy-Ready Prompt Blocks (No Code)**

* **Amazon Data Generation Prompt:**
  “Generate 12 months of synthetic Amazon e-commerce data with the OLTP tables `customers`, `products`, `orders`, `order_items`, `shipments` and the event stream `order_events`. Include realistic category hierarchies, promotions, returns, and delivery SLAs. Then produce OLAP aggregates `agg_daily_sales` and `agg_fulfillment_sla`. Ensure AED currency and realistic distributions (AOV, discount %, return %).”
* **Netflix Data Generation Prompt:**
  “Generate 12 months of synthetic Netflix streaming data with `users`, `profiles`, `content_catalog`, `subscriptions` and the event stream `view_events`. Capture playback quality (bitrate, rebuffering), devices, countries. Then build `agg_hourly_engagement` and `agg_churn_signals_daily`.”
* **Uber Data Generation Prompt:**
  “Generate 12 months of synthetic Uber ride data with `riders`, `drivers`, `rides`, `payments` and the event stream `ride_events`. Include city distribution, surge, cancellations, ETAs. Then produce `agg_city_daily` and `agg_driver_perf_weekly`.”
* **Airbnb Data Generation Prompt:**
  “Generate 24 months of synthetic Airbnb marketplace data with `guests`, `hosts`, `properties`, `bookings`, `reviews` and the event stream `booking_events`. Include seasonality, amenities, price bands, cancellations. Then build `agg_city_occupancy_daily` and `agg_host_performance_monthly`.”
* **NYSE High-Dimensional Prompt:**
  “Generate 6 months of NYSE high-frequency data for 100 tickers: `trade_ticks`, `order_book_snapshots`, `market_events`, plus `features_minute`. Include ms timestamps, multi-venue trades, spreads, order imbalance, latency. Build `agg_minute_ohlc` and `agg_event_reactions` with derived factors (momentum, rolling volatility).”

---

Want me to turn this into a **downloadable README.md** file or expand any company into a **full data dictionary** (with value ranges and categorical domains) you can paste into your generator?
