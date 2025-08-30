#!/usr/bin/env python3
"""
Enhanced Big Data Integration for Streamlit App
==============================================

This module integrates the comprehensive Big Data analysis into the main Streamlit app.
It replaces the existing show_big_data_scaling() function with enhanced functionality.

Features:
- Real synthetic data integration
- OLTP vs OLAP live demonstrations
- Interactive EDA analysis
- Performance comparisons
- Scaling pattern analysis
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import time
import json
from datetime import datetime, timedelta

def enhanced_show_big_data_scaling():
    """Enhanced Big Data & Scaling module with real synthetic data"""
    st.header("📊 Big Data & Scaling - Interactive Analysis")
    st.markdown("""
    **Comprehensive Big Data analysis with real synthetic datasets:**
    Amazon, Netflix, Uber, Airbnb, and NYSE market data
    """)
    
    # Check if big data database exists
    db_exists = check_big_data_database()
    
    if not db_exists:
        st.warning("🔧 Big Data database not found. Initializing...")
        if st.button("🚀 Initialize Big Data Module"):
            initialize_big_data_module()
            st.success("✅ Big Data module initialized!")
            st.experimental_rerun()
    else:
        # Enhanced tabs with real data analysis
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Volume Analysis", 
            "⚡ Velocity & Performance", 
            "🌟 Data Variety", 
            "🏢 Company Insights", 
            "⚖️ OLTP vs OLAP", 
            "📈 Scaling Strategies"
        ])
        
        with tab1:
            show_volume_analysis()
        
        with tab2:
            show_velocity_performance()
        
        with tab3:
            show_data_variety()
        
        with tab4:
            show_company_insights()
        
        with tab5:
            show_oltp_olap_comparison()
        
        with tab6:
            show_scaling_strategies()

def check_big_data_database():
    """Check if big data database exists and has data"""
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()
        return len(tables) > 0
    except:
        return False

@st.cache_data
def load_database_summary():
    """Load database summary with caching"""
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        
        # Get table counts
        tables_info = [
            ('amazon_customers', 'Amazon Customers'),
            ('amazon_orders', 'Amazon Orders'),
            ('amazon_order_items', 'Amazon Order Items'),
            ('netflix_users', 'Netflix Users'),
            ('netflix_viewing_events', 'Netflix Events'),
            ('uber_rides', 'Uber Rides'),
            ('airbnb_bookings', 'Airbnb Bookings'),
            ('nyse_trade_ticks', 'NYSE Ticks'),
            ('nyse_features_minute', 'NYSE Features'),
        ]
        
        summary = {}
        total_records = 0
        
        for table_name, display_name in tables_info:
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                summary[display_name] = count
                total_records += count
            except:
                summary[display_name] = 0
        
        summary['Total Records'] = total_records
        conn.close()
        return summary
        
    except Exception as e:
        st.error(f"Database error: {e}")
        return {}

def show_volume_analysis():
    """Show volume analysis with real data"""
    st.subheader("📊 Data Volume Analysis")
    
    summary = load_database_summary()
    
    if summary:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Create volume chart
            chart_data = []
            for table, count in summary.items():
                if table != 'Total Records':
                    chart_data.append({'Table': table, 'Records': count})
            
            df = pd.DataFrame(chart_data)
            
            fig = px.bar(df, x='Table', y='Records', 
                        title='📊 Data Volume by Table',
                        color='Records',
                        color_continuous_scale='viridis')
            fig.update_xaxis(tickangle=45)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 📈 Volume Metrics")
            for table, count in summary.items():
                if table == 'Total Records':
                    st.metric("🎯 Total Records", f"{count:,}")
                else:
                    st.metric(table, f"{count:,}")
        
        # Volume scaling projection
        st.markdown("### 🚀 Production Scale Projection")
        
        scaling_factor = st.slider("Scaling Factor", 10, 10000, 1000, step=100)
        
        projected_total = summary['Total Records'] * scaling_factor
        projected_size_gb = projected_total * 0.001  # Rough estimate
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Projected Records", f"{projected_total:,}")
        with col2:
            st.metric("💾 Estimated Storage", f"{projected_size_gb:.1f} GB")
        with col3:
            monthly_growth = projected_size_gb * 0.2  # 20% monthly growth
            st.metric("📈 Monthly Growth", f"{monthly_growth:.1f} GB")
    
    else:
        st.info("💡 Initialize the Big Data module to see volume analysis")

def show_velocity_performance():
    """Show velocity and performance analysis"""
    st.subheader("⚡ Velocity & Performance Analysis")
    
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        
        # Performance benchmark
        st.markdown("### 🏃‍♂️ Live Query Performance Test")
        
        query_type = st.selectbox("Select query type:", [
            "OLTP - Customer Lookup",
            "OLTP - Order Details",
            "OLAP - Category Analysis",
            "OLAP - Time Series Analysis"
        ])
        
        if st.button("🚀 Run Performance Test"):
            if query_type == "OLTP - Customer Lookup":
                query = "SELECT * FROM amazon_customers WHERE customer_id = 'CUST_000001'"
                description = "Point lookup - single customer"
                
            elif query_type == "OLTP - Order Details":
                query = """
                SELECT o.order_id, o.total_aed, oi.product_id, oi.quantity
                FROM amazon_orders o
                JOIN amazon_order_items oi ON o.order_id = oi.order_id
                WHERE o.order_id = 'ORDER_00000001'
                """
                description = "Simple join - order details"
                
            elif query_type == "OLAP - Category Analysis":
                query = """
                SELECT p.category_lvl1,
                       COUNT(DISTINCT o.order_id) as orders,
                       SUM(oi.line_total_aed) as revenue
                FROM amazon_order_items oi
                JOIN amazon_products p ON oi.product_id = p.product_id
                JOIN amazon_orders o ON oi.order_id = o.order_id
                GROUP BY p.category_lvl1
                ORDER BY revenue DESC
                """
                description = "Complex aggregation - category performance"
                
            else:  # Time Series Analysis
                query = """
                SELECT DATE(order_ts) as date,
                       COUNT(*) as orders,
                       AVG(total_aed) as avg_value
                FROM amazon_orders
                GROUP BY DATE(order_ts)
                ORDER BY date DESC
                LIMIT 10
                """
                description = "Time series aggregation - daily trends"
            
            # Execute and time the query
            start_time = time.time()
            try:
                df = pd.read_sql_query(query, conn)
                end_time = time.time()
                execution_time = (end_time - start_time) * 1000  # Convert to ms
                
                col1, col2 = st.columns(2)
                with col1:
                    st.success(f"✅ Query executed successfully")
                    st.metric("⏱️ Execution Time", f"{execution_time:.2f} ms")
                    st.metric("📊 Records Returned", len(df))
                
                with col2:
                    st.markdown(f"**Query:** {description}")
                    if len(df) > 0:
                        st.dataframe(df.head(), use_container_width=True)
                    
                # Performance categorization
                if execution_time < 10:
                    st.success("🟢 Excellent Performance (<10ms)")
                elif execution_time < 100:
                    st.info("🟡 Good Performance (<100ms)")
                else:
                    st.warning("🟠 Needs Optimization (>100ms)")
                    
            except Exception as e:
                st.error(f"Query failed: {e}")
        
        # Velocity simulation
        st.markdown("### 📈 Data Velocity Simulation")
        
        scenario = st.selectbox("Choose scenario:", [
            "Netflix Streaming Events",
            "NYSE Trade Ticks", 
            "Uber Ride Updates",
            "Amazon Order Flow"
        ])
        
        if scenario == "Netflix Streaming Events":
            events_per_second = st.slider("Events per second", 100, 10000, 1000)
            peak_multiplier = st.slider("Peak hour multiplier", 1.0, 10.0, 3.0)
            
        elif scenario == "NYSE Trade Ticks":
            events_per_second = st.slider("Trades per second", 1000, 100000, 10000)
            peak_multiplier = st.slider("Market open multiplier", 1.0, 5.0, 2.0)
            
        elif scenario == "Uber Ride Updates":
            events_per_second = st.slider("GPS updates per second", 50, 5000, 500)
            peak_multiplier = st.slider("Rush hour multiplier", 1.0, 8.0, 4.0)
            
        else:  # Amazon Order Flow
            events_per_second = st.slider("Orders per second", 10, 1000, 100)
            peak_multiplier = st.slider("Black Friday multiplier", 1.0, 20.0, 10.0)
        
        # Calculate metrics
        base_daily = events_per_second * 86400  # 24 hours
        peak_daily = base_daily * peak_multiplier
        monthly = base_daily * 30
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Daily Events (Normal)", f"{base_daily:,.0f}")
        with col2:
            st.metric("🔥 Daily Events (Peak)", f"{peak_daily:,.0f}")
        with col3:
            st.metric("📅 Monthly Events", f"{monthly:,.0f}")
        
        # Velocity impact assessment
        if events_per_second > 5000:
            st.error("🚨 High velocity! Requires specialized streaming architecture")
        elif events_per_second > 1000:
            st.warning("⚠️ Medium velocity. Consider stream processing")
        else:
            st.success("✅ Manageable velocity with standard batch processing")
        
        conn.close()
        
    except Exception as e:
        st.error(f"Error accessing database: {e}")

def show_data_variety():
    """Show data variety analysis"""
    st.subheader("🌟 Data Variety Analysis")
    
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        
        st.markdown("### 📋 Data Types & Structures")
        
        variety_examples = {
            "🔢 Structured Data": {
                "description": "Traditional relational data with fixed schema",
                "examples": [
                    "Customer records (ID, name, region)",
                    "Product catalog (SKU, price, category)",
                    "Order transactions (order_id, amount, timestamp)"
                ],
                "query": "SELECT customer_id, region, lifetime_value_aed FROM amazon_customers LIMIT 3"
            },
            "📊 Semi-structured Data": {
                "description": "Data with some organization but flexible schema",
                "examples": [
                    "JSON amenities in Airbnb properties", 
                    "Nested order items within orders",
                    "Variable-length arrays and objects"
                ],
                "query": "SELECT property_id, amenities_json FROM airbnb_properties WHERE amenities_json IS NOT NULL LIMIT 3"
            },
            "📈 Time-series Data": {
                "description": "Data points indexed by time with high dimensionality",
                "examples": [
                    "NYSE minute-level features (25+ dimensions)",
                    "Netflix viewing events with timestamps",
                    "Uber GPS coordinates over time"
                ],
                "query": "SELECT ticker, minute_timestamp, return_1m, volume_shares, realized_volatility_5m FROM nyse_features_minute LIMIT 3"
            },
            "🌍 Geospatial Data": {
                "description": "Location-based data with coordinates",
                "examples": [
                    "Uber pickup/dropoff coordinates",
                    "City-based property listings", 
                    "Regional customer distributions"
                ],
                "query": "SELECT ride_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng FROM uber_rides LIMIT 3"
            }
        }
        
        for variety_type, info in variety_examples.items():
            with st.expander(f"{variety_type}"):
                st.markdown(f"**{info['description']}**")
                
                st.markdown("**Examples:**")
                for example in info['examples']:
                    st.markdown(f"• {example}")
                
                if st.button(f"🔍 Show Sample Data", key=variety_type):
                    try:
                        df = pd.read_sql_query(info['query'], conn)
                        st.dataframe(df, use_container_width=True)
                        
                        # Special handling for JSON data
                        if 'amenities_json' in df.columns and not df.empty:
                            st.markdown("**Parsed JSON Example:**")
                            json_sample = df.iloc[0]['amenities_json']
                            if json_sample:
                                try:
                                    parsed = json.loads(json_sample)
                                    st.json(parsed)
                                except:
                                    st.text(json_sample)
                                    
                    except Exception as e:
                        st.error(f"Query error: {e}")
        
        # Data format analysis
        st.markdown("### 🎭 Data Format Diversity")
        
        format_metrics = [
            {"Format": "Decimal Precision", "Example": "NYSE prices (4 decimals)", "Use Case": "Financial accuracy"},
            {"Format": "Timestamps", "Example": "Millisecond precision", "Use Case": "High-frequency trading"},
            {"Format": "Boolean Flags", "Example": "instant_book, superhost_flag", "Use Case": "Feature flags"},
            {"Format": "Categorical", "Example": "subscription_plan, ride_status", "Use Case": "Classification"},
            {"Format": "Text/JSON", "Example": "Amenities, review comments", "Use Case": "Flexible attributes"},
            {"Format": "Coordinates", "Example": "Lat/Lng pairs", "Use Case": "Location services"}
        ]
        
        df_formats = pd.DataFrame(format_metrics)
        st.dataframe(df_formats, use_container_width=True)
        
        conn.close()
        
    except Exception as e:
        st.error(f"Error analyzing data variety: {e}")

def show_company_insights():
    """Show company-specific business insights"""
    st.subheader("🏢 Company-Specific Business Insights")
    
    company = st.selectbox("Select Company:", [
        "🛒 Amazon E-commerce",
        "🎬 Netflix Streaming", 
        "🚗 Uber Mobility",
        "🏠 Airbnb Marketplace",
        "📈 NYSE Market Data"
    ])
    
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        
        if company == "🛒 Amazon E-commerce":
            show_amazon_insights(conn)
        elif company == "🎬 Netflix Streaming":
            show_netflix_insights(conn)
        elif company == "🚗 Uber Mobility":
            show_uber_insights(conn)
        elif company == "🏠 Airbnb Marketplace":
            show_airbnb_insights(conn)
        else:  # NYSE
            show_nyse_insights(conn)
        
        conn.close()
        
    except Exception as e:
        st.error(f"Error loading company insights: {e}")

def show_amazon_insights(conn):
    """Show Amazon-specific insights"""
    st.markdown("### 🛒 Amazon E-commerce Analysis")
    
    # Regional performance
    regional_query = """
    SELECT c.region, 
           COUNT(DISTINCT c.customer_id) as customers,
           COUNT(DISTINCT o.order_id) as orders,
           AVG(o.total_aed) as avg_order_value,
           SUM(o.total_aed) as total_revenue
    FROM amazon_customers c
    LEFT JOIN amazon_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'completed'
    GROUP BY c.region
    ORDER BY total_revenue DESC
    """
    
    regional_df = pd.read_sql_query(regional_query, conn)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not regional_df.empty:
            fig = px.bar(regional_df, x='region', y='total_revenue',
                        title='💰 Revenue by Region',
                        color='total_revenue',
                        color_continuous_scale='blues')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if not regional_df.empty:
            fig = px.scatter(regional_df, x='customers', y='avg_order_value',
                           size='orders', hover_name='region',
                           title='📊 Customers vs AOV',
                           labels={'customers': 'Number of Customers',
                                  'avg_order_value': 'Avg Order Value (AED)'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Category performance
    category_query = """
    SELECT p.category_lvl1,
           COUNT(DISTINCT oi.order_id) as orders,
           SUM(oi.line_total_aed) as revenue,
           AVG(oi.line_total_aed) as avg_item_value
    FROM amazon_order_items oi
    JOIN amazon_products p ON oi.product_id = p.product_id
    JOIN amazon_orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'completed'
    GROUP BY p.category_lvl1
    ORDER BY revenue DESC
    """
    
    category_df = pd.read_sql_query(category_query, conn)
    
    if not category_df.empty:
        st.markdown("### 📦 Category Performance")
        
        fig = px.treemap(category_df, path=['category_lvl1'], values='revenue',
                        title='📦 Revenue by Category (Treemap)',
                        color='avg_item_value',
                        color_continuous_scale='viridis')
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(category_df, use_container_width=True)

def show_netflix_insights(conn):
    """Show Netflix-specific insights"""
    st.markdown("### 🎬 Netflix Streaming Analysis")
    
    # Content engagement analysis
    content_query = """
    SELECT c.genre_primary, c.content_type,
           COUNT(DISTINCT ve.event_id) as total_events,
           COUNT(DISTINCT ve.user_id) as unique_viewers,
           AVG(ve.watch_duration_sec) as avg_watch_time
    FROM netflix_content c
    JOIN netflix_viewing_events ve ON c.content_id = ve.content_id
    GROUP BY c.genre_primary, c.content_type
    ORDER BY total_events DESC
    """
    
    content_df = pd.read_sql_query(content_query, conn)
    
    if not content_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(content_df.head(8), x='genre_primary', y='total_events',
                        color='content_type',
                        title='🎭 Engagement by Genre',
                        labels={'total_events': 'Total Events'})
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            content_df['avg_watch_minutes'] = content_df['avg_watch_time'] / 60
            fig = px.scatter(content_df, x='unique_viewers', y='avg_watch_minutes',
                           size='total_events', color='content_type',
                           hover_name='genre_primary',
                           title='👥 Viewers vs Watch Time')
            st.plotly_chart(fig, use_container_width=True)
    
    # Device usage analysis
    device_query = """
    SELECT device_type,
           COUNT(*) as events,
           COUNT(DISTINCT user_id) as unique_users,
           AVG(watch_duration_sec) as avg_duration
    FROM netflix_viewing_events
    GROUP BY device_type
    ORDER BY events DESC
    """
    
    device_df = pd.read_sql_query(device_query, conn)
    
    if not device_df.empty:
        st.markdown("### 📱 Device Usage Analysis")
        
        fig = px.pie(device_df, values='events', names='device_type',
                    title='📱 Events by Device Type')
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(device_df, use_container_width=True)

def show_uber_insights(conn):
    """Show Uber-specific insights"""
    st.markdown("### 🚗 Uber Mobility Analysis")
    
    # Ride status analysis
    ride_status_query = """
    SELECT ride_status,
           COUNT(*) as rides,
           AVG(distance_km) as avg_distance,
           AVG(fare_aed) as avg_fare
    FROM uber_rides
    GROUP BY ride_status
    ORDER BY rides DESC
    """
    
    status_df = pd.read_sql_query(ride_status_query, conn)
    
    if not status_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(status_df, values='rides', names='ride_status',
                        title='🚗 Rides by Status')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(status_df, x='ride_status', y='avg_fare',
                        title='💰 Average Fare by Status',
                        color='avg_fare',
                        color_continuous_scale='greens')
            st.plotly_chart(fig, use_container_width=True)
    
    # Surge analysis
    surge_query = """
    SELECT 
        CASE 
            WHEN surge_multiplier = 1.0 THEN 'No Surge'
            WHEN surge_multiplier <= 1.5 THEN 'Low Surge'
            ELSE 'High Surge'
        END as surge_category,
        COUNT(*) as rides,
        AVG(fare_aed) as avg_fare,
        AVG(distance_km) as avg_distance
    FROM uber_rides
    GROUP BY surge_category
    ORDER BY rides DESC
    """
    
    surge_df = pd.read_sql_query(surge_query, conn)
    
    if not surge_df.empty:
        st.markdown("### ⚡ Surge Pricing Impact")
        
        fig = px.bar(surge_df, x='surge_category', y='avg_fare',
                    color='rides',
                    title='⚡ Surge Impact on Pricing',
                    labels={'avg_fare': 'Average Fare (AED)'})
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(surge_df, use_container_width=True)

def show_airbnb_insights(conn):
    """Show Airbnb-specific insights"""
    st.markdown("### 🏠 Airbnb Marketplace Analysis")
    
    # Property performance by city and type
    property_query = """
    SELECT p.city, p.property_type,
           COUNT(*) as properties,
           AVG(p.base_price_aed) as avg_price,
           COUNT(b.booking_id) as bookings
    FROM airbnb_properties p
    LEFT JOIN airbnb_bookings b ON p.property_id = b.property_id
    GROUP BY p.city, p.property_type
    ORDER BY properties DESC
    """
    
    property_df = pd.read_sql_query(property_query, conn)
    
    if not property_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            city_summary = property_df.groupby('city').agg({
                'properties': 'sum',
                'avg_price': 'mean'
            }).reset_index()
            
            fig = px.bar(city_summary, x='city', y='properties',
                        title='🏙️ Properties by City',
                        color='avg_price',
                        color_continuous_scale='viridis')
            fig.update_xaxis(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(property_df, x='properties', y='avg_price',
                           color='city', size='bookings',
                           hover_name='property_type',
                           title='💰 Properties vs Price by City')
            st.plotly_chart(fig, use_container_width=True)
    
    # Booking patterns
    booking_query = """
    SELECT booking_status,
           COUNT(*) as bookings,
           AVG(nights) as avg_nights,
           AVG(total_price_aed) as avg_total
    FROM airbnb_bookings
    GROUP BY booking_status
    ORDER BY bookings DESC
    """
    
    booking_df = pd.read_sql_query(booking_query, conn)
    
    if not booking_df.empty:
        st.markdown("### 📅 Booking Patterns")
        st.dataframe(booking_df, use_container_width=True)

def show_nyse_insights(conn):
    """Show NYSE-specific insights"""
    st.markdown("### 📈 NYSE Market Data Analysis")
    
    # Ticker performance analysis
    ticker_query = """
    SELECT ticker,
           COUNT(*) as data_points,
           AVG(return_1m) as avg_return,
           AVG(volume_shares) as avg_volume,
           AVG(realized_volatility_5m) as avg_volatility
    FROM nyse_features_minute
    GROUP BY ticker
    ORDER BY avg_volume DESC
    """
    
    ticker_df = pd.read_sql_query(ticker_query, conn)
    
    if not ticker_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            ticker_df['avg_return_bps'] = ticker_df['avg_return'] * 10000  # Convert to basis points
            fig = px.bar(ticker_df.head(10), x='ticker', y='avg_volume',
                        title='📊 Average Volume by Ticker',
                        color='avg_return_bps',
                        color_continuous_scale='RdBu',
                        color_continuous_midpoint=0)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            ticker_df['avg_volatility_pct'] = ticker_df['avg_volatility'] * 100
            fig = px.scatter(ticker_df, x='avg_return_bps', y='avg_volatility_pct',
                           size='avg_volume', hover_name='ticker',
                           title='📈 Risk vs Return Profile',
                           labels={'avg_return_bps': 'Average Return (bps)',
                                  'avg_volatility_pct': 'Volatility (%)'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Market microstructure
    microstructure_query = """
    SELECT 
        COUNT(*) as total_minutes,
        AVG(bid_ask_spread_bps) as avg_spread,
        AVG(order_flow_imbalance) as avg_flow_imbalance,
        AVG(buy_volume_ratio) as avg_buy_ratio
    FROM nyse_features_minute
    """
    
    micro_df = pd.read_sql_query(microstructure_query, conn)
    
    if not micro_df.empty:
        st.markdown("### 🏭 Market Microstructure")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Data Points", f"{micro_df.iloc[0]['total_minutes']:,.0f}")
        with col2:
            st.metric("💹 Avg Spread", f"{micro_df.iloc[0]['avg_spread']:.1f} bps")
        with col3:
            st.metric("⚖️ Flow Imbalance", f"{micro_df.iloc[0]['avg_flow_imbalance']:.3f}")
        with col4:
            st.metric("📈 Buy Ratio", f"{micro_df.iloc[0]['avg_buy_ratio']:.1%}")

def show_oltp_olap_comparison():
    """Show OLTP vs OLAP comparison with live demonstrations"""
    st.subheader("⚖️ OLTP vs OLAP Comparison")
    
    st.markdown("""
    **Interactive demonstration of transactional vs analytical query patterns**
    """)
    
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🔄 OLTP (Transactional)")
            st.markdown("""
            **Characteristics:**
            - Point lookups and simple joins
            - Low latency (<10ms)
            - High concurrency
            - Normalized data structure
            - Frequent small transactions
            """)
            
            st.markdown("**Sample OLTP Queries:**")
            
            oltp_queries = {
                "Customer Lookup": {
                    "query": "SELECT * FROM amazon_customers WHERE customer_id = 'CUST_000001'",
                    "description": "Find specific customer details"
                },
                "Order Details": {
                    "query": """
                    SELECT o.order_id, o.total_aed, c.region
                    FROM amazon_orders o
                    JOIN amazon_customers c ON o.customer_id = c.customer_id
                    WHERE o.order_id = 'ORDER_00000001'
                    """,
                    "description": "Get order with customer info"
                },
                "User Profile": {
                    "query": "SELECT * FROM netflix_users WHERE user_id = 'USER_000001'",
                    "description": "Retrieve user profile"
                }
            }
            
            for query_name, query_info in oltp_queries.items():
                if st.button(f"🚀 Run: {query_name}", key=f"oltp_{query_name}"):
                    start_time = time.time()
                    try:
                        df = pd.read_sql_query(query_info["query"], conn)
                        end_time = time.time()
                        execution_time = (end_time - start_time) * 1000
                        
                        st.success(f"✅ {execution_time:.2f} ms")
                        st.markdown(f"*{query_info['description']}*")
                        if not df.empty:
                            st.dataframe(df, use_container_width=True)
                        
                    except Exception as e:
                        st.error(f"Error: {e}")
        
        with col2:
            st.markdown("### 📊 OLAP (Analytical)")
            st.markdown("""
            **Characteristics:**
            - Complex aggregations and joins
            - Higher latency (10ms-1s+)
            - Read-optimized
            - Denormalized data structure
            - Large batch operations
            """)
            
            st.markdown("**Sample OLAP Queries:**")
            
            olap_queries = {
                "Revenue by Region": {
                    "query": """
                    SELECT c.region, 
                           COUNT(o.order_id) as orders,
                           SUM(o.total_aed) as revenue,
                           AVG(o.total_aed) as avg_order
                    FROM amazon_orders o
                    JOIN amazon_customers c ON o.customer_id = c.customer_id
                    WHERE o.order_status = 'completed'
                    GROUP BY c.region
                    ORDER BY revenue DESC
                    """,
                    "description": "Aggregate sales by geography"
                },
                "Genre Performance": {
                    "query": """
                    SELECT c.genre_primary,
                           COUNT(ve.event_id) as events,
                           COUNT(DISTINCT ve.user_id) as unique_users,
                           AVG(ve.watch_duration_sec) as avg_watch_time
                    FROM netflix_viewing_events ve
                    JOIN netflix_content c ON ve.content_id = c.content_id
                    GROUP BY c.genre_primary
                    ORDER BY events DESC
                    """,
                    "description": "Content engagement analysis"
                },
                "Market Summary": {
                    "query": """
                    SELECT ticker,
                           COUNT(*) as periods,
                           AVG(return_1m * 10000) as avg_return_bps,
                           AVG(volume_shares) as avg_volume
                    FROM nyse_features_minute
                    GROUP BY ticker
                    ORDER BY avg_volume DESC
                    LIMIT 5
                    """,
                    "description": "Market performance summary"
                }
            }
            
            for query_name, query_info in olap_queries.items():
                if st.button(f"📊 Run: {query_name}", key=f"olap_{query_name}"):
                    start_time = time.time()
                    try:
                        df = pd.read_sql_query(query_info["query"], conn)
                        end_time = time.time()
                        execution_time = (end_time - start_time) * 1000
                        
                        if execution_time < 10:
                            st.success(f"✅ {execution_time:.2f} ms - Excellent!")
                        elif execution_time < 100:
                            st.info(f"⏱️ {execution_time:.2f} ms - Good")
                        else:
                            st.warning(f"⏳ {execution_time:.2f} ms - Needs optimization")
                        
                        st.markdown(f"*{query_info['description']}*")
                        if not df.empty:
                            st.dataframe(df, use_container_width=True)
                            
                    except Exception as e:
                        st.error(f"Error: {e}")
        
        # Performance comparison summary
        st.markdown("### 📈 Performance Comparison Summary")
        
        comparison_data = {
            "Aspect": ["Query Type", "Typical Latency", "Data Access", "Optimization", "Use Case", "Concurrency"],
            "OLTP": ["Point lookups, Simple joins", "<10ms", "Row-oriented", "Indexes, Normalization", "Real-time transactions", "High (1000s users)"],
            "OLAP": ["Aggregations, Complex analytics", "10ms-1s+", "Column-oriented", "Denormalization, Partitioning", "Business intelligence", "Low-Medium (10s analysts)"]
        }
        
        comparison_df = pd.DataFrame(comparison_data)
        st.dataframe(comparison_df, use_container_width=True)
        
        conn.close()
        
    except Exception as e:
        st.error(f"Database connection error: {e}")

def show_scaling_strategies():
    """Show scaling strategies and recommendations"""
    st.subheader("📈 Scaling Strategies & Recommendations")
    
    scaling_approach = st.selectbox("Choose scaling strategy:", [
        "🔄 Horizontal vs Vertical Scaling",
        "🗂️ Data Partitioning Strategies", 
        "💾 Caching & Performance",
        "🏗️ Architecture Patterns",
        "📊 Performance Monitoring"
    ])
    
    if scaling_approach == "🔄 Horizontal vs Vertical Scaling":
        show_horizontal_vertical_scaling()
    elif scaling_approach == "🗂️ Data Partitioning Strategies":
        show_partitioning_strategies()
    elif scaling_approach == "💾 Caching & Performance":
        show_caching_strategies()
    elif scaling_approach == "🏗️ Architecture Patterns":
        show_architecture_patterns()
    else:
        show_performance_monitoring()

def show_horizontal_vertical_scaling():
    """Show horizontal vs vertical scaling comparison"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 Vertical Scaling (Scale Up)")
        st.markdown("""
        **Add more power to existing machines:**
        - Increase CPU cores
        - Add more RAM
        - Upgrade to faster SSDs
        - Higher network bandwidth
        
        **Pros:**
        - Simple to implement
        - No code changes required
        - Single machine simplicity
        
        **Cons:**
        - Hardware limits
        - Single point of failure
        - Expensive at scale
        """)
        
        # Vertical scaling simulator
        st.markdown("#### 🎮 Vertical Scaling Simulation")
        current_cpu = st.slider("Current CPU cores", 2, 64, 8)
        current_ram = st.slider("Current RAM (GB)", 4, 512, 32)
        
        # Calculate capacity
        capacity_score = (current_cpu * 2) + (current_ram / 4)
        
        st.metric("💪 System Capacity Score", f"{capacity_score:.0f}")
        
        if capacity_score > 100:
            st.success("🚀 High-performance system")
        elif capacity_score > 50:
            st.info("⚡ Good performance")
        else:
            st.warning("📈 Consider upgrading")
    
    with col2:
        st.markdown("### 📊 Horizontal Scaling (Scale Out)")
        st.markdown("""
        **Add more machines to the pool:**
        - Distribute load across nodes
        - Add commodity hardware
        - Use load balancing
        - Implement clustering
        
        **Pros:**
        - Nearly unlimited scaling
        - Fault tolerance
        - Cost-effective
        
        **Cons:**
        - Complex architecture
        - Network overhead
        - Data consistency challenges
        """)
        
        # Horizontal scaling simulator
        st.markdown("#### 🎮 Horizontal Scaling Simulation")
        num_nodes = st.slider("Number of nodes", 1, 50, 5)
        load_per_node = st.slider("Load per node (%)", 10, 100, 60)
        
        total_capacity = num_nodes * (100 - load_per_node)
        redundancy = num_nodes - 1  # Fault tolerance
        
        st.metric("🌐 Total Capacity", f"{total_capacity:.0f}%")
        st.metric("🛡️ Fault Tolerance", f"{redundancy} node failures")
        
        if num_nodes >= 10:
            st.success("🏭 Enterprise-scale system")
        elif num_nodes >= 3:
            st.info("🔗 Distributed system")
        else:
            st.warning("⚠️ Limited redundancy")

def show_partitioning_strategies():
    """Show data partitioning strategies"""
    st.markdown("### 🗂️ Data Partitioning Strategies")
    
    partition_type = st.selectbox("Select partitioning approach:", [
        "📅 Time-based Partitioning",
        "🌍 Geographic Partitioning", 
        "🔢 Hash Partitioning",
        "📋 Range Partitioning"
    ])
    
    if partition_type == "📅 Time-based Partitioning":
        st.markdown("""
        **Partition data by time periods:**
        
        **Example: Netflix Viewing Events**
        - Daily partitions: `viewing_events_2024_01_15`
        - Monthly archives: `viewing_events_2024_01`
        - Hot data: Last 7 days (fast SSD)
        - Cold data: >30 days (slower storage)
        
        **Benefits:**
        - Efficient time-range queries
        - Easy data lifecycle management
        - Parallel processing by time windows
        """)
        
        # Time partition simulation
        days_to_simulate = st.slider("Days of data", 30, 365, 90)
        partition_size = st.selectbox("Partition by:", ["Daily", "Weekly", "Monthly"])
        
        if partition_size == "Daily":
            num_partitions = days_to_simulate
            partition_unit = "days"
        elif partition_size == "Weekly":
            num_partitions = days_to_simulate // 7
            partition_unit = "weeks"
        else:  # Monthly
            num_partitions = days_to_simulate // 30
            partition_unit = "months"
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📂 Number of Partitions", num_partitions)
        with col2:
            st.metric("🗂️ Partition Unit", partition_unit)
    
    elif partition_type == "🌍 Geographic Partitioning":
        st.markdown("""
        **Partition data by geographic regions:**
        
        **Example: Amazon Orders**
        - UAE partition: High-performance local storage
        - KSA partition: Regional data center
        - Egypt partition: Shared infrastructure
        - Other MENA: Distributed across regions
        
        **Benefits:**
        - Data locality (reduced latency)
        - Regulatory compliance
        - Regional scaling
        """)
        
        # Show geographic distribution from data
        try:
            conn = sqlite3.connect('big_data_analytics.db')
            geo_query = """
            SELECT region, COUNT(*) as customers
            FROM amazon_customers
            GROUP BY region
            ORDER BY customers DESC
            """
            geo_df = pd.read_sql_query(geo_query, conn)
            
            if not geo_df.empty:
                fig = px.pie(geo_df, values='customers', names='region',
                           title='🌍 Geographic Distribution of Customers')
                st.plotly_chart(fig, use_container_width=True)
            
            conn.close()
        except:
            st.info("Database not available for geographic analysis")
    
    elif partition_type == "🔢 Hash Partitioning":
        st.markdown("""
        **Partition data using hash functions:**
        
        **Example: User Data Distribution**
        ```sql
        -- Partition by user_id hash
        PARTITION BY HASH(user_id) PARTITIONS 16
        ```
        
        **Benefits:**
        - Even data distribution
        - Good for parallel processing
        - No hot partitions
        
        **Use cases:**
        - User profiles
        - Session data
        - Large lookup tables
        """)
        
        # Hash partition simulation
        total_records = st.number_input("Total records", 1000, 10000000, 100000)
        num_hash_partitions = st.slider("Number of hash partitions", 2, 64, 16)
        
        records_per_partition = total_records // num_hash_partitions
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📊 Records per partition", f"{records_per_partition:,}")
        with col2:
            storage_per_partition = (records_per_partition * 1024) // (1024*1024)  # MB
            st.metric("💾 Storage per partition", f"{storage_per_partition} MB")
    
    else:  # Range Partitioning
        st.markdown("""
        **Partition data by value ranges:**
        
        **Example: NYSE Stock Prices**
        ```sql
        -- Partition by price ranges
        PARTITION p1 VALUES LESS THAN (100),    -- Penny stocks
        PARTITION p2 VALUES LESS THAN (500),    -- Mid-price
        PARTITION p3 VALUES LESS THAN (1000),   -- High-price
        PARTITION p4 VALUES LESS THAN MAXVALUE  -- Ultra high-price
        ```
        
        **Benefits:**
        - Intuitive data organization
        - Efficient range queries
        - Partition pruning optimization
        """)

def show_caching_strategies():
    """Show caching strategies for performance"""
    st.markdown("### 💾 Caching & Performance Optimization")
    
    cache_layer = st.selectbox("Select caching layer:", [
        "🔥 Application-level Cache",
        "🗄️ Database Query Cache",
        "🌐 CDN (Content Delivery Network)",
        "📊 Result Set Cache"
    ])
    
    if cache_layer == "🔥 Application-level Cache":
        st.markdown("""
        **In-memory caching for frequently accessed data:**
        
        **Technologies:**
        - Redis: Key-value store with persistence
        - Memcached: Simple distributed cache
        - In-process: Application memory cache
        
        **Use Cases:**
        - User sessions
        - Product catalogs
        - Configuration data
        """)
        
        # Cache simulation
        st.markdown("#### 🎯 Cache Performance Simulation")
        
        cache_hit_rate = st.slider("Cache hit rate (%)", 0, 100, 80)
        avg_db_latency = st.slider("Database latency (ms)", 10, 500, 100)
        avg_cache_latency = st.slider("Cache latency (ms)", 1, 20, 2)
        
        # Calculate effective latency
        effective_latency = (cache_hit_rate/100) * avg_cache_latency + \
                          (1 - cache_hit_rate/100) * avg_db_latency
        
        performance_improvement = ((avg_db_latency - effective_latency) / avg_db_latency) * 100
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("⚡ Effective Latency", f"{effective_latency:.1f} ms")
        with col2:
            st.metric("📈 Performance Gain", f"{performance_improvement:.0f}%")
        with col3:
            if cache_hit_rate > 80:
                st.success("🎯 Excellent cache efficiency")
            elif cache_hit_rate > 60:
                st.info("👍 Good cache performance")
            else:
                st.warning("📈 Consider cache optimization")
    
    elif cache_layer == "🗄️ Database Query Cache":
        st.markdown("""
        **Cache query results at database level:**
        
        **Strategies:**
        - Query result caching
        - Materialized views
        - Index optimization
        - Connection pooling
        
        **Example Cache Policy:**
        ```sql
        -- Cache expensive aggregations
        CREATE MATERIALIZED VIEW daily_sales AS
        SELECT date, region, SUM(revenue) 
        FROM orders 
        GROUP BY date, region;
        
        -- Refresh every hour
        REFRESH MATERIALIZED VIEW daily_sales;
        ```
        """)
    
    elif cache_layer == "🌐 CDN (Content Delivery Network)":
        st.markdown("""
        **Global content caching and delivery:**
        
        **Benefits:**
        - Reduced latency (geographic proximity)
        - Bandwidth offloading
        - DDoS protection
        - High availability
        
        **Use Cases:**
        - Static assets (images, CSS, JS)
        - API responses
        - Video streaming (Netflix model)
        - Product images (Amazon model)
        """)
        
        # CDN simulation
        st.markdown("#### 🌍 Global CDN Performance")
        
        user_locations = ["Dubai", "Riyadh", "Cairo", "London", "New York"]
        origin_server = "Dubai"
        
        cdn_data = []
        for location in user_locations:
            if location == origin_server:
                latency = 5  # Origin server
            elif location in ["Riyadh", "Cairo"]:
                latency = 25  # Regional CDN
            else:
                latency = 150  # Distant CDN
                
            cdn_data.append({
                "Location": location,
                "Latency_ms": latency,
                "Status": "Origin" if location == origin_server else "CDN Edge"
            })
        
        cdn_df = pd.DataFrame(cdn_data)
        
        fig = px.bar(cdn_df, x='Location', y='Latency_ms',
                    color='Status',
                    title='🌍 CDN Latency by Location')
        st.plotly_chart(fig, use_container_width=True)
    
    else:  # Result Set Cache
        st.markdown("""
        **Cache computed results and analytics:**
        
        **Implementation:**
        - Pre-computed dashboards
        - Scheduled report generation
        - Incremental updates
        - Cache invalidation policies
        
        **Example: Netflix Recommendations**
        ```python
        # Daily batch job
        def update_user_recommendations():
            for user in active_users:
                recommendations = ml_model.predict(user.profile)
                cache.set(f"recs:{user.id}", recommendations, ttl=24h)
        ```
        """)

def show_architecture_patterns():
    """Show scalable architecture patterns"""
    st.markdown("### 🏗️ Scalable Architecture Patterns")
    
    pattern = st.selectbox("Select architecture pattern:", [
        "🔄 Lambda Architecture",
        "🌊 Kappa Architecture", 
        "⚡ Event Sourcing",
        "📊 CQRS (Command Query Responsibility Segregation)"
    ])
    
    if pattern == "🔄 Lambda Architecture":
        st.markdown("""
        **Batch + Streaming processing for big data:**
        
        **Components:**
        1. **Batch Layer**: Historical data processing (Hadoop, Spark)
        2. **Speed Layer**: Real-time processing (Storm, Flink)
        3. **Serving Layer**: Query interface (HBase, Cassandra)
        
        **Example: Netflix Architecture**
        - Batch: Daily recommendation model training
        - Speed: Real-time view tracking and recommendations
        - Serving: Combined results for user interface
        """)
        
        # Lambda architecture diagram (text-based)
        st.markdown("""
        ```
        Data Sources → [Batch Layer] ────┐
                    ↓                    ├→ [Serving Layer] → Query Interface
                    [Speed Layer] ───────┘
        
        Batch Layer:  Hadoop/Spark (hours/days latency)
        Speed Layer:  Kafka/Flink (seconds latency) 
        Serving Layer: HBase/Redis (milliseconds latency)
        ```
        """)
    
    elif pattern == "🌊 Kappa Architecture":
        st.markdown("""
        **Stream-only processing architecture:**
        
        **Philosophy:** Everything is a stream
        - Single technology stack
        - Reprocessing by replaying streams
        - Simpler than Lambda architecture
        
        **Example: Uber Real-time Analytics**
        - All events (GPS, payments, ratings) as streams
        - Apache Kafka as central event store
        - Stream processors for all analytics
        """)
    
    elif pattern == "⚡ Event Sourcing":
        st.markdown("""
        **Store events, not current state:**
        
        **Principles:**
        - Immutable event log
        - State reconstruction from events
        - Complete audit trail
        - Time travel capabilities
        
        **Example: Amazon Order System**
        ```
        Events:
        - OrderCreated(order_id, items, customer)
        - PaymentProcessed(order_id, amount, method)
        - OrderShipped(order_id, carrier, tracking)
        - OrderDelivered(order_id, timestamp, signature)
        
        Current State = replay(all_events)
        ```
        """)
        
        # Event sourcing example
        st.markdown("#### 📋 Event Sourcing Example")
        
        # Simulate order events
        events = [
            {"timestamp": "2024-01-15 10:00:00", "event": "OrderCreated", "data": {"order_id": "ORD123", "amount": 250.0}},
            {"timestamp": "2024-01-15 10:05:00", "event": "PaymentProcessed", "data": {"order_id": "ORD123", "method": "credit_card"}},
            {"timestamp": "2024-01-15 14:00:00", "event": "OrderShipped", "data": {"order_id": "ORD123", "carrier": "DHL"}},
            {"timestamp": "2024-01-16 09:00:00", "event": "OrderDelivered", "data": {"order_id": "ORD123", "signature": "received"}},
        ]
        
        events_df = pd.DataFrame(events)
        st.dataframe(events_df, use_container_width=True)
    
    else:  # CQRS
        st.markdown("""
        **Separate read and write models:**
        
        **Components:**
        - **Command Side**: Handles writes/updates (normalized)
        - **Query Side**: Handles reads/queries (denormalized)
        - **Event Bus**: Synchronizes between sides
        
        **Benefits:**
        - Independent scaling
        - Optimized data models
        - Better performance
        
        **Example: E-commerce System**
        - Commands: CreateOrder, UpdateInventory, ProcessPayment
        - Queries: ProductCatalog, OrderHistory, RecommendationEngine
        """)

def show_performance_monitoring():
    """Show performance monitoring strategies"""
    st.markdown("### 📊 Performance Monitoring & Optimization")
    
    # Simulate performance metrics
    try:
        conn = sqlite3.connect('big_data_analytics.db')
        
        # Run sample performance tests
        st.markdown("#### ⚡ Live Performance Dashboard")
        
        # OLTP performance
        start_time = time.time()
        oltp_query = "SELECT * FROM amazon_customers LIMIT 10"
        oltp_df = pd.read_sql_query(oltp_query, conn)
        oltp_time = (time.time() - start_time) * 1000
        
        # OLAP performance  
        start_time = time.time()
        olap_query = """
        SELECT region, COUNT(*) as customers, AVG(lifetime_value_aed) as avg_ltv
        FROM amazon_customers 
        GROUP BY region
        """
        olap_df = pd.read_sql_query(olap_query, conn)
        olap_time = (time.time() - start_time) * 1000
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🔄 OLTP Query", f"{oltp_time:.2f} ms", 
                     "✅ Excellent" if oltp_time < 10 else "⚠️ Review")
        with col2:
            st.metric("📊 OLAP Query", f"{olap_time:.2f} ms",
                     "✅ Good" if olap_time < 100 else "⚠️ Optimize")
        with col3:
            table_count_query = "SELECT COUNT(name) FROM sqlite_master WHERE type='table'"
            table_count = pd.read_sql_query(table_count_query, conn).iloc[0, 0]
            st.metric("🗄️ Active Tables", table_count)
        with col4:
            # Estimate database size
            size_query = "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
            db_size = pd.read_sql_query(size_query, conn).iloc[0, 0] / (1024*1024)
            st.metric("💾 DB Size", f"{db_size:.1f} MB")
        
        # Performance trends (simulated)
        st.markdown("#### 📈 Performance Trends")
        
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        oltp_trend = np.random.normal(5, 2, 30)  # OLTP around 5ms
        olap_trend = np.random.normal(50, 15, 30)  # OLAP around 50ms
        
        trend_df = pd.DataFrame({
            'Date': dates,
            'OLTP_Latency': np.maximum(oltp_trend, 1),  # Minimum 1ms
            'OLAP_Latency': np.maximum(olap_trend, 10)  # Minimum 10ms
        })
        
        fig = px.line(trend_df, x='Date', y=['OLTP_Latency', 'OLAP_Latency'],
                     title='📊 Query Performance Trends',
                     labels={'value': 'Latency (ms)', 'variable': 'Query Type'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Performance recommendations
        st.markdown("#### 💡 Performance Optimization Recommendations")
        
        recommendations = [
            {"Priority": "High", "Area": "Indexing", "Action": "Add composite index on (customer_id, order_date)", "Impact": "50% faster OLTP queries"},
            {"Priority": "High", "Area": "Partitioning", "Action": "Partition orders table by date", "Impact": "Parallel query execution"},
            {"Priority": "Medium", "Area": "Caching", "Action": "Cache frequent category aggregations", "Impact": "80% cache hit rate"},
            {"Priority": "Medium", "Area": "Archiving", "Action": "Move orders >1 year to cold storage", "Impact": "Reduced active dataset size"},
            {"Priority": "Low", "Area": "Compression", "Action": "Enable table compression", "Impact": "30% storage reduction"}
        ]
        
        rec_df = pd.DataFrame(recommendations)
        st.dataframe(rec_df, use_container_width=True)
        
        conn.close()
        
    except Exception as e:
        st.error(f"Performance monitoring error: {e}")

def initialize_big_data_module():
    """Initialize the big data module by running the setup"""
    try:
        import subprocess
        import os
        
        # Change to the correct directory
        current_dir = os.getcwd()
        
        # Run the big data module setup
        result = subprocess.run([
            'python3', 'simple_big_data_module.py'
        ], capture_output=True, text=True, cwd=current_dir)
        
        if result.returncode == 0:
            return True
        else:
            st.error(f"Setup error: {result.stderr}")
            return False
            
    except Exception as e:
        st.error(f"Initialization error: {e}")
        return False

# Example of how to integrate this into your main app.py:
# Replace the existing show_big_data_scaling() function with enhanced_show_big_data_scaling()