import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import sqlite3
from random import choice, randint

st.set_page_config(
    page_title="Data Architecture & Engineering Learning Hub",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
        "🧠 Data Science & Analytics"
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
    
    selected_module = st.sidebar.selectbox("Choose a module:", modules)
    selected_company = st.sidebar.selectbox("Choose a company case study:", ["Select a company..."] + companies)
    
    # Main content area
    if selected_module == "🏠 Home":
        show_home()
    elif selected_module == "📥 Data Ingestion":
        show_data_ingestion()
    elif selected_module == "💾 Data Storage":
        show_data_storage()
    elif selected_module == "🔄 ETL/ELT Pipelines":
        show_etl_pipelines()
    elif selected_module == "⚡ Processing Systems":
        show_processing_systems()
    elif selected_module == "📊 Big Data & Scaling":
        show_big_data_scaling()
    elif selected_module == "🔍 OLAP vs OLTP":
        show_olap_vs_oltp()
    elif selected_module == "🧠 Data Science & Analytics":
        show_data_science_analytics()
    
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
    st.header("📥 Data Ingestion")
    st.markdown("Learn about different methods of getting data into your systems")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📚 Theory", "🛠️ Interactive Demo", "📊 EDA Charts", "🔄 Flow Charts", "🏢 Real Examples"])
    
    with tab1:
        st.subheader("Types of Data Ingestion")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            ### 📦 Batch Ingestion
            - **When:** Scheduled intervals (hourly, daily)
            - **Volume:** Large datasets
            - **Latency:** Minutes to hours
            - **Tools:** Cron jobs, Airflow, AWS Batch
            
            **Use Cases:**
            - Daily sales reports
            - Historical data loading
            - ETL processes
            """)
            
        with col2:
            st.markdown("""
            ### ⚡ Real-time Ingestion
            - **When:** Continuous streaming
            - **Volume:** High-frequency events
            - **Latency:** Milliseconds to seconds
            - **Tools:** Kafka, Kinesis, Pulsar
            
            **Use Cases:**
            - Live user activity
            - Financial transactions
            - IoT sensor data
            """)
    
    with tab2:
        st.subheader("🛠️ Interactive Ingestion Simulation")
        
        ingestion_type = st.selectbox("Choose ingestion type:", ["Batch", "Real-time"])
        
        if ingestion_type == "Batch":
            st.markdown("### Batch Processing Simulation")
            batch_size = st.slider("Batch Size (records)", 100, 10000, 1000)
            interval = st.selectbox("Processing Interval", ["Every hour", "Every day", "Every week"])
            
            if st.button("Simulate Batch Ingestion"):
                # Simulate batch processing
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i in range(101):
                    progress_bar.progress(i)
                    status_text.text(f'Processing batch... {i}% complete')
                    
                st.success(f"✅ Successfully ingested {batch_size:,} records!")
                
                # Show sample data
                sample_data = pd.DataFrame({
                    'timestamp': pd.date_range('2024-01-01', periods=10, freq='h'),
                    'user_id': np.random.randint(1000, 9999, 10),
                    'event_type': np.random.choice(['purchase', 'view', 'click'], 10),
                    'value': np.round(np.random.uniform(10, 500, 10), 2)
                })
                st.dataframe(sample_data)
        
        elif ingestion_type == "Real-time":
            st.markdown("### Real-time Streaming Simulation")
            
            if st.button("Start Real-time Stream"):
                placeholder = st.empty()
                
                for i in range(20):
                    # Simulate real-time data
                    current_time = datetime.now() + timedelta(seconds=i)
                    event = {
                        'timestamp': current_time.strftime('%H:%M:%S'),
                        'user_id': np.random.randint(1000, 9999),
                        'event': np.random.choice(['login', 'purchase', 'logout']),
                        'value': round(np.random.uniform(1, 100), 2)
                    }
                    
                    with placeholder.container():
                        st.json(event)
                    
                    import time
                    time.sleep(0.5)
    
    with tab3:
        st.subheader("📊 Data Ingestion EDA Charts")
        
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
    
    with tab5:
        st.subheader("🏢 Real-World Examples")
        
        examples = {
            "Amazon": {
                "icon": "🛒",
                "batch": "Daily order reports, inventory updates",
                "realtime": "Real-time order tracking, recommendation updates",
                "tools": "Kinesis, DynamoDB Streams, Lambda"
            },
            "Netflix": {
                "icon": "🎬", 
                "batch": "Daily viewing analytics, content metadata",
                "realtime": "Play events, user interactions, A/B test data",
                "tools": "Kafka, Spark Streaming, S3"
            },
            "Uber": {
                "icon": "🚗",
                "batch": "Daily trip summaries, driver analytics", 
                "realtime": "GPS locations, ride requests, surge pricing",
                "tools": "Kafka, Flink, Cassandra"
            }
        }
        
        for company, details in examples.items():
            with st.expander(f"{details['icon']} {company}"):
                col1, col2, col3 = st.columns([1, 2, 2])
                
                # Display company logo if available
                logo_path = f"/home/gee_devops254/Downloads/Data Architecture Enginnering ingestion/Pictures/{company}.png"
                try:
                    with col1:
                        st.image(logo_path, width=80)
                except:
                    # Fallback to emoji if logo not found
                    try:
                        logo_path = f"/home/gee_devops254/Downloads/Data Architecture Enginnering ingestion/Pictures/{company}.jpg"
                        with col1:
                            st.image(logo_path, width=80)
                    except:
                        with col1:
                            st.markdown(f"## {details['icon']}")
                
                with col2:
                    st.markdown(f"**📦 Batch:** {details['batch']}")
                with col3:
                    st.markdown(f"**⚡ Real-time:** {details['realtime']}")
                st.markdown(f"**🛠️ Tools:** {details['tools']}")

def show_data_storage():
    st.header("💾 Data Storage")
    st.markdown("Explore different storage systems and their use cases")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📚 Storage Types", "🛠️ Interactive Comparison", "📊 Storage Analytics", "🔄 Storage Flow Charts", "🏢 Real Examples"])
    
    with tab1:
        st.subheader("Types of Data Storage Systems")
        
        storage_types = st.selectbox("Choose storage type to learn about:", 
            ["OLTP (Transactional)", "NoSQL", "Data Lakes", "Data Warehouses (OLAP)"])
        
        if storage_types == "OLTP (Transactional)":
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                ### 🏦 OLTP Systems
                **Online Transaction Processing**
                
                - **Purpose:** Handle real-time transactions
                - **Characteristics:** ACID compliance, normalized data
                - **Performance:** Fast writes, consistent reads
                - **Examples:** MySQL, PostgreSQL, Oracle
                
                **Use Cases:**
                - E-commerce transactions
                - Banking systems
                - User authentication
                - Inventory management
                """)
            with col2:
                # Sample OLTP schema
                oltp_data = pd.DataFrame({
                    'user_id': [1, 2, 3, 4],
                    'order_id': [1001, 1002, 1003, 1004],
                    'product_name': ['Laptop', 'Phone', 'Tablet', 'Watch'],
                    'price': [999.99, 699.99, 399.99, 299.99],
                    'timestamp': pd.date_range('2024-01-01', periods=4, freq='1H')
                })
                st.markdown("**Sample OLTP Data:**")
                st.dataframe(oltp_data, use_container_width=True)
                
        elif storage_types == "NoSQL":
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                ### 📊 NoSQL Systems
                **Not Only SQL**
                
                - **Purpose:** Handle unstructured/semi-structured data
                - **Characteristics:** Flexible schema, horizontal scaling
                - **Performance:** High availability, eventual consistency
                - **Types:** Document, Key-Value, Column, Graph
                
                **Use Cases:**
                - User profiles and preferences
                - Product catalogs
                - Real-time recommendations
                - IoT data storage
                """)
            with col2:
                nosql_types = st.selectbox("NoSQL Type:", 
                    ["Document (MongoDB)", "Key-Value (DynamoDB)", "Column (Cassandra)", "Graph (Neo4j)"])
                
                if nosql_types == "Document (MongoDB)":
                    st.json({
                        "_id": "user_123",
                        "name": "John Doe", 
                        "preferences": {
                            "categories": ["electronics", "books"],
                            "price_range": {"min": 10, "max": 500}
                        },
                        "purchase_history": [
                            {"item": "laptop", "date": "2024-01-01"},
                            {"item": "mouse", "date": "2024-01-15"}
                        ]
                    })
                    
        elif storage_types == "Data Lakes":
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                ### 🏊 Data Lakes
                **Raw Data Repository**
                
                - **Purpose:** Store raw data in native format
                - **Characteristics:** Schema-on-read, cost-effective
                - **Performance:** Optimized for analytics, not transactions  
                - **Examples:** AWS S3, Azure Data Lake, Google Cloud Storage
                
                **Use Cases:**
                - Data archiving
                - Machine learning datasets
                - Data science exploration
                - Backup and disaster recovery
                """)
            with col2:
                st.markdown("**Data Lake Structure:**")
                lake_structure = {
                    "Raw Zone": ["Logs", "JSON files", "CSV files", "Images"],
                    "Processed Zone": ["Cleaned data", "Aggregated data"],
                    "Curated Zone": ["Business-ready datasets", "ML features"]
                }
                for zone, contents in lake_structure.items():
                    st.markdown(f"**{zone}:**")
                    for item in contents:
                        st.markdown(f"  - {item}")
                        
        elif storage_types == "Data Warehouses (OLAP)":
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                ### 🏢 Data Warehouses (OLAP)
                **Online Analytical Processing**
                
                - **Purpose:** Optimized for complex analytical queries
                - **Characteristics:** Star/snowflake schema, aggregated data
                - **Performance:** Fast reads, optimized for reporting
                - **Examples:** Redshift, Snowflake, BigQuery
                
                **Use Cases:**
                - Business intelligence
                - Historical reporting
                - Data analytics
                - Executive dashboards
                """)
            with col2:
                # Sample warehouse data
                warehouse_data = pd.DataFrame({
                    'date': pd.date_range('2024-01-01', periods=5, freq='D'),
                    'total_sales': [15000, 18000, 22000, 19000, 25000],
                    'orders_count': [150, 180, 220, 190, 250],
                    'avg_order_value': [100, 100, 100, 100, 100]
                })
                st.markdown("**Sample Warehouse Data:**")
                fig = px.line(warehouse_data, x='date', y='total_sales', title='Daily Sales Trend')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("🛠️ Storage System Comparison")
        
        comparison_metric = st.selectbox("Compare by:", 
            ["Performance", "Scalability", "Consistency", "Use Cases"])
        
        if comparison_metric == "Performance":
            perf_data = pd.DataFrame({
                'System': ['OLTP', 'NoSQL', 'Data Lake', 'Data Warehouse'],
                'Read Speed': [9, 8, 6, 10],
                'Write Speed': [10, 9, 8, 4],
                'Query Complexity': [6, 5, 7, 10]
            })
            
            fig = px.bar(perf_data, x='System', y=['Read Speed', 'Write Speed', 'Query Complexity'],
                        title='Performance Comparison (1-10 scale)', barmode='group')
            st.plotly_chart(fig, use_container_width=True)
            
        elif comparison_metric == "Use Cases":
            use_cases = {
                'OLTP': ['E-commerce transactions', 'Banking', 'User management', 'Inventory tracking'],
                'NoSQL': ['User profiles', 'Product catalogs', 'IoT data', 'Content management'],
                'Data Lake': ['Data archiving', 'ML datasets', 'Raw data storage', 'Data science'],
                'Data Warehouse': ['Business intelligence', 'Reporting', 'Analytics', 'Dashboards']
            }
            
            for system, cases in use_cases.items():
                with st.expander(f"{system} Use Cases"):
                    for case in cases:
                        st.markdown(f"• {case}")
        
        # Interactive storage selector
        st.markdown("---")
        st.subheader("🎯 Which Storage Should You Choose?")
        
        col1, col2 = st.columns(2)
        with col1:
            data_type = st.selectbox("Data Type:", ["Structured", "Semi-structured", "Unstructured"])
            workload = st.selectbox("Workload:", ["High writes", "High reads", "Complex analytics"])
        with col2:
            consistency = st.selectbox("Consistency:", ["Strong", "Eventual", "Don't care"])
            scale = st.selectbox("Scale:", ["Small", "Medium", "Large"])
        
        if st.button("Get Recommendation"):
            if workload == "High writes" and consistency == "Strong":
                st.success("🏦 **Recommendation: OLTP** - Perfect for transactional systems")
            elif data_type == "Unstructured" or data_type == "Semi-structured":
                st.success("📊 **Recommendation: NoSQL** - Great for flexible schema needs")
            elif workload == "Complex analytics":
                st.success("🏢 **Recommendation: Data Warehouse** - Optimized for analytics")
            elif scale == "Large" and workload == "High reads":
                st.success("🏊 **Recommendation: Data Lake** - Cost-effective for large datasets")
    
    with tab3:
        st.subheader("📊 Storage System Analytics")
        
        # Generate sample storage metrics data
        np.random.seed(43)
        n_days = 30
        storage_data = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=n_days, freq='D'),
            'storage_used_tb': np.random.lognormal(mean=2, sigma=0.5, size=n_days).round(2),
            'read_iops': np.random.exponential(scale=1000, size=n_days).round(0),
            'write_iops': np.random.exponential(scale=500, size=n_days).round(0),
            'latency_ms': np.random.gamma(shape=2, scale=5, size=n_days).round(1),
            'availability': np.random.normal(loc=99.9, scale=0.1, size=n_days).round(3),
            'storage_type': np.random.choice(['SSD', 'HDD', 'NVMe'], n_days, p=[0.5, 0.3, 0.2])
        })
        
        # Add derived metrics
        storage_data['throughput_mbps'] = (storage_data['read_iops'] + storage_data['write_iops']) * 0.1
        storage_data['cost_per_gb'] = np.where(storage_data['storage_type'] == 'NVMe', 0.08, 
                                              np.where(storage_data['storage_type'] == 'SSD', 0.05, 0.02))
        
        chart_type = st.selectbox("Choose Analytics Chart:", 
            ["Performance Metrics", "Storage Utilization", "Cost Analysis", "Availability Trends", "IOPS Distribution"])
        
        if chart_type == "Performance Metrics":
            col1, col2 = st.columns(2)
            
            with col1:
                # Latency trends
                fig_perf1 = px.line(storage_data, x='date', y='latency_ms',
                                   title='Storage Latency Over Time',
                                   labels={'latency_ms': 'Latency (ms)', 'date': 'Date'})
                fig_perf1.update_layout(height=400)
                st.plotly_chart(fig_perf1, use_container_width=True)
                
            with col2:
                # Throughput by storage type
                fig_perf2 = px.box(storage_data, x='storage_type', y='throughput_mbps',
                                  title='Throughput by Storage Type',
                                  labels={'throughput_mbps': 'Throughput (MB/s)', 'storage_type': 'Storage Type'})
                fig_perf2.update_layout(height=400)
                st.plotly_chart(fig_perf2, use_container_width=True)
            
            # IOPS comparison
            fig_perf3 = go.Figure()
            fig_perf3.add_trace(go.Scatter(x=storage_data['date'], y=storage_data['read_iops'], 
                                         mode='lines', name='Read IOPS'))
            fig_perf3.add_trace(go.Scatter(x=storage_data['date'], y=storage_data['write_iops'], 
                                         mode='lines', name='Write IOPS'))
            fig_perf3.update_layout(title='Read vs Write IOPS Over Time', 
                                   xaxis_title='Date', yaxis_title='IOPS')
            st.plotly_chart(fig_perf3, use_container_width=True)
            
        elif chart_type == "Storage Utilization":
            col1, col2 = st.columns(2)
            
            with col1:
                # Storage growth histogram
                fig_util1 = px.histogram(storage_data, x='storage_used_tb', nbins=20,
                                        title='Storage Usage Distribution',
                                        labels={'storage_used_tb': 'Storage Used (TB)', 'count': 'Days'})
                fig_util1.update_layout(height=400)
                st.plotly_chart(fig_util1, use_container_width=True)
                
            with col2:
                # Storage type pie chart
                storage_type_counts = storage_data['storage_type'].value_counts()
                fig_util2 = px.pie(values=storage_type_counts.values, names=storage_type_counts.index,
                                  title='Storage Type Distribution')
                fig_util2.update_layout(height=400)
                st.plotly_chart(fig_util2, use_container_width=True)
            
            # Storage growth over time
            fig_util3 = px.line(storage_data, x='date', y='storage_used_tb',
                               title='Storage Usage Growth',
                               labels={'storage_used_tb': 'Storage Used (TB)', 'date': 'Date'})
            st.plotly_chart(fig_util3, use_container_width=True)
            
        elif chart_type == "Cost Analysis":
            # Calculate daily costs
            storage_data['daily_cost'] = storage_data['storage_used_tb'] * 1024 * storage_data['cost_per_gb']
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Cost by storage type
                cost_by_type = storage_data.groupby('storage_type')['daily_cost'].mean().sort_values(ascending=False)
                fig_cost1 = px.bar(x=cost_by_type.index, y=cost_by_type.values,
                                  title='Average Daily Cost by Storage Type',
                                  labels={'x': 'Storage Type', 'y': 'Daily Cost ($)'})
                fig_cost1.update_layout(height=400)
                st.plotly_chart(fig_cost1, use_container_width=True)
                
            with col2:
                # Cost efficiency (cost per GB vs performance)
                fig_cost2 = px.scatter(storage_data, x='cost_per_gb', y='throughput_mbps', 
                                      color='storage_type',
                                      title='Cost vs Performance',
                                      labels={'cost_per_gb': 'Cost per GB ($)', 'throughput_mbps': 'Throughput (MB/s)'})
                fig_cost2.update_layout(height=400)
                st.plotly_chart(fig_cost2, use_container_width=True)
            
            # Daily cost trend
            fig_cost3 = px.line(storage_data, x='date', y='daily_cost',
                               title='Daily Storage Costs',
                               labels={'daily_cost': 'Daily Cost ($)', 'date': 'Date'})
            st.plotly_chart(fig_cost3, use_container_width=True)
            
        elif chart_type == "Availability Trends":
            col1, col2 = st.columns(2)
            
            with col1:
                # Availability over time
                fig_avail1 = px.line(storage_data, x='date', y='availability',
                                    title='Storage Availability Over Time',
                                    labels={'availability': 'Availability (%)', 'date': 'Date'})
                fig_avail1.update_layout(height=400)
                st.plotly_chart(fig_avail1, use_container_width=True)
                
            with col2:
                # Availability distribution
                fig_avail2 = px.histogram(storage_data, x='availability', nbins=15,
                                         title='Availability Distribution',
                                         labels={'availability': 'Availability (%)', 'count': 'Days'})
                fig_avail2.update_layout(height=400)
                st.plotly_chart(fig_avail2, use_container_width=True)
            
            # SLA compliance (assuming SLA is 99.9%)
            sla_compliance = (storage_data['availability'] >= 99.9).mean() * 100
            st.metric("SLA Compliance (99.9%)", f"{sla_compliance:.1f}%", 
                     delta=f"{sla_compliance - 95:.1f}%" if sla_compliance >= 95 else f"{sla_compliance - 95:.1f}%")
            
        elif chart_type == "IOPS Distribution":
            col1, col2 = st.columns(2)
            
            with col1:
                # Read IOPS histogram
                fig_iops1 = px.histogram(storage_data, x='read_iops', nbins=20,
                                        title='Read IOPS Distribution',
                                        labels={'read_iops': 'Read IOPS', 'count': 'Days'})
                fig_iops1.update_layout(height=400)
                st.plotly_chart(fig_iops1, use_container_width=True)
                
            with col2:
                # Write IOPS histogram
                fig_iops2 = px.histogram(storage_data, x='write_iops', nbins=20,
                                        title='Write IOPS Distribution',
                                        labels={'write_iops': 'Write IOPS', 'count': 'Days'})
                fig_iops2.update_layout(height=400)
                st.plotly_chart(fig_iops2, use_container_width=True)
            
            # IOPS correlation with latency
            fig_iops3 = px.scatter(storage_data, x='read_iops', y='latency_ms', 
                                  size='write_iops', color='storage_type',
                                  title='IOPS vs Latency Correlation',
                                  labels={'read_iops': 'Read IOPS', 'latency_ms': 'Latency (ms)'})
            st.plotly_chart(fig_iops3, use_container_width=True)
    
    with tab4:
        st.subheader("🔄 Storage Architecture Flow Charts")
        
        flow_type = st.selectbox("Choose Storage Flow:", 
            ["OLTP Storage Flow", "Data Lake Architecture", "Data Warehouse Flow", "Hybrid Storage Strategy"])
        
        if flow_type == "OLTP Storage Flow":
            fig_oltp = go.Figure()
            
            nodes = {
                'Application': (1, 8),
                'Connection\nPool': (3, 8),
                'Primary\nDatabase': (5, 8),
                'Read\nReplica': (5, 6),
                'Backup\nStorage': (7, 8),
                'Monitoring': (7, 6),
                'Cache\nLayer': (3, 10),
                'Load\nBalancer': (1, 10)
            }
            
            for node, (x, y) in nodes.items():
                color = 'lightgreen' if 'Database' in node else 'orange' if 'Cache' in node else 'lightblue'
                fig_oltp.add_shape(type="rect", x0=x-0.6, y0=y-0.4, x1=x+0.6, y1=y+0.4,
                                  fillcolor=color, line=dict(color="black", width=2))
                fig_oltp.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=9))
            
            connections = [
                ('Load\nBalancer', 'Application'), ('Application', 'Cache\nLayer'),
                ('Application', 'Connection\nPool'), ('Connection\nPool', 'Primary\nDatabase'),
                ('Primary\nDatabase', 'Read\nReplica'), ('Primary\nDatabase', 'Backup\nStorage'),
                ('Primary\nDatabase', 'Monitoring'), ('Read\nReplica', 'Monitoring')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_oltp.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_oltp.update_layout(
                title="OLTP Storage Architecture Flow",
                xaxis=dict(range=[0, 8], showgrid=False, showticklabels=False),
                yaxis=dict(range=[5, 11], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_oltp, use_container_width=True)
            
            st.markdown("""
            **OLTP Storage Components:**
            - **Load Balancer**: Distributes incoming requests
            - **Cache Layer**: Fast access to frequently used data
            - **Connection Pool**: Manages database connections efficiently  
            - **Primary Database**: Handles all write operations
            - **Read Replica**: Offloads read queries for better performance
            - **Backup Storage**: Ensures data durability and recovery
            - **Monitoring**: Tracks performance and health metrics
            """)
            
        elif flow_type == "Data Lake Architecture":
            fig_lake = go.Figure()
            
            nodes = {
                'Raw Data\nSources': (1, 8),
                'Data\nIngestion': (3, 8),
                'Raw Data\nZone': (5, 9),
                'Processed\nZone': (7, 9),
                'Curated\nZone': (9, 9),
                'Metadata\nCatalog': (5, 7),
                'Data\nGovernance': (7, 7),
                'Analytics\nTools': (11, 8),
                'Security\nLayer': (5, 5)
            }
            
            for node, (x, y) in nodes.items():
                if 'Zone' in node:
                    color = 'lightgreen'
                elif 'Security' in node or 'Governance' in node:
                    color = 'lightcoral'
                elif 'Catalog' in node:
                    color = 'orange'
                else:
                    color = 'lightblue'
                    
                fig_lake.add_shape(type="rect", x0=x-0.7, y0=y-0.4, x1=x+0.7, y1=y+0.4,
                                  fillcolor=color, line=dict(color="black", width=2))
                fig_lake.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Raw Data\nSources', 'Data\nIngestion'), ('Data\nIngestion', 'Raw Data\nZone'),
                ('Raw Data\nZone', 'Processed\nZone'), ('Processed\nZone', 'Curated\nZone'),
                ('Raw Data\nZone', 'Metadata\nCatalog'), ('Processed\nZone', 'Data\nGovernance'),
                ('Curated\nZone', 'Analytics\nTools'), ('Security\nLayer', 'Raw Data\nZone'),
                ('Security\nLayer', 'Processed\nZone'), ('Security\nLayer', 'Curated\nZone')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_lake.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_lake.update_layout(
                title="Data Lake Architecture Flow",
                xaxis=dict(range=[0, 12], showgrid=False, showticklabels=False),
                yaxis=dict(range=[4, 10], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_lake, use_container_width=True)
            
            st.markdown("""
            **Data Lake Architecture:**
            - **Raw Data Zone**: Stores data in its original format
            - **Processed Zone**: Cleaned and validated data
            - **Curated Zone**: Business-ready, transformed data
            - **Metadata Catalog**: Tracks data lineage and schema
            - **Data Governance**: Ensures data quality and compliance
            - **Security Layer**: Controls access across all zones
            """)
            
        elif flow_type == "Data Warehouse Flow":
            fig_warehouse = go.Figure()
            
            nodes = {
                'OLTP\nSystems': (1, 9),
                'External\nData': (1, 7),
                'ETL\nProcess': (3, 8),
                'Staging\nArea': (5, 8),
                'Data\nWarehouse': (7, 8),
                'Data\nMarts': (9, 9),
                'OLAP\nCubes': (9, 7),
                'BI Tools': (11, 8),
                'Reports': (13, 9),
                'Dashboards': (13, 7)
            }
            
            for node, (x, y) in nodes.items():
                if 'Warehouse' in node or 'Marts' in node:
                    color = 'lightgreen'
                elif 'ETL' in node:
                    color = 'orange'
                elif 'BI' in node or 'Reports' in node or 'Dashboards' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_warehouse.add_shape(type="rect", x0=x-0.6, y0=y-0.4, x1=x+0.6, y1=y+0.4,
                                       fillcolor=color, line=dict(color="black", width=2))
                fig_warehouse.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('OLTP\nSystems', 'ETL\nProcess'), ('External\nData', 'ETL\nProcess'),
                ('ETL\nProcess', 'Staging\nArea'), ('Staging\nArea', 'Data\nWarehouse'),
                ('Data\nWarehouse', 'Data\nMarts'), ('Data\nWarehouse', 'OLAP\nCubes'),
                ('Data\nMarts', 'BI Tools'), ('OLAP\nCubes', 'BI Tools'),
                ('BI Tools', 'Reports'), ('BI Tools', 'Dashboards')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_warehouse.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_warehouse.update_layout(
                title="Data Warehouse Architecture Flow",
                xaxis=dict(range=[0, 14], showgrid=False, showticklabels=False),
                yaxis=dict(range=[6, 10], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_warehouse, use_container_width=True)
            
            st.markdown("""
            **Data Warehouse Flow:**
            - **ETL Process**: Extract, Transform, Load operations
            - **Staging Area**: Temporary storage for data transformation
            - **Data Warehouse**: Central repository for integrated data
            - **Data Marts**: Subject-specific subsets for departments
            - **OLAP Cubes**: Multidimensional data for fast analysis
            - **BI Tools**: Business intelligence and reporting tools
            """)
            
        elif flow_type == "Hybrid Storage Strategy":
            fig_hybrid_storage = go.Figure()
            
            nodes = {
                'Operational\nApps': (1, 9),
                'Real-time\nStreams': (1, 7),
                'Batch\nSources': (1, 5),
                'OLTP\nDatabase': (3, 9),
                'Stream\nStorage': (3, 7),
                'Object\nStorage': (3, 5),
                'Data\nLake': (5, 6),
                'Data\nWarehouse': (7, 8),
                'Cache\nLayer': (7, 6),
                'Analytics': (9, 8),
                'ML Models': (9, 6),
                'Dashboards': (11, 7)
            }
            
            for node, (x, y) in nodes.items():
                if 'Database' in node or 'Lake' in node or 'Warehouse' in node:
                    color = 'lightgreen'
                elif 'Cache' in node:
                    color = 'orange'
                elif 'Analytics' in node or 'ML' in node or 'Dashboards' in node:
                    color = 'lightcoral'
                else:
                    color = 'lightblue'
                    
                fig_hybrid_storage.add_shape(type="rect", x0=x-0.6, y0=y-0.4, x1=x+0.6, y1=y+0.4,
                                            fillcolor=color, line=dict(color="black", width=2))
                fig_hybrid_storage.add_annotation(x=x, y=y, text=node, showarrow=False, font=dict(size=8))
            
            connections = [
                ('Operational\nApps', 'OLTP\nDatabase'), ('Real-time\nStreams', 'Stream\nStorage'),
                ('Batch\nSources', 'Object\nStorage'), ('OLTP\nDatabase', 'Data\nLake'),
                ('Stream\nStorage', 'Data\nLake'), ('Object\nStorage', 'Data\nLake'),
                ('Data\nLake', 'Data\nWarehouse'), ('Data\nWarehouse', 'Cache\nLayer'),
                ('Data\nWarehouse', 'Analytics'), ('Data\nLake', 'ML Models'),
                ('Analytics', 'Dashboards'), ('ML Models', 'Dashboards')
            ]
            
            for start, end in connections:
                x0, y0 = nodes[start]
                x1, y1 = nodes[end]
                fig_hybrid_storage.add_annotation(ax=x0, ay=y0, x=x1, y=y1, arrowhead=2, arrowsize=1, arrowwidth=2)
            
            fig_hybrid_storage.update_layout(
                title="Hybrid Storage Strategy Flow",
                xaxis=dict(range=[0, 12], showgrid=False, showticklabels=False),
                yaxis=dict(range=[4, 10], showgrid=False, showticklabels=False),
                height=500,
                showlegend=False
            )
            st.plotly_chart(fig_hybrid_storage, use_container_width=True)
            
            st.markdown("""
            **Hybrid Storage Strategy:**
            - **Multiple Storage Types**: OLTP, Stream, Object storage for different needs
            - **Central Data Lake**: Unified repository for all data types
            - **Specialized Processing**: Data warehouse for OLAP, cache for performance
            - **Diverse Analytics**: Traditional BI and modern ML capabilities
            - **Unified Access**: Single dashboard for all insights
            """)
    
    with tab5:
        st.subheader("🏢 Real-World Storage Examples")
        
        companies_storage = {
            "Netflix": {
                "icon": "🎬",
                "oltp": "User accounts, billing (MySQL)",
                "nosql": "Movie metadata, user preferences (Cassandra)", 
                "lake": "Viewing logs, A/B test data (S3)",
                "warehouse": "Business analytics, content performance (Redshift)"
            },
            "Airbnb": {
                "icon": "🏠", 
                "oltp": "Bookings, payments (MySQL)",
                "nosql": "Property listings, search data (MongoDB)",
                "lake": "Raw logs, ML training data (S3)", 
                "warehouse": "Revenue analytics, host insights (Hive/Presto)"
            },
            "NYSE": {
                "icon": "💰",
                "oltp": "Trade execution, order management (In-memory DBs)",
                "nosql": "Market data feeds (Time-series DBs)",
                "lake": "Historical trade data (HDFS)",
                "warehouse": "Regulatory reporting, analytics (Teradata)"
            }
        }
        
        for company, storage in companies_storage.items():
            with st.expander(f"{storage['icon']} {company} Storage Architecture"):
                col1, col2, col3 = st.columns([1, 2, 2])
                
                # Display company logo if available
                logo_path = f"/home/gee_devops254/Downloads/Data Architecture Enginnering ingestion/Pictures/{company}.png"
                try:
                    with col1:
                        st.image(logo_path, width=80)
                except:
                    # Check for NYSE alternative naming
                    if company == "NYSE":
                        try:
                            logo_path = "/home/gee_devops254/Downloads/Data Architecture Enginnering ingestion/Pictures/nyse-new-york-stock-exchange.png"
                            with col1:
                                st.image(logo_path, width=80)
                        except:
                            with col1:
                                st.markdown(f"## {storage['icon']}")
                    else:
                        # Fallback to emoji if logo not found
                        try:
                            logo_path = f"/home/gee_devops254/Downloads/Data Architecture Enginnering ingestion/Pictures/{company}.jpg"
                            with col1:
                                st.image(logo_path, width=80)
                        except:
                            with col1:
                                st.markdown(f"## {storage['icon']}")
                
                with col2:
                    st.markdown(f"**🏦 OLTP:** {storage['oltp']}")
                    st.markdown(f"**📊 NoSQL:** {storage['nosql']}")
                with col3:
                    st.markdown(f"**🏊 Data Lake:** {storage['lake']}")
                    st.markdown(f"**🏢 Data Warehouse:** {storage['warehouse']}")

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

if __name__ == "__main__":
    main()