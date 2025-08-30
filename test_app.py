import streamlit as st
import pandas as pd
import numpy as np
import sqlite3

st.set_page_config(
    page_title="Test Data Engineering App",
    page_icon="🛠️",
    layout="wide"
)

st.title("🛠️ Test Data Engineering App")

try:
    st.write("Testing basic functionality...")
    
    # Test database connection
    conn = sqlite3.connect(':memory:')
    st.success("✅ Database connection works")
    
    # Test data creation
    df = pd.DataFrame({
        'test': [1, 2, 3],
        'data': ['a', 'b', 'c']
    })
    st.dataframe(df)
    st.success("✅ DataFrame display works")
    
    # Test charts
    chart_data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['a', 'b', 'c']
    )
    st.line_chart(chart_data)
    st.success("✅ Charts work")
    
    st.balloons()
    st.success("✅ All basic tests passed!")
    
except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    import traceback
    st.code(traceback.format_exc())