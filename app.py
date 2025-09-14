"""
Streamlit dashboard for traffic simulation visualization and analytics.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import logging

from database import DatabaseManager
from simulation import TrafficSimulation
from analytics import TrafficAnalytics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure page
st.set_page_config(
    page_title="Traffic Simulation Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def initialize_system():
    """Initialize the traffic simulation system."""
    try:
        db_manager = DatabaseManager()
        simulation = TrafficSimulation(db_manager)
        analytics = TrafficAnalytics(db_manager)
        return db_manager, simulation, analytics
    except Exception as e:
        st.error(f"Failed to initialize system: {e}")
        logger.error(f"System initialization error: {e}")
        return None, None, None

def main():
    """Main dashboard application."""
    st.title("🚗 Traffic Simulation Dashboard")
    st.markdown("Real-time traffic simulation with congestion analytics and visualization")
    
    # Initialize system
    db_manager, simulation, analytics = initialize_system()
    
    if not all([db_manager, simulation, analytics]):
        st.error("System initialization failed. Please check your database connection.")
        st.stop()
    
    # Sidebar controls
    st.sidebar.header("Simulation Controls")
    
    # Simulation status
    status = simulation.get_simulation_status()
    
    if status['running']:
        st.sidebar.success("✅ Simulation Running")
        if st.sidebar.button("Stop Simulation"):
            simulation.stop_simulation()
            st.rerun()
    else:
        st.sidebar.info("⏸️ Simulation Stopped")
        if st.sidebar.button("Start Simulation"):
            simulation.start_simulation()
            st.rerun()
    
    # Display current status
    st.sidebar.subheader("Current Status")
    st.sidebar.metric("Active Vehicles", status['active_vehicles'])
    st.sidebar.metric("Total Routes", status['routes'])
    
    # Vehicle type breakdown
    if status['vehicle_types']:
        st.sidebar.subheader("Vehicle Types")
        for vtype, count in status['vehicle_types'].items():
            st.sidebar.text(f"{vtype.title()}: {count}")
    
    # Data generation controls
    st.sidebar.subheader("Data Management")
    
    if st.sidebar.button("Generate Sample Data"):
        with st.spinner("Generating sample data..."):
            simulation.generate_batch_data(num_vehicles=50, duration_minutes=120)
        st.sidebar.success("Sample data generated!")
        st.rerun()
    
    if st.sidebar.button("Clean Old Data"):
        with st.spinner("Cleaning old data..."):
            db_manager.cleanup_old_data(days=1)
        st.sidebar.success("Old data cleaned!")
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Real-time Overview", 
        "🚦 Traffic Flow", 
        "🔥 Congestion Analysis", 
        "📈 Speed Analytics", 
        "🛣️ Route Performance"
    ])
    
    with tab1:
        show_realtime_overview(db_manager, analytics)
    
    with tab2:
        show_traffic_flow(db_manager)
    
    with tab3:
        show_congestion_analysis(db_manager, analytics)
    
    with tab4:
        show_speed_analytics(db_manager, analytics)
    
    with tab5:
        show_route_performance(db_manager, analytics)

def show_realtime_overview(db_manager, analytics):
    """Display real-time overview dashboard."""
    st.header("Real-time Traffic Overview")
    
    # Time range selector
    col1, col2 = st.columns([3, 1])
    
    with col2:
        time_range = st.selectbox(
            "Time Range",
            ["Last Hour", "Last 6 Hours", "Last 24 Hours"],
            index=1
        )
        
        hours_map = {"Last Hour": 1, "Last 6 Hours": 6, "Last 24 Hours": 24}
        selected_hours = hours_map[time_range]
    
    # Get recent data
    traffic_data = db_manager.get_recent_traffic_data(selected_hours)
    
    if traffic_data.empty:
        st.warning("No traffic data available. Start the simulation or generate sample data.")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_records = len(traffic_data)
        st.metric("Total Data Points", f"{total_records:,}")
    
    with col2:
        unique_vehicles = traffic_data['vehicle_id'].nunique()
        st.metric("Unique Vehicles", unique_vehicles)
    
    with col3:
        avg_speed = traffic_data['speed'].mean()
        st.metric("Average Speed", f"{avg_speed:.1f} km/h")
    
    with col4:
        active_routes = traffic_data['route_id'].nunique()
        st.metric("Active Routes", active_routes)
    
    # Time series plot
    st.subheader("Traffic Activity Over Time")
    
    # Prepare time series data
    traffic_data['timestamp'] = pd.to_datetime(traffic_data['timestamp'])
    traffic_data['hour'] = traffic_data['timestamp'].dt.floor('H')
    
    hourly_data = traffic_data.groupby('hour').agg({
        'speed': 'mean',
        'vehicle_id': 'nunique'
    }).reset_index()
    
    if not hourly_data.empty:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # Speed over time
        ax1.plot(hourly_data['hour'], hourly_data['speed'], marker='o', linewidth=2)
        ax1.set_title('Average Speed Over Time')
        ax1.set_ylabel('Speed (km/h)')
        ax1.grid(True, alpha=0.3)
        
        # Vehicle count over time
        ax2.plot(hourly_data['hour'], hourly_data['vehicle_id'], marker='s', color='orange', linewidth=2)
        ax2.set_title('Unique Vehicles Per Hour')
        ax2.set_ylabel('Vehicle Count')
        ax2.set_xlabel('Time')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        st.pyplot(fig)
    
    # Live update
    if st.button("🔄 Refresh Data"):
        st.rerun()

def show_traffic_flow(db_manager):
    """Display traffic flow visualization."""
    st.header("Traffic Flow Analysis")
    
    # Get recent traffic data
    traffic_data = db_manager.get_recent_traffic_data(6)
    
    if traffic_data.empty:
        st.warning("No traffic data available for flow analysis.")
        return
    
    # Traffic flow heatmap
    st.subheader("Traffic Density Heatmap")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        heatmap_resolution = st.slider("Grid Resolution", 10, 50, 20)
    
    with col1:
        # Create position heatmap
        fig = px.density_heatmap(
            traffic_data, 
            x='position_x', 
            y='position_y',
            title="Vehicle Position Density",
            nbinsx=heatmap_resolution,
            nbinsy=heatmap_resolution
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # Route-wise traffic distribution
    st.subheader("Traffic Distribution by Route")
    
    route_counts = traffic_data.groupby(['route_id', 'route_name']).size().reset_index(name='vehicle_count')
    route_counts = route_counts.sort_values('vehicle_count', ascending=True)
    
    if not route_counts.empty:
        fig = px.bar(
            route_counts, 
            x='vehicle_count', 
            y='route_name',
            orientation='h',
            title="Vehicle Count by Route",
            labels={'vehicle_count': 'Number of Vehicles', 'route_name': 'Route'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Speed flow visualization
    st.subheader("Speed Flow Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Speed distribution
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.hist(traffic_data['speed'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        ax.set_xlabel('Speed (km/h)')
        ax.set_ylabel('Frequency')
        ax.set_title('Speed Distribution')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)
    
    with col2:
        # Average speed by route
        route_speeds = traffic_data.groupby('route_name')['speed'].mean().sort_values()
        
        fig, ax = plt.subplots(figsize=(8, 6))
        route_speeds.plot(kind='barh', ax=ax, color='coral')
        ax.set_xlabel('Average Speed (km/h)')
        ax.set_title('Average Speed by Route')
        plt.tight_layout()
        st.pyplot(fig)

def show_congestion_analysis(db_manager, analytics):
    """Display congestion analysis dashboard."""
    st.header("Congestion Analysis")
    
    # Run congestion detection
    col1, col2 = st.columns([3, 1])
    
    with col2:
        if st.button("🔍 Detect Congestion"):
            with st.spinner("Analyzing congestion points..."):
                congestion_points = analytics.detect_congestion_points(hours=2)
            st.success(f"Found {len(congestion_points)} congestion points")
    
    # Get congestion data
    congestion_data = db_manager.get_congestion_data(hours=6)
    
    if congestion_data.empty:
        st.info("No congestion data available. Click 'Detect Congestion' to analyze current traffic.")
        return
    
    # Congestion metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_congestion = len(congestion_data)
        st.metric("Congestion Points", total_congestion)
    
    with col2:
        high_congestion = len(congestion_data[congestion_data['congestion_level'] == 'high'])
        st.metric("High Congestion", high_congestion)
    
    with col3:
        avg_vehicles_in_congestion = congestion_data['vehicle_count'].mean()
        st.metric("Avg Vehicles in Congestion", f"{avg_vehicles_in_congestion:.1f}")
    
    # Congestion map
    st.subheader("Congestion Heatmap")
    
    if not congestion_data.empty:
        # Color mapping for congestion levels
        color_map = {'low': 'yellow', 'medium': 'orange', 'high': 'red'}
        congestion_data['color'] = congestion_data['congestion_level'].map(color_map)
        
        fig = px.scatter(
            congestion_data,
            x='location_x',
            y='location_y',
            size='vehicle_count',
            color='congestion_level',
            color_discrete_map=color_map,
            title="Congestion Points by Severity",
            hover_data=['route_name', 'average_speed', 'vehicle_count']
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # Congestion trends
    st.subheader("Congestion Trends")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Congestion level distribution
        congestion_counts = congestion_data['congestion_level'].value_counts()
        
        fig = px.pie(
            values=congestion_counts.values,
            names=congestion_counts.index,
            title="Congestion Level Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Average speed in congestion areas
        avg_speeds = congestion_data.groupby('congestion_level')['average_speed'].mean()
        
        fig = px.bar(
            x=avg_speeds.index,
            y=avg_speeds.values,
            title="Average Speed by Congestion Level",
            labels={'x': 'Congestion Level', 'y': 'Average Speed (km/h)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Congestion details table
    st.subheader("Congestion Details")
    
    display_data = congestion_data[['route_name', 'congestion_level', 'average_speed', 'vehicle_count', 'timestamp']].sort_values('timestamp', ascending=False)
    st.dataframe(display_data, use_container_width=True)

def show_speed_analytics(db_manager, analytics):
    """Display speed analytics dashboard."""
    st.header("Speed Analytics")
    
    # Get speed statistics
    speed_stats = analytics.get_speed_distribution_stats(hours=24)
    
    if not speed_stats:
        st.warning("No speed data available for analysis.")
        return
    
    # Overall speed metrics
    st.subheader("Speed Statistics")
    
    if 'overall' in speed_stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Mean Speed", f"{speed_stats['overall']['mean_speed']:.1f} km/h")
        
        with col2:
            st.metric("Median Speed", f"{speed_stats['overall']['median_speed']:.1f} km/h")
        
        with col3:
            st.metric("Speed Std Dev", f"{speed_stats['overall']['std_speed']:.1f} km/h")
        
        with col4:
            speed_range = speed_stats['overall']['max_speed'] - speed_stats['overall']['min_speed']
            st.metric("Speed Range", f"{speed_range:.1f} km/h")
    
    # Get traffic data for detailed analysis
    traffic_data = db_manager.get_recent_traffic_data(hours=24)
    
    if not traffic_data.empty:
        # Speed distribution plot
        st.subheader("Speed Distribution Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogram with KDE
            fig, ax = plt.subplots(figsize=(8, 6))
            
            # Plot histogram
            ax.hist(traffic_data['speed'], bins=40, alpha=0.6, density=True, color='skyblue', edgecolor='black')
            
            # Add KDE
            import scipy.stats as stats
            speed_data = traffic_data['speed'].dropna()
            if len(speed_data) > 10:
                kde = stats.gaussian_kde(speed_data)
                x_range = np.linspace(speed_data.min(), speed_data.max(), 100)
                ax.plot(x_range, kde(x_range), 'r-', linewidth=2, label='KDE')
                ax.legend()
            
            ax.set_xlabel('Speed (km/h)')
            ax.set_ylabel('Density')
            ax.set_title('Speed Distribution with KDE')
            ax.grid(True, alpha=0.3)
            st.pyplot(fig)
        
        with col2:
            # Box plot by route
            fig, ax = plt.subplots(figsize=(8, 6))
            
            # Limit to top routes for readability
            top_routes = traffic_data['route_name'].value_counts().head(8).index
            route_data = traffic_data[traffic_data['route_name'].isin(top_routes)]
            
            sns.boxplot(data=route_data, y='route_name', x='speed', ax=ax)
            ax.set_xlabel('Speed (km/h)')
            ax.set_ylabel('Route')
            ax.set_title('Speed Distribution by Route')
            plt.tight_layout()
            st.pyplot(fig)
        
        # Speed trends over time
        st.subheader("Speed Trends Over Time")
        
        # Prepare hourly data
        traffic_data['timestamp'] = pd.to_datetime(traffic_data['timestamp'])
        traffic_data['hour'] = traffic_data['timestamp'].dt.hour
        
        hourly_speeds = traffic_data.groupby('hour')['speed'].agg(['mean', 'std', 'min', 'max']).reset_index()
        
        if not hourly_speeds.empty:
            fig = go.Figure()
            
            # Add mean speed line
            fig.add_trace(go.Scatter(
                x=hourly_speeds['hour'],
                y=hourly_speeds['mean'],
                mode='lines+markers',
                name='Mean Speed',
                line=dict(width=3)
            ))
            
            # Add confidence band
            fig.add_trace(go.Scatter(
                x=hourly_speeds['hour'],
                y=hourly_speeds['mean'] + hourly_speeds['std'],
                mode='lines',
                name='Mean + Std',
                line=dict(width=0),
                showlegend=False
            ))
            
            fig.add_trace(go.Scatter(
                x=hourly_speeds['hour'],
                y=hourly_speeds['mean'] - hourly_speeds['std'],
                mode='lines',
                name='Mean - Std',
                line=dict(width=0),
                fill='tonexty',
                fillcolor='rgba(68, 68, 68, 0.2)',
                showlegend=False
            ))
            
            fig.update_layout(
                title='Average Speed by Hour of Day',
                xaxis_title='Hour of Day',
                yaxis_title='Speed (km/h)',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    # Speed by route analysis
    if 'by_route' in speed_stats and speed_stats['by_route']:
        st.subheader("Speed Performance by Route")
        
        route_data = []
        for route_id, stats in speed_stats['by_route'].items():
            route_data.append({
                'Route': stats.get('route_name', route_id),
                'Average Speed (km/h)': f"{stats['mean_speed']:.1f}",
                'Data Points': stats['count']
            })
        
        route_df = pd.DataFrame(route_data)
        st.dataframe(route_df, use_container_width=True)

def show_route_performance(db_manager, analytics):
    """Display route performance analytics."""
    st.header("Route Performance Analysis")
    
    # Calculate travel times
    travel_times = analytics.calculate_average_travel_times(hours=24)
    
    if travel_times.empty:
        st.warning("No travel time data available. Ensure vehicles have completed routes.")
        return
    
    # Travel time metrics
    st.subheader("Travel Time Statistics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        avg_travel_time = travel_times['avg_travel_time_minutes'].mean()
        st.metric("Overall Avg Travel Time", f"{avg_travel_time:.1f} min")
    
    with col2:
        fastest_route = travel_times.loc[travel_times['avg_travel_time_minutes'].idxmin(), 'route_name']
        st.metric("Fastest Route", fastest_route)
    
    with col3:
        slowest_route = travel_times.loc[travel_times['avg_travel_time_minutes'].idxmax(), 'route_name']
        st.metric("Slowest Route", slowest_route)
    
    # Travel time comparison
    st.subheader("Travel Time by Route")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Bar chart of average travel times
        fig = px.bar(
            travel_times,
            x='avg_travel_time_minutes',
            y='route_name',
            orientation='h',
            title="Average Travel Time by Route",
            labels={'avg_travel_time_minutes': 'Travel Time (minutes)', 'route_name': 'Route'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Travel time range (min-max)
        fig = go.Figure()
        
        for _, row in travel_times.iterrows():
            fig.add_trace(go.Scatter(
                x=[row['min_travel_time_minutes'], row['max_travel_time_minutes']],
                y=[row['route_name'], row['route_name']],
                mode='lines+markers',
                name=row['route_name'],
                showlegend=False,
                line=dict(width=4),
                marker=dict(size=8)
            ))
        
        fig.update_layout(
            title='Travel Time Range (Min-Max) by Route',
            xaxis_title='Travel Time (minutes)',
            yaxis_title='Route',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Route statistics table
    st.subheader("Detailed Route Statistics")
    
    # Get route statistics from database
    route_stats = db_manager.get_route_statistics()
    
    if not route_stats.empty:
        # Format the data for display
        display_stats = route_stats[['route_name', 'speed_limit', 'avg_speed', 'unique_vehicles', 'data_points']].copy()
        display_stats['speed_compliance'] = (display_stats['avg_speed'] / display_stats['speed_limit'] * 100).round(1)
        display_stats['avg_speed'] = display_stats['avg_speed'].round(1)
        
        display_stats.columns = [
            'Route Name', 'Speed Limit (km/h)', 'Avg Speed (km/h)', 
            'Unique Vehicles', 'Data Points', 'Speed Compliance (%)'
        ]
        
        st.dataframe(display_stats, use_container_width=True)
    
    # Performance insights
    st.subheader("Performance Insights")
    
    if not travel_times.empty and not route_stats.empty:
        # Merge data for analysis
        merged_data = travel_times.merge(route_stats, on='route_name', how='inner')
        
        if not merged_data.empty:
            # Speed vs Travel Time correlation
            fig = px.scatter(
                merged_data,
                x='avg_speed',
                y='avg_travel_time_minutes',
                size='unique_vehicles',
                hover_name='route_name',
                title='Speed vs Travel Time Correlation',
                labels={'avg_speed': 'Average Speed (km/h)', 'avg_travel_time_minutes': 'Travel Time (minutes)'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Calculate correlation
            if len(merged_data) > 3:
                correlation = merged_data['avg_speed'].corr(merged_data['avg_travel_time_minutes'])
                st.info(f"Speed-Travel Time Correlation: {correlation:.3f}")

if __name__ == "__main__":
    main()
