"""
Analytics engine for detecting congestion and calculating traffic metrics.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging
from models import CongestionPoint
from database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrafficAnalytics:
    """Analytics engine for traffic data analysis."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize the analytics engine."""
        self.db_manager = db_manager
        
        # Congestion detection thresholds
        self.high_congestion_speed_threshold = 30  # km/h
        self.medium_congestion_speed_threshold = 50  # km/h
        self.min_vehicles_for_congestion = 5
        self.congestion_area_radius = 100  # meters
    
    def detect_congestion_points(self, hours: int = 1) -> List[CongestionPoint]:
        """Detect congestion points based on vehicle density and speed."""
        try:
            # Get recent traffic data
            traffic_data = self.db_manager.get_recent_traffic_data(hours)
            
            if traffic_data.empty:
                logger.warning("No traffic data available for congestion analysis")
                return []
            
            congestion_points = []
            
            # Group by route for analysis
            for route_id in traffic_data['route_id'].unique():
                route_data = traffic_data[traffic_data['route_id'] == route_id]
                
                if len(route_data) < self.min_vehicles_for_congestion:
                    continue
                
                # Analyze congestion in grid cells
                congestion_areas = self._analyze_route_congestion(route_data)
                congestion_points.extend(congestion_areas)
            
            # Store detected congestion points
            for point in congestion_points:
                try:
                    self.db_manager.insert_congestion_point(point)
                except Exception as e:
                    logger.error(f"Error storing congestion point: {e}")
            
            logger.info(f"Detected {len(congestion_points)} congestion points")
            return congestion_points
            
        except Exception as e:
            logger.error(f"Error detecting congestion points: {e}")
            return []
    
    def _analyze_route_congestion(self, route_data: pd.DataFrame) -> List[CongestionPoint]:
        """Analyze congestion for a specific route."""
        congestion_points = []
        route_id = route_data['route_id'].iloc[0]
        
        # Create spatial grid for analysis
        x_min, x_max = route_data['position_x'].min(), route_data['position_x'].max()
        y_min, y_max = route_data['position_y'].min(), route_data['position_y'].max()
        
        # Grid size based on congestion area radius
        grid_size = self.congestion_area_radius
        x_bins = np.arange(x_min, x_max + grid_size, grid_size)
        y_bins = np.arange(y_min, y_max + grid_size, grid_size)
        
        # Analyze each grid cell
        for i in range(len(x_bins) - 1):
            for j in range(len(y_bins) - 1):
                x_start, x_end = x_bins[i], x_bins[i + 1]
                y_start, y_end = y_bins[j], y_bins[j + 1]
                
                # Get vehicles in this grid cell
                cell_vehicles = route_data[
                    (route_data['position_x'] >= x_start) & 
                    (route_data['position_x'] < x_end) &
                    (route_data['position_y'] >= y_start) & 
                    (route_data['position_y'] < y_end)
                ]
                
                if len(cell_vehicles) >= self.min_vehicles_for_congestion:
                    avg_speed = cell_vehicles['speed'].mean()
                    vehicle_count = len(cell_vehicles)
                    
                    # Determine congestion level
                    congestion_level = self._classify_congestion(avg_speed, vehicle_count)
                    
                    if congestion_level != 'none':
                        congestion_point = CongestionPoint(
                            location_x=(x_start + x_end) / 2,
                            location_y=(y_start + y_end) / 2,
                            congestion_level=congestion_level,
                            average_speed=avg_speed,
                            vehicle_count=vehicle_count,
                            timestamp=datetime.now(),
                            route_id=route_id
                        )
                        congestion_points.append(congestion_point)
        
        return congestion_points
    
    def _classify_congestion(self, avg_speed: float, vehicle_count: int) -> str:
        """Classify congestion level based on speed and vehicle count."""
        if avg_speed <= self.high_congestion_speed_threshold and vehicle_count >= 10:
            return 'high'
        elif avg_speed <= self.medium_congestion_speed_threshold and vehicle_count >= 7:
            return 'medium'
        elif vehicle_count >= self.min_vehicles_for_congestion:
            return 'low'
        else:
            return 'none'
    
    def calculate_average_travel_times(self, hours: int = 24) -> pd.DataFrame:
        """Calculate average travel times for each route."""
        try:
            # Get traffic data for the specified period
            traffic_data = self.db_manager.get_recent_traffic_data(hours)
            
            if traffic_data.empty:
                logger.warning("No traffic data available for travel time analysis")
                return pd.DataFrame()
            
            # Calculate travel times by route
            travel_times = []
            
            for route_id in traffic_data['route_id'].unique():
                route_data = traffic_data[traffic_data['route_id'] == route_id]
                
                # Get route distance
                route_info = route_data.iloc[0]  # Get route metadata
                
                # Calculate travel time for each vehicle
                vehicle_travel_times = []
                
                for vehicle_id in route_data['vehicle_id'].unique():
                    vehicle_data = route_data[route_data['vehicle_id'] == vehicle_id].sort_values('timestamp')
                    
                    if len(vehicle_data) >= 2:
                        # Calculate average speed for this vehicle's journey
                        avg_speed = vehicle_data['speed'].mean()
                        
                        if avg_speed > 0:
                            # Estimate travel time based on route distance and average speed
                            # This is a simplified calculation
                            estimated_distance = self._estimate_route_distance(vehicle_data)
                            travel_time_hours = estimated_distance / avg_speed
                            travel_time_minutes = travel_time_hours * 60
                            vehicle_travel_times.append(travel_time_minutes)
                
                if vehicle_travel_times:
                    travel_times.append({
                        'route_id': route_id,
                        'route_name': route_info.get('route_name', f'Route {route_id}'),
                        'avg_travel_time_minutes': np.mean(vehicle_travel_times),
                        'min_travel_time_minutes': np.min(vehicle_travel_times),
                        'max_travel_time_minutes': np.max(vehicle_travel_times),
                        'std_travel_time_minutes': np.std(vehicle_travel_times),
                        'sample_size': len(vehicle_travel_times)
                    })
            
            return pd.DataFrame(travel_times)
            
        except Exception as e:
            logger.error(f"Error calculating travel times: {e}")
            return pd.DataFrame()
    
    def _estimate_route_distance(self, vehicle_data: pd.DataFrame) -> float:
        """Estimate distance traveled by a vehicle."""
        if len(vehicle_data) < 2:
            return 0
        
        total_distance = 0
        for i in range(1, len(vehicle_data)):
            prev_pos = vehicle_data.iloc[i-1]
            curr_pos = vehicle_data.iloc[i]
            
            # Calculate Euclidean distance
            dx = curr_pos['position_x'] - prev_pos['position_x']
            dy = curr_pos['position_y'] - prev_pos['position_y']
            distance = np.sqrt(dx**2 + dy**2) / 100  # Convert to km
            total_distance += distance
        
        return max(total_distance, 0.1)  # Minimum distance
    
    def get_speed_distribution_stats(self, hours: int = 24) -> Dict:
        """Get speed distribution statistics."""
        try:
            traffic_data = self.db_manager.get_recent_traffic_data(hours)
            
            if traffic_data.empty:
                return {}
            
            stats = {
                'overall': {
                    'mean_speed': traffic_data['speed'].mean(),
                    'median_speed': traffic_data['speed'].median(),
                    'std_speed': traffic_data['speed'].std(),
                    'min_speed': traffic_data['speed'].min(),
                    'max_speed': traffic_data['speed'].max()
                },
                'by_vehicle_type': {},
                'by_route': {}
            }
            
            # Stats by vehicle type (if available in joined data)
            if 'vehicle_type' in traffic_data.columns:
                for vtype in traffic_data['vehicle_type'].unique():
                    vtype_data = traffic_data[traffic_data['vehicle_type'] == vtype]
                    stats['by_vehicle_type'][vtype] = {
                        'mean_speed': vtype_data['speed'].mean(),
                        'count': len(vtype_data)
                    }
            
            # Stats by route
            for route_id in traffic_data['route_id'].unique():
                route_data = traffic_data[traffic_data['route_id'] == route_id]
                stats['by_route'][route_id] = {
                    'mean_speed': route_data['speed'].mean(),
                    'count': len(route_data),
                    'route_name': route_data.get('route_name', [''])[0] if 'route_name' in route_data.columns else route_id
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating speed distribution: {e}")
            return {}
    
    def generate_hourly_traffic_summary(self, hours: int = 24) -> pd.DataFrame:
        """Generate hourly traffic flow summary."""
        try:
            traffic_data = self.db_manager.get_recent_traffic_data(hours)
            
            if traffic_data.empty:
                return pd.DataFrame()
            
            # Convert timestamp to datetime if it's not already
            traffic_data['timestamp'] = pd.to_datetime(traffic_data['timestamp'])
            traffic_data['hour'] = traffic_data['timestamp'].dt.hour
            
            # Group by hour and calculate metrics
            hourly_summary = traffic_data.groupby('hour').agg({
                'speed': ['mean', 'std', 'count'],
                'vehicle_id': 'nunique'
            }).round(2)
            
            # Flatten column names
            hourly_summary.columns = ['avg_speed', 'speed_std', 'total_records', 'unique_vehicles']
            hourly_summary = hourly_summary.reset_index()
            
            return hourly_summary
            
        except Exception as e:
            logger.error(f"Error generating hourly summary: {e}")
            return pd.DataFrame()
    
    def run_full_analysis(self) -> Dict:
        """Run complete traffic analysis and return results."""
        logger.info("Running full traffic analysis")
        
        try:
            results = {
                'timestamp': datetime.now().isoformat(),
                'congestion_points': len(self.detect_congestion_points()),
                'travel_times': self.calculate_average_travel_times().to_dict('records') if not self.calculate_average_travel_times().empty else [],
                'speed_stats': self.get_speed_distribution_stats(),
                'hourly_summary': self.generate_hourly_traffic_summary().to_dict('records') if not self.generate_hourly_traffic_summary().empty else []
            }
            
            logger.info("Full traffic analysis completed")
            return results
            
        except Exception as e:
            logger.error(f"Error in full analysis: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
