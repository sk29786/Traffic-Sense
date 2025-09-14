"""
Database operations for the traffic simulation system.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta
import logging
from models import Vehicle, Route, CongestionPoint

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages PostgreSQL database operations for traffic simulation."""
    
    def __init__(self):
        """Initialize database connection using environment variables."""
        self.connection_params = {
            'host': os.getenv('PGHOST', 'localhost'),
            'database': os.getenv('PGDATABASE', 'traffic_db'),
            'user': os.getenv('PGUSER', 'postgres'),
            'password': os.getenv('PGPASSWORD', 'password'),
            'port': os.getenv('PGPORT', '5432')
        }
        
        # Alternative: use DATABASE_URL if provided
        self.database_url = os.getenv('DATABASE_URL')
        
        self._create_tables()
    
    def get_connection(self):
        """Get a database connection."""
        try:
            if self.database_url:
                conn = psycopg2.connect(self.database_url)
            else:
                conn = psycopg2.connect(**self.connection_params)
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _create_tables(self):
        """Create necessary tables if they don't exist."""
        create_tables_sql = """
        -- Vehicles table
        CREATE TABLE IF NOT EXISTS vehicles (
            id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) UNIQUE NOT NULL,
            vehicle_type VARCHAR(20) NOT NULL,
            current_speed FLOAT NOT NULL,
            max_speed FLOAT NOT NULL,
            route_id VARCHAR(50) NOT NULL,
            position_x FLOAT NOT NULL,
            position_y FLOAT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Routes table
        CREATE TABLE IF NOT EXISTS routes (
            id SERIAL PRIMARY KEY,
            route_id VARCHAR(50) UNIQUE NOT NULL,
            start_x FLOAT NOT NULL,
            start_y FLOAT NOT NULL,
            end_x FLOAT NOT NULL,
            end_y FLOAT NOT NULL,
            distance_km FLOAT NOT NULL,
            route_name VARCHAR(100) NOT NULL,
            speed_limit FLOAT NOT NULL
        );
        
        -- Traffic data table (time-series data)
        CREATE TABLE IF NOT EXISTS traffic_data (
            id SERIAL PRIMARY KEY,
            vehicle_id VARCHAR(50) NOT NULL,
            route_id VARCHAR(50) NOT NULL,
            speed FLOAT NOT NULL,
            position_x FLOAT NOT NULL,
            position_y FLOAT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (route_id) REFERENCES routes(route_id)
        );
        
        -- Congestion points table
        CREATE TABLE IF NOT EXISTS congestion_points (
            id SERIAL PRIMARY KEY,
            route_id VARCHAR(50) NOT NULL,
            location_x FLOAT NOT NULL,
            location_y FLOAT NOT NULL,
            congestion_level VARCHAR(10) NOT NULL,
            average_speed FLOAT NOT NULL,
            vehicle_count INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_traffic_data_timestamp ON traffic_data(timestamp);
        CREATE INDEX IF NOT EXISTS idx_traffic_data_route ON traffic_data(route_id);
        CREATE INDEX IF NOT EXISTS idx_vehicles_route ON vehicles(route_id);
        CREATE INDEX IF NOT EXISTS idx_congestion_timestamp ON congestion_points(timestamp);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_tables_sql)
                    conn.commit()
                    logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def insert_vehicle(self, vehicle: Vehicle):
        """Insert or update vehicle data."""
        sql = """
        INSERT INTO vehicles (vehicle_id, vehicle_type, current_speed, max_speed, 
                             route_id, position_x, position_y, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vehicle_id) DO UPDATE SET
            current_speed = EXCLUDED.current_speed,
            position_x = EXCLUDED.position_x,
            position_y = EXCLUDED.position_y,
            timestamp = EXCLUDED.timestamp
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (
                        vehicle.vehicle_id, vehicle.vehicle_type, vehicle.current_speed,
                        vehicle.max_speed, vehicle.route_id, vehicle.position_x,
                        vehicle.position_y, vehicle.timestamp
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error inserting vehicle: {e}")
            raise
    
    def insert_traffic_data(self, vehicle: Vehicle):
        """Insert traffic data point."""
        sql = """
        INSERT INTO traffic_data (vehicle_id, route_id, speed, position_x, position_y, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (
                        vehicle.vehicle_id, vehicle.route_id, vehicle.current_speed,
                        vehicle.position_x, vehicle.position_y, vehicle.timestamp
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error inserting traffic data: {e}")
            raise
    
    def insert_routes(self, routes: List[Route]):
        """Insert route data."""
        sql = """
        INSERT INTO routes (route_id, start_x, start_y, end_x, end_y, 
                           distance_km, route_name, speed_limit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (route_id) DO NOTHING
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for route in routes:
                        cursor.execute(sql, (
                            route.route_id, route.start_point[0], route.start_point[1],
                            route.end_point[0], route.end_point[1], route.distance_km,
                            route.route_name, route.speed_limit
                        ))
                    conn.commit()
                    logger.info(f"Inserted {len(routes)} routes")
        except Exception as e:
            logger.error(f"Error inserting routes: {e}")
            raise
    
    def insert_congestion_point(self, congestion: CongestionPoint):
        """Insert congestion point data."""
        sql = """
        INSERT INTO congestion_points (route_id, location_x, location_y, 
                                     congestion_level, average_speed, vehicle_count, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (
                        congestion.route_id, congestion.location_x, congestion.location_y,
                        congestion.congestion_level, congestion.average_speed,
                        congestion.vehicle_count, congestion.timestamp
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error inserting congestion point: {e}")
            raise
    
    def get_recent_traffic_data(self, hours: int = 1) -> pd.DataFrame:
        """Get recent traffic data as DataFrame."""
        sql = """
        SELECT td.*, r.route_name, r.speed_limit
        FROM traffic_data td
        JOIN routes r ON td.route_id = r.route_id
        WHERE td.timestamp >= %s
        ORDER BY td.timestamp DESC
        """
        
        try:
            with self.get_connection() as conn:
                cutoff_time = datetime.now() - timedelta(hours=hours)
                df = pd.read_sql_query(sql, conn, params=[cutoff_time])
                return df
        except Exception as e:
            logger.error(f"Error fetching traffic data: {e}")
            return pd.DataFrame()
    
    def get_congestion_data(self, hours: int = 1) -> pd.DataFrame:
        """Get congestion data as DataFrame."""
        sql = """
        SELECT cp.*, r.route_name
        FROM congestion_points cp
        JOIN routes r ON cp.route_id = r.route_id
        WHERE cp.timestamp >= %s
        ORDER BY cp.timestamp DESC
        """
        
        try:
            with self.get_connection() as conn:
                cutoff_time = datetime.now() - timedelta(hours=hours)
                df = pd.read_sql_query(sql, conn, params=[cutoff_time])
                return df
        except Exception as e:
            logger.error(f"Error fetching congestion data: {e}")
            return pd.DataFrame()
    
    def get_route_statistics(self) -> pd.DataFrame:
        """Get route-wise statistics."""
        sql = """
        SELECT 
            r.route_id,
            r.route_name,
            r.speed_limit,
            AVG(td.speed) as avg_speed,
            MIN(td.speed) as min_speed,
            MAX(td.speed) as max_speed,
            COUNT(*) as data_points,
            COUNT(DISTINCT td.vehicle_id) as unique_vehicles
        FROM routes r
        LEFT JOIN traffic_data td ON r.route_id = td.route_id
        WHERE td.timestamp >= %s
        GROUP BY r.route_id, r.route_name, r.speed_limit
        ORDER BY avg_speed ASC
        """
        
        try:
            with self.get_connection() as conn:
                cutoff_time = datetime.now() - timedelta(hours=24)
                df = pd.read_sql_query(sql, conn, params=[cutoff_time])
                return df
        except Exception as e:
            logger.error(f"Error fetching route statistics: {e}")
            return pd.DataFrame()
    
    def cleanup_old_data(self, days: int = 7):
        """Remove old data to prevent database bloat."""
        sql_traffic = "DELETE FROM traffic_data WHERE timestamp < %s"
        sql_congestion = "DELETE FROM congestion_points WHERE timestamp < %s"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cutoff_time = datetime.now() - timedelta(days=days)
                    cursor.execute(sql_traffic, [cutoff_time])
                    traffic_deleted = cursor.rowcount
                    cursor.execute(sql_congestion, [cutoff_time])
                    congestion_deleted = cursor.rowcount
                    conn.commit()
                    logger.info(f"Cleaned up {traffic_deleted} traffic records and {congestion_deleted} congestion records")
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            raise
