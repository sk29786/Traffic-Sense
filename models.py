"""
Data models and structures for the traffic simulation system.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import random

@dataclass
class Vehicle:
    """Represents a vehicle in the traffic simulation."""
    vehicle_id: str
    vehicle_type: str  # car, truck, bus, motorcycle
    current_speed: float
    max_speed: float
    route_id: str
    position_x: float
    position_y: float
    timestamp: datetime
    
    @classmethod
    def generate_random(cls, vehicle_id: str, route_id: str) -> 'Vehicle':
        """Generate a random vehicle with realistic attributes."""
        vehicle_types = {
            'car': {'max_speed': random.uniform(80, 120), 'weight': 0.7},
            'truck': {'max_speed': random.uniform(60, 90), 'weight': 0.15},
            'bus': {'max_speed': random.uniform(50, 80), 'weight': 0.1},
            'motorcycle': {'max_speed': random.uniform(90, 140), 'weight': 0.05}
        }
        
        # Weighted random selection
        vehicle_type = random.choices(
            list(vehicle_types.keys()),
            weights=[v['weight'] for v in vehicle_types.values()]
        )[0]
        
        max_speed = vehicle_types[vehicle_type]['max_speed']
        current_speed = random.uniform(0, max_speed * 0.8)  # Usually not at max speed
        
        return cls(
            vehicle_id=vehicle_id,
            vehicle_type=vehicle_type,
            current_speed=current_speed,
            max_speed=max_speed,
            route_id=route_id,
            position_x=random.uniform(0, 1000),  # City coordinates
            position_y=random.uniform(0, 1000),
            timestamp=datetime.now()
        )

@dataclass
class Route:
    """Represents a route in the traffic system."""
    route_id: str
    start_point: tuple  # (x, y)
    end_point: tuple    # (x, y)
    distance_km: float
    route_name: str
    speed_limit: float
    
    @classmethod
    def generate_routes(cls, num_routes: int = 10) -> List['Route']:
        """Generate a set of predefined routes."""
        routes = []
        route_names = [
            "Main Street", "Highway 1", "Broadway", "Park Avenue", "Industrial Road",
            "City Center", "Suburban Loop", "Airport Highway", "University Drive", "Shopping District"
        ]
        
        for i in range(min(num_routes, len(route_names))):
            start_x, start_y = random.uniform(0, 1000), random.uniform(0, 1000)
            end_x, end_y = random.uniform(0, 1000), random.uniform(0, 1000)
            distance = ((end_x - start_x) ** 2 + (end_y - start_y) ** 2) ** 0.5 / 100  # Convert to km
            
            routes.append(cls(
                route_id=f"route_{i+1:02d}",
                start_point=(start_x, start_y),
                end_point=(end_x, end_y),
                distance_km=distance,
                route_name=route_names[i],
                speed_limit=random.choice([50, 60, 80, 100])
            ))
        
        return routes

@dataclass
class CongestionPoint:
    """Represents a detected congestion point."""
    location_x: float
    location_y: float
    congestion_level: str  # low, medium, high
    average_speed: float
    vehicle_count: int
    timestamp: datetime
    route_id: str
