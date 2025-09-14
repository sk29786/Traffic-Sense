"""
Traffic simulation engine for generating realistic vehicle traffic data.
"""
import random
import time
import threading
from datetime import datetime, timedelta
from typing import List, Dict
import logging
from models import Vehicle, Route
from database import DatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TrafficSimulation:
    """Main traffic simulation engine."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize the traffic simulation."""
        self.db_manager = db_manager
        self.routes: List[Route] = []
        self.active_vehicles: Dict[str, Vehicle] = {}
        self.simulation_running = False
        self.simulation_thread = None
        
        # Simulation parameters
        self.max_vehicles_per_route = 20
        self.vehicle_spawn_rate = 0.3  # Probability of spawning a new vehicle per second
        self.vehicle_despawn_rate = 0.1  # Probability of vehicle leaving simulation
        self.speed_variation_factor = 0.2  # How much speeds can vary
        
        self._initialize_routes()
    
    def _initialize_routes(self):
        """Initialize route data in the database."""
        self.routes = Route.generate_routes(10)
        try:
            self.db_manager.insert_routes(self.routes)
            logger.info(f"Initialized {len(self.routes)} routes")
        except Exception as e:
            logger.error(f"Error initializing routes: {e}")
            raise
    
    def _get_route_by_id(self, route_id: str) -> Route:
        """Get route object by ID."""
        for route in self.routes:
            if route.route_id == route_id:
                return route
        return None
    
    def _update_vehicle_position(self, vehicle: Vehicle) -> Vehicle:
        """Update vehicle position based on time and speed."""
        route = self._get_route_by_id(vehicle.route_id)
        if not route:
            return vehicle
        
        # Simple linear movement simulation
        time_delta = 0.1  # Assume 0.1 hour time step
        distance_traveled = vehicle.current_speed * time_delta
        
        # Calculate direction vector
        dx = route.end_point[0] - route.start_point[0]
        dy = route.end_point[1] - route.start_point[1]
        total_distance = (dx**2 + dy**2)**0.5
        
        if total_distance > 0:
            # Normalize direction
            dx_norm = dx / total_distance
            dy_norm = dy / total_distance
            
            # Update position
            vehicle.position_x += dx_norm * distance_traveled
            vehicle.position_y += dy_norm * distance_traveled
        
        # Add some randomness to speed (traffic conditions)
        speed_variation = random.uniform(-vehicle.max_speed * self.speed_variation_factor, 
                                       vehicle.max_speed * self.speed_variation_factor)
        vehicle.current_speed = max(0, min(vehicle.max_speed, 
                                         vehicle.current_speed + speed_variation))
        
        # Update timestamp
        vehicle.timestamp = datetime.now()
        
        return vehicle
    
    def _spawn_new_vehicles(self):
        """Spawn new vehicles on random routes."""
        for route in self.routes:
            # Count current vehicles on this route
            vehicles_on_route = sum(1 for v in self.active_vehicles.values() 
                                  if v.route_id == route.route_id)
            
            # Spawn new vehicle if under limit and random chance
            if (vehicles_on_route < self.max_vehicles_per_route and 
                random.random() < self.vehicle_spawn_rate):
                
                vehicle_id = f"{route.route_id}_v{random.randint(1000, 9999)}"
                if vehicle_id not in self.active_vehicles:
                    new_vehicle = Vehicle.generate_random(vehicle_id, route.route_id)
                    # Position vehicle at route start
                    new_vehicle.position_x = route.start_point[0]
                    new_vehicle.position_y = route.start_point[1]
                    
                    self.active_vehicles[vehicle_id] = new_vehicle
                    logger.info(f"Spawned new vehicle {vehicle_id} on {route.route_name}")
    
    def _remove_vehicles(self):
        """Remove vehicles that have reached destination or randomly despawn."""
        vehicles_to_remove = []
        
        for vehicle_id, vehicle in self.active_vehicles.items():
            route = self._get_route_by_id(vehicle.route_id)
            if not route:
                vehicles_to_remove.append(vehicle_id)
                continue
            
            # Check if vehicle reached destination
            distance_to_end = ((vehicle.position_x - route.end_point[0])**2 + 
                             (vehicle.position_y - route.end_point[1])**2)**0.5
            
            # Remove if close to destination or random despawn
            if distance_to_end < 50 or random.random() < self.vehicle_despawn_rate:
                vehicles_to_remove.append(vehicle_id)
        
        for vehicle_id in vehicles_to_remove:
            del self.active_vehicles[vehicle_id]
            logger.info(f"Removed vehicle {vehicle_id}")
    
    def _simulate_congestion(self):
        """Create realistic congestion patterns."""
        # Current hour affects traffic density
        current_hour = datetime.now().hour
        
        # Rush hour simulation (7-9 AM and 5-7 PM)
        is_rush_hour = (7 <= current_hour <= 9) or (17 <= current_hour <= 19)
        
        if is_rush_hour:
            # Increase spawn rate and reduce speeds during rush hour
            self.vehicle_spawn_rate = 0.5
            congestion_factor = 0.6  # Reduce speeds by 40%
        else:
            self.vehicle_spawn_rate = 0.3
            congestion_factor = 1.0
        
        # Apply congestion to vehicles
        for vehicle in self.active_vehicles.values():
            if random.random() < 0.3:  # 30% chance of being affected by congestion
                vehicle.current_speed *= congestion_factor
                vehicle.current_speed = max(5, vehicle.current_speed)  # Minimum speed
    
    def _simulation_step(self):
        """Execute one step of the simulation."""
        try:
            # Update existing vehicles
            for vehicle in self.active_vehicles.values():
                updated_vehicle = self._update_vehicle_position(vehicle)
                
                # Store in database
                self.db_manager.insert_vehicle(updated_vehicle)
                self.db_manager.insert_traffic_data(updated_vehicle)
            
            # Spawn new vehicles
            self._spawn_new_vehicles()
            
            # Remove vehicles
            self._remove_vehicles()
            
            # Simulate congestion
            self._simulate_congestion()
            
            logger.info(f"Simulation step completed. Active vehicles: {len(self.active_vehicles)}")
            
        except Exception as e:
            logger.error(f"Error in simulation step: {e}")
    
    def start_simulation(self, step_interval: float = 5.0):
        """Start the traffic simulation."""
        if self.simulation_running:
            logger.warning("Simulation is already running")
            return
        
        self.simulation_running = True
        logger.info("Starting traffic simulation")
        
        def simulation_loop():
            while self.simulation_running:
                self._simulation_step()
                time.sleep(step_interval)
            logger.info("Traffic simulation stopped")
        
        self.simulation_thread = threading.Thread(target=simulation_loop, daemon=True)
        self.simulation_thread.start()
    
    def stop_simulation(self):
        """Stop the traffic simulation."""
        if self.simulation_running:
            self.simulation_running = False
            logger.info("Stopping traffic simulation")
            if self.simulation_thread:
                self.simulation_thread.join(timeout=10)
    
    def get_simulation_status(self) -> Dict:
        """Get current simulation status."""
        return {
            'running': self.simulation_running,
            'active_vehicles': len(self.active_vehicles),
            'routes': len(self.routes),
            'vehicle_types': {
                vtype: sum(1 for v in self.active_vehicles.values() if v.vehicle_type == vtype)
                for vtype in ['car', 'truck', 'bus', 'motorcycle']
            }
        }
    
    def generate_batch_data(self, num_vehicles: int = 100, duration_minutes: int = 60):
        """Generate batch data for testing/initialization."""
        logger.info(f"Generating batch data: {num_vehicles} vehicles over {duration_minutes} minutes")
        
        start_time = datetime.now() - timedelta(minutes=duration_minutes)
        
        for i in range(num_vehicles):
            # Random route
            route = random.choice(self.routes)
            vehicle_id = f"batch_v{i:04d}"
            
            # Generate vehicle path over time
            vehicle = Vehicle.generate_random(vehicle_id, route.route_id)
            vehicle.position_x = route.start_point[0]
            vehicle.position_y = route.start_point[1]
            
            # Generate data points over time
            num_points = random.randint(5, 20)
            for j in range(num_points):
                vehicle.timestamp = start_time + timedelta(
                    minutes=random.uniform(0, duration_minutes)
                )
                
                # Update position along route
                progress = j / num_points
                vehicle.position_x = (route.start_point[0] + 
                                    (route.end_point[0] - route.start_point[0]) * progress)
                vehicle.position_y = (route.start_point[1] + 
                                    (route.end_point[1] - route.start_point[1]) * progress)
                
                # Vary speed
                vehicle.current_speed = max(5, vehicle.max_speed * random.uniform(0.3, 1.0))
                
                try:
                    self.db_manager.insert_traffic_data(vehicle)
                except Exception as e:
                    logger.error(f"Error inserting batch data: {e}")
        
        logger.info("Batch data generation completed")
