"""
Fleet Analytics Pipeline - Markov Chain Route Generator
Step 2.2: Generate realistic GPS routes using Markov Chain

Mathematical Model:
- States: S = {warehouse, highway, urban, customer}
- Transition Matrix: P(s_t | s_{t-1})
- Next state = current_state × transition_matrix

This model ensures realistic route progression from warehouse to customer.
"""

import numpy as np
from typing import List, Tuple, Dict
from dataclasses import dataclass
import sys
sys.path.append('.')
from data_generators.utils.base_generator import BaseGenerator, WAREHOUSE_CITIES
from loguru import logger


@dataclass
class GPSCoordinate:
    """Represents a GPS coordinate point."""
    latitude: float
    longitude: float
    road_type: str
    timestamp: str


class MarkovRouteGenerator(BaseGenerator):
    """
    Generate vehicle routes using Markov Chain.
    
    Mathematical Foundation:
    P(X_t = j | X_{t-1} = i) = P_ij
    where P is the transition matrix
    
    States:
    - warehouse: Starting point (loading dock)
    - highway: High-speed interstate travel
    - urban: City streets and local roads
    - customer: Delivery destination
    """
    
    def __init__(self):
        super().__init__()
        
        # Load Markov Chain configuration
        mc_config = self.config['statistical_models']['markov_chain']
        self.states = mc_config['states']
        
        # Transition probability matrix P_ij
        # P_ij = probability of transitioning from state i to state j
        self.transition_matrix = np.array([
            mc_config['transition_matrix']['warehouse'],   # From warehouse
            mc_config['transition_matrix']['highway'],      # From highway
            mc_config['transition_matrix']['urban'],        # From urban
            mc_config['transition_matrix']['customer']      # From customer
        ])
        
        # Verify transition matrix is stochastic (rows sum to 1)
        assert np.allclose(self.transition_matrix.sum(axis=1), 1.0), \
            "Transition matrix rows must sum to 1"
        
        # GPS coordinates for warehouses (Karachi area as base)
        # In production, these would come from a database
        self.warehouse_coordinates = self._initialize_warehouse_coords()
        
        logger.info("Markov Chain Route Generator initialized")
        logger.info(f"States: {self.states}")
        logger.info(f"Transition Matrix:\n{self.transition_matrix}")
    
    def _initialize_warehouse_coords(self) -> Dict[str, Tuple[float, float]]:
        """
        Initialize warehouse GPS coordinates.
        Using Karachi, Pakistan as base with realistic spread.
        """
        # Karachi base: 24.8607° N, 67.0011° E
        base_lat, base_lon = 24.8607, 67.0011
        
        coords = {}
        for i, city in enumerate(WAREHOUSE_CITIES):
            # Spread warehouses across realistic locations
            lat_offset = np.random.uniform(-2, 2)
            lon_offset = np.random.uniform(-2, 2)
            coords[city] = (base_lat + lat_offset, base_lon + lon_offset)
        
        return coords
    
    def generate_route(self, 
                       warehouse_location: str,
                       customer_location: Tuple[float, float],
                       num_waypoints: int = 50) -> List[GPSCoordinate]:
        """
        Generate a complete route from warehouse to customer using Markov Chain.
        
        Args:
            warehouse_location: Warehouse city name
            customer_location: (latitude, longitude) of customer
            num_waypoints: Number of GPS points to generate
            
        Returns:
            List of GPSCoordinate objects representing the route
        
        Algorithm:
        1. Start at warehouse state
        2. For each waypoint:
           - Sample next state from transition matrix
           - Generate GPS coordinates for that state
           - Move towards customer destination
        3. End at customer state
        """
        route = []
        
        # Get warehouse coordinates
        if warehouse_location not in self.warehouse_coordinates:
            warehouse_location = np.random.choice(WAREHOUSE_CITIES)
        
        start_lat, start_lon = self.warehouse_coordinates[warehouse_location]
        end_lat, end_lon = customer_location
        
        # Current state (start at warehouse)
        current_state_idx = 0  # warehouse
        current_lat, current_lon = start_lat, start_lon
        
        # Calculate increments for smooth progression
        lat_increment = (end_lat - start_lat) / num_waypoints
        lon_increment = (end_lon - start_lon) / num_waypoints
        
        for waypoint in range(num_waypoints):
            # Get current state name and road type
            current_state = self.states[current_state_idx]
            road_type = self._state_to_road_type(current_state)
            
            # Generate GPS coordinate
            # Add small random noise for realistic GPS variance
            noise_lat = np.random.normal(0, 0.001)
            noise_lon = np.random.normal(0, 0.001)
            
            coord = GPSCoordinate(
                latitude=round(current_lat + noise_lat, 6),
                longitude=round(current_lon + noise_lon, 6),
                road_type=road_type,
                timestamp=self.get_timestamp()
            )
            route.append(coord)
            
            # Transition to next state using Markov Chain
            # Sample from categorical distribution defined by transition probabilities
            next_state_idx = np.random.choice(
                len(self.states),
                p=self.transition_matrix[current_state_idx]
            )
            
            # Update position (move towards customer)
            current_lat += lat_increment
            current_lon += lon_increment
            
            # Update state
            current_state_idx = next_state_idx
            
            # If we're near the end and not at customer state, force transition
            if waypoint >= num_waypoints - 5 and current_state_idx != 3:
                current_state_idx = 3  # Force to customer state
        
        logger.debug(f"Generated route with {len(route)} waypoints")
        return route
    
    def _state_to_road_type(self, state: str) -> str:
        """
        Map Markov state to road type.
        
        Args:
            state: Markov chain state name
            
        Returns:
            Road type string
        """
        mapping = {
            'warehouse': 'urban',
            'highway': 'highway',
            'urban': 'urban',
            'customer': 'urban'
        }
        return mapping.get(state, 'urban')
    
    def calculate_route_distance(self, route: List[GPSCoordinate]) -> float:
        """
        Calculate total route distance using Haversine formula.
        
        Args:
            route: List of GPS coordinates
            
        Returns:
            Total distance in kilometers
        """
        total_distance = 0.0
        
        for i in range(len(route) - 1):
            dist = self._haversine_distance(
                route[i].latitude, route[i].longitude,
                route[i+1].latitude, route[i+1].longitude
            )
            total_distance += dist
        
        return total_distance
    
    def _haversine_distance(self, lat1: float, lon1: float, 
                           lat2: float, lon2: float) -> float:
        """
        Calculate distance between two GPS points using Haversine formula.
        
        Formula:
        a = sin²(Δlat/2) + cos(lat1) × cos(lat2) × sin²(Δlon/2)
        c = 2 × atan2(√a, √(1-a))
        d = R × c (where R = Earth's radius = 6371 km)
        
        Args:
            lat1, lon1: First point coordinates
            lat2, lon2: Second point coordinates
            
        Returns:
            Distance in kilometers
        """
        R = 6371  # Earth's radius in kilometers
        
        # Convert to radians
        lat1_rad = np.radians(lat1)
        lat2_rad = np.radians(lat2)
        delta_lat = np.radians(lat2 - lat1)
        delta_lon = np.radians(lon2 - lon1)
        
        # Haversine formula
        a = (np.sin(delta_lat / 2) ** 2 + 
             np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2)
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        distance = R * c
        
        return distance
    
    def get_state_distribution(self, route: List[GPSCoordinate]) -> Dict[str, float]:
        """
        Calculate the distribution of road types in a route.
        Useful for validation and statistics.
        
        Args:
            route: List of GPS coordinates
            
        Returns:
            Dictionary with road type percentages
        """
        road_types = [coord.road_type for coord in route]
        total = len(road_types)
        
        distribution = {}
        for road_type in set(road_types):
            count = road_types.count(road_type)
            distribution[road_type] = round(count / total * 100, 2)
        
        return distribution


if __name__ == "__main__":
    # Test the Markov Route Generator
    generator = MarkovRouteGenerator()
    
    # Generate a sample route
    warehouse = "Karachi"
    customer_coords = (24.9056, 67.0822)  # Sample customer location
    
    route = generator.generate_route(
        warehouse_location=warehouse,
        customer_location=customer_coords,
        num_waypoints=50
    )
    
    # Calculate statistics
    distance = generator.calculate_route_distance(route)
    distribution = generator.get_state_distribution(route)
    
    logger.info(f"Generated route from {warehouse} to customer")
    logger.info(f"Total waypoints: {len(route)}")
    logger.info(f"Total distance: {distance:.2f} km")
    logger.info(f"Road type distribution: {distribution}")
    
    # Show first and last 3 waypoints
    logger.info("\nFirst 3 waypoints:")
    for i, coord in enumerate(route[:3]):
        logger.info(f"  {i+1}. Lat: {coord.latitude}, Lon: {coord.longitude}, Type: {coord.road_type}")
    
    logger.info("\nLast 3 waypoints:")
    for i, coord in enumerate(route[-3:]):
        logger.info(f"  {i+1}. Lat: {coord.latitude}, Lon: {coord.longitude}, Type: {coord.road_type}")