"""
Fleet Analytics Pipeline - Poisson Incident Generator
Step 2.4: Generate safety incidents using Poisson distribution

Mathematical Model:
Number of incidents ~ Poisson(λ)
where λ = average rate of incidents per time period

Probability Mass Function:
P(X = k) = (λ^k × e^(-λ)) / k!

λ depends on:
- Driver experience (novice drivers have higher λ)
- Traffic conditions (heavy traffic increases λ)
- Weather conditions (poor weather increases λ)
- Time of day (rush hour increases λ)
"""

import numpy as np
from typing import Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import sys
sys.path.append('.')
from data_generators.utils.base_generator import (
    BaseGenerator, INCIDENT_TYPES, WEATHER_CONDITIONS
)
from data_generators.models.gaussian_speed import DriverBehavior, TimeOfDay
from loguru import logger


@dataclass
class Incident:
    """Represents a safety incident."""
    incident_id: str
    vehicle_id: str
    incident_type: str
    severity: str  # low, medium, high
    latitude: float
    longitude: float
    speed_at_incident: float
    timestamp: str
    weather_condition: str
    description: str


class PoissonIncidentGenerator(BaseGenerator):
    """
    Generate safety incidents using Poisson distribution.
    
    Mathematical Foundation:
    The Poisson distribution models the number of events occurring in a fixed
    interval of time when events occur independently at a constant average rate.
    
    Properties:
    - Mean = λ
    - Variance = λ
    - Discrete distribution (counts: 0, 1, 2, ...)
    
    For fleet safety:
    - Events = safety incidents (harsh braking, speeding, etc.)
    - λ = expected number of incidents per hour
    - λ varies based on driver risk factors
    """
    
    def __init__(self):
        super().__init__()
        
        # Base lambda (incident rate per hour) from config
        poisson_config = self.config['statistical_models']['poisson_incidents']
        self.base_lambda = poisson_config['base_lambda']
        self.aggressive_multiplier = poisson_config['aggressive_driver_multiplier']
        
        # Risk multipliers for different factors
        # These multiply the base λ to get adjusted incident rate
        
        # Driver experience affects incident rate
        self.experience_multipliers = {
            'Novice': 3.0,        # 3x more incidents
            'Intermediate': 1.5,   # 1.5x more incidents
            'Expert': 1.0,         # Baseline
            'Master': 0.6          # 40% fewer incidents
        }
        
        # Weather affects incident probability
        self.weather_multipliers = {
            'clear': 1.0,      # Baseline
            'rain': 2.0,       # 2x more incidents
            'fog': 2.5,        # 2.5x more incidents
            'dust': 1.8        # 1.8x more incidents
        }
        
        # Traffic conditions
        self.traffic_multipliers = {
            'light': 0.8,      # 20% fewer incidents
            'moderate': 1.0,   # Baseline
            'heavy': 1.7,      # 70% more incidents
            'congested': 2.2   # 2.2x more incidents
        }
        
        # Time of day affects incident rate
        self.time_multipliers = {
            TimeOfDay.MORNING_RUSH: 1.5,    # Higher stress
            TimeOfDay.MIDDAY: 1.0,          # Normal
            TimeOfDay.EVENING_RUSH: 1.6,    # Highest stress + fatigue
            TimeOfDay.NIGHT: 1.3            # Fatigue + reduced visibility
        }
        
        # Incident severity distribution
        # Given an incident occurred, what's the probability of each severity?
        self.severity_distribution = {
            'low': 0.70,       # 70% are low severity
            'medium': 0.25,    # 25% are medium severity
            'high': 0.05       # 5% are high severity
        }
        
        self.incident_counter = 0
        
        logger.info("Poisson Incident Generator initialized")
        logger.info(f"Base λ: {self.base_lambda} incidents/hour")
    
    def calculate_lambda(self,
                        driver_experience: str,
                        driver_behavior: DriverBehavior,
                        weather: str,
                        traffic_condition: str,
                        time_of_day: TimeOfDay) -> float:
        """
        Calculate adjusted λ (lambda) for Poisson distribution.
        
        Formula:
        λ_adjusted = λ_base × exp_mult × weather_mult × traffic_mult × time_mult × behavior_mult
        
        Args:
            driver_experience: Driver experience level
            driver_behavior: Driver behavior pattern
            weather: Weather condition
            traffic_condition: Traffic density
            time_of_day: Time period
            
        Returns:
            Adjusted lambda value (incidents per hour)
        """
        lambda_value = self.base_lambda
        
        # Apply multipliers
        lambda_value *= self.experience_multipliers.get(driver_experience, 1.0)
        lambda_value *= self.weather_multipliers.get(weather, 1.0)
        lambda_value *= self.traffic_multipliers.get(traffic_condition, 1.0)
        lambda_value *= self.time_multipliers.get(time_of_day, 1.0)
        
        # Aggressive drivers have higher incident rates
        if driver_behavior == DriverBehavior.AGGRESSIVE:
            lambda_value *= self.aggressive_multiplier
        elif driver_behavior == DriverBehavior.CAUTIOUS:
            lambda_value *= 0.7  # 30% fewer incidents
        
        return lambda_value
    
    def generate_incidents_count(self, lambda_value: float, 
                                duration_hours: float) -> int:
        """
        Generate number of incidents using Poisson distribution.
        
        Formula:
        For duration > 1 hour, λ_total = λ_hourly × duration
        X ~ Poisson(λ_total)
        
        Args:
            lambda_value: Incident rate per hour
            duration_hours: Duration of observation period
            
        Returns:
            Number of incidents that occurred
        """
        # Scale lambda by duration
        lambda_total = lambda_value * duration_hours
        
        # Sample from Poisson distribution
        # np.random.poisson returns number of events
        num_incidents = np.random.poisson(lambda_total)
        
        return num_incidents
    
    def generate_incident(self,
                         vehicle_id: str,
                         latitude: float,
                         longitude: float,
                         current_speed: float,
                         weather: str,
                         timestamp: datetime = None) -> Incident:
        """
        Generate a single incident with details.
        
        Args:
            vehicle_id: Vehicle identifier
            latitude: GPS latitude
            longitude: GPS longitude
            current_speed: Vehicle speed when incident occurred
            weather: Weather condition
            timestamp: Time of incident
            
        Returns:
            Incident object with full details
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Select incident type based on speed
        # Speeding more likely at high speeds
        # Harsh braking more likely at low-medium speeds
        if current_speed > 100:
            incident_type = np.random.choice(
                ['speeding', 'harsh_acceleration', 'sharp_turn'],
                p=[0.6, 0.3, 0.1]
            )
        elif current_speed > 60:
            incident_type = np.random.choice(
                ['harsh_braking', 'sharp_turn', 'sudden_lane_change', 'speeding'],
                p=[0.4, 0.3, 0.2, 0.1]
            )
        else:
            incident_type = np.random.choice(
                ['harsh_braking', 'harsh_acceleration', 'sudden_lane_change'],
                p=[0.5, 0.3, 0.2]
            )
        
        # Assign severity based on distribution
        severity = np.random.choice(
            list(self.severity_distribution.keys()),
            p=list(self.severity_distribution.values())
        )
        
        # Generate description
        descriptions = {
            'harsh_braking': f"Sudden deceleration from {current_speed:.0f} km/h",
            'harsh_acceleration': f"Rapid acceleration to {current_speed:.0f} km/h",
            'speeding': f"Speed exceeded limit: {current_speed:.0f} km/h",
            'sharp_turn': f"Sharp turn at {current_speed:.0f} km/h",
            'sudden_lane_change': f"Abrupt lane change at {current_speed:.0f} km/h"
        }
        
        self.incident_counter += 1
        
        incident = Incident(
            incident_id=f"INC-{self.incident_counter:08d}",
            vehicle_id=vehicle_id,
            incident_type=incident_type,
            severity=severity,
            latitude=round(latitude, 6),
            longitude=round(longitude, 6),
            speed_at_incident=round(current_speed, 2),
            timestamp=timestamp.isoformat(),
            weather_condition=weather,
            description=descriptions.get(incident_type, "Safety incident occurred")
        )
        
        return incident
    
    def generate_incident_timeline(self,
                                  vehicle_id: str,
                                  lambda_value: float,
                                  duration_hours: float,
                                  start_location: Tuple[float, float],
                                  avg_speed: float,
                                  weather: str) -> List[Incident]:
        """
        Generate a timeline of incidents over a duration.
        
        Args:
            vehicle_id: Vehicle identifier
            lambda_value: Incident rate per hour
            duration_hours: Duration of trip
            start_location: Starting GPS coordinates
            avg_speed: Average vehicle speed
            weather: Weather condition
            
        Returns:
            List of Incident objects
        """
        # Determine number of incidents
        num_incidents = self.generate_incidents_count(lambda_value, duration_hours)
        
        if num_incidents == 0:
            return []
        
        incidents = []
        start_time = datetime.utcnow()
        
        # Generate random timestamps within the duration
        incident_times = sorted([
            start_time + timedelta(hours=np.random.uniform(0, duration_hours))
            for _ in range(num_incidents)
        ])
        
        for i, incident_time in enumerate(incident_times):
            # Simulate location change (simple linear progression)
            progress = (i + 1) / num_incidents
            lat = start_location[0] + np.random.uniform(-0.1, 0.1)
            lon = start_location[1] + np.random.uniform(-0.1, 0.1)
            
            # Speed varies around average
            speed = max(0, avg_speed + np.random.normal(0, 15))
            
            incident = self.generate_incident(
                vehicle_id=vehicle_id,
                latitude=lat,
                longitude=lon,
                current_speed=speed,
                weather=weather,
                timestamp=incident_time
            )
            incidents.append(incident)
        
        return incidents
    
    def calculate_incident_statistics(self, 
                                     incident_counts: List[int]) -> Dict[str, float]:
        """
        Calculate statistics for incident counts to verify Poisson properties.
        
        For Poisson distribution: Mean ≈ Variance
        
        Args:
            incident_counts: List of incident counts from multiple observations
            
        Returns:
            Statistical measures
        """
        counts_array = np.array(incident_counts)
        mean = np.mean(counts_array)
        variance = np.var(counts_array)
        
        # Check if mean ≈ variance (Poisson property)
        ratio = variance / mean if mean > 0 else 0
        is_poisson_like = 0.8 <= ratio <= 1.2  # Allow 20% tolerance
        
        return {
            'mean': round(mean, 3),
            'variance': round(variance, 3),
            'variance_to_mean_ratio': round(ratio, 3),
            'std': round(np.std(counts_array), 3),
            'min': int(np.min(counts_array)),
            'max': int(np.max(counts_array)),
            'follows_poisson': is_poisson_like
        }


if __name__ == "__main__":
    # Test the Poisson Incident Generator
    generator = PoissonIncidentGenerator()
    
    logger.info("Testing Poisson Incident Generator...")
    
    # Test 1: Calculate lambda for different scenarios
    logger.info("\n=== Test 1: Lambda Calculation ===")
    
    scenarios = [
        ('Novice', DriverBehavior.AGGRESSIVE, 'rain', 'heavy', TimeOfDay.EVENING_RUSH),
        ('Expert', DriverBehavior.NORMAL, 'clear', 'light', TimeOfDay.MIDDAY),
        ('Master', DriverBehavior.CAUTIOUS, 'clear', 'light', TimeOfDay.MIDDAY)
    ]
    
    for experience, behavior, weather, traffic, time in scenarios:
        lambda_val = generator.calculate_lambda(experience, behavior, weather, traffic, time)
        logger.info(f"{experience}, {behavior.value}, {weather}, {traffic}, {time.value}: "
                   f"λ = {lambda_val:.3f} incidents/hour")
    
    # Test 2: Generate incident counts
    logger.info("\n=== Test 2: Incident Count Generation ===")
    
    # Simulate 100 trips with same conditions
    lambda_test = 0.5  # 0.5 incidents per hour
    duration = 2.0      # 2 hour trips
    
    incident_counts = [
        generator.generate_incidents_count(lambda_test, duration)
        for _ in range(100)
    ]
    
    stats = generator.calculate_incident_statistics(incident_counts)
    logger.info(f"Simulated 100 trips (λ={lambda_test}, duration={duration}h)")
    logger.info(f"Statistics: {stats}")
    logger.info(f"Expected λ_total = {lambda_test * duration:.2f}")
    logger.info(f"Follows Poisson properties: {stats['follows_poisson']}")
    
    # Test 3: Generate incident timeline
    logger.info("\n=== Test 3: Incident Timeline Generation ===")
    
    lambda_risky = 1.5  # High risk scenario
    incidents = generator.generate_incident_timeline(
        vehicle_id="VEH-00001",
        lambda_value=lambda_risky,
        duration_hours=3.0,
        start_location=(24.8607, 67.0011),
        avg_speed=75.0,
        weather='rain'
    )
    
    logger.info(f"Generated {len(incidents)} incidents over 3 hours")
    logger.info(f"Expected: {lambda_risky * 3:.1f} incidents")
    
    if incidents:
        logger.info("\nSample incidents:")
        for inc in incidents[:3]:
            logger.info(f"  - {inc.incident_type} ({inc.severity}): {inc.description}")