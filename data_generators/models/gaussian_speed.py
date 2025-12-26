"""
Fleet Analytics Pipeline - Gaussian Speed Generator
Step 2.3: Generate realistic vehicle speed using Gaussian/Normal distribution

Mathematical Model:
Speed ~ N(μ, σ²)
where:
- μ (mu) = mean speed (depends on road type, time, weather, driver)
- σ (sigma) = standard deviation (traffic variability)

Formula: X = μ + σ × Z, where Z ~ N(0,1)

This is NOT random - it's based on statistical properties of real-world driving.
"""

import numpy as np
from typing import Dict, Tuple
from enum import Enum
import sys
sys.path.append('.')
from data_generators.utils.base_generator import BaseGenerator, ROAD_TYPES, WEATHER_CONDITIONS
from loguru import logger


class TimeOfDay(Enum):
    """Time of day categories affecting speed."""
    MORNING_RUSH = "morning_rush"      # 7-9 AM
    MIDDAY = "midday"                  # 10 AM - 4 PM
    EVENING_RUSH = "evening_rush"      # 5-7 PM
    NIGHT = "night"                    # 8 PM - 6 AM


class DriverBehavior(Enum):
    """Driver behavior patterns affecting speed."""
    CAUTIOUS = "cautious"      # Drives below average
    NORMAL = "normal"          # Drives at average
    AGGRESSIVE = "aggressive"  # Drives above average


class GaussianSpeedGenerator(BaseGenerator):
    """
    Generate vehicle speeds using Gaussian (Normal) distribution.
    
    Mathematical Foundation:
    For a Gaussian distribution N(μ, σ²):
    - Probability Density: f(x) = (1/(σ√(2π))) × e^(-(x-μ)²/(2σ²))
    - 68% of values fall within μ ± σ
    - 95% of values fall within μ ± 2σ
    - 99.7% of values fall within μ ± 3σ
    
    We adjust μ and σ based on:
    1. Road type (highway vs urban vs rural)
    2. Time of day (rush hour vs off-peak)
    3. Weather conditions (clear vs rain vs fog)
    4. Driver behavior (cautious vs normal vs aggressive)
    """
    
    def __init__(self):
        super().__init__()
        
        # Base parameters from config
        speed_config = self.config['statistical_models']['gaussian_speed']
        
        # Base mean (μ) and standard deviation (σ) for each road type
        # These represent ideal conditions (clear weather, normal traffic)
        self.base_params = {
            'highway': {
                'mean': speed_config['highway_mean'],      # 90 km/h
                'std': speed_config['highway_std']         # 15 km/h
            },
            'urban': {
                'mean': speed_config['urban_mean'],        # 45 km/h
                'std': speed_config['urban_std']           # 10 km/h
            },
            'rural': {
                'mean': 70,   # 70 km/h
                'std': 12     # 12 km/h
            }
        }
        
        # Adjustment factors for different conditions
        # These multiply the mean speed
        self.time_factors = {
            TimeOfDay.MORNING_RUSH: 0.7,   # 30% slower
            TimeOfDay.MIDDAY: 1.0,          # Normal speed
            TimeOfDay.EVENING_RUSH: 0.65,   # 35% slower
            TimeOfDay.NIGHT: 1.1            # 10% faster (less traffic)
        }
        
        self.weather_factors = {
            'clear': 1.0,      # Normal speed
            'rain': 0.8,       # 20% slower
            'fog': 0.6,        # 40% slower
            'dust': 0.7        # 30% slower
        }
        
        self.driver_factors = {
            DriverBehavior.CAUTIOUS: 0.85,     # 15% slower
            DriverBehavior.NORMAL: 1.0,        # Normal speed
            DriverBehavior.AGGRESSIVE: 1.25    # 25% faster
        }
        
        logger.info("Gaussian Speed Generator initialized")
        logger.info(f"Base parameters: {self.base_params}")
    
    def generate_speed(self,
                      road_type: str,
                      time_of_day: TimeOfDay,
                      weather: str,
                      driver_behavior: DriverBehavior,
                      previous_speed: float = None) -> float:
        """
        Generate vehicle speed using Gaussian distribution with adjustments.
        
        Args:
            road_type: Type of road (highway, urban, rural)
            time_of_day: Time period (rush hour, midday, etc.)
            weather: Weather condition (clear, rain, fog, dust)
            driver_behavior: Driver's typical behavior pattern
            previous_speed: Previous speed for smoothing (optional)
            
        Returns:
            Speed in km/h
            
        Algorithm:
        1. Get base μ and σ for road type
        2. Adjust μ based on time, weather, and driver
        3. Sample from N(adjusted_μ, σ²)
        4. Apply constraints (min/max speed)
        5. Smooth with previous speed if provided
        """
        # Get base parameters
        if road_type not in self.base_params:
            logger.warning(f"Unknown road type {road_type}, defaulting to urban")
            road_type = 'urban'
        
        base_mean = self.base_params[road_type]['mean']
        base_std = self.base_params[road_type]['std']
        
        # Calculate adjusted mean (μ)
        # μ_adjusted = μ_base × time_factor × weather_factor × driver_factor
        adjusted_mean = base_mean
        adjusted_mean *= self.time_factors[time_of_day]
        adjusted_mean *= self.weather_factors.get(weather, 1.0)
        adjusted_mean *= self.driver_factors[driver_behavior]
        
        # Increase variance during rush hour (more unpredictable)
        adjusted_std = base_std
        if time_of_day in [TimeOfDay.MORNING_RUSH, TimeOfDay.EVENING_RUSH]:
            adjusted_std *= 1.3
        
        # Sample from Gaussian distribution
        # Using numpy's normal distribution: X ~ N(μ, σ)
        speed = np.random.normal(adjusted_mean, adjusted_std)
        
        # Apply physical constraints
        min_speed = 10   # Minimum speed (almost stopped)
        max_speed = 140  # Maximum speed (safety limit)
        speed = np.clip(speed, min_speed, max_speed)
        
        # Smooth with previous speed for realistic acceleration/deceleration
        if previous_speed is not None:
            # Don't change speed too drastically between samples
            max_change = 15  # Maximum km/h change per time step
            speed_diff = speed - previous_speed
            if abs(speed_diff) > max_change:
                speed = previous_speed + np.sign(speed_diff) * max_change
        
        return round(speed, 2)
    
    def generate_speed_profile(self,
                              road_type: str,
                              duration_minutes: int,
                              sample_interval_seconds: int = 3) -> list:
        """
        Generate a complete speed profile over time.
        
        Args:
            road_type: Type of road
            duration_minutes: Duration of the trip
            sample_interval_seconds: How often to sample speed
            
        Returns:
            List of speed values over time
        """
        num_samples = int((duration_minutes * 60) / sample_interval_seconds)
        
        # Randomly select conditions (in production, these would be inputs)
        time_of_day = np.random.choice(list(TimeOfDay))
        weather = np.random.choice(WEATHER_CONDITIONS)
        driver_behavior = np.random.choice(list(DriverBehavior))
        
        speeds = []
        previous_speed = None
        
        for _ in range(num_samples):
            speed = self.generate_speed(
                road_type=road_type,
                time_of_day=time_of_day,
                weather=weather,
                driver_behavior=driver_behavior,
                previous_speed=previous_speed
            )
            speeds.append(speed)
            previous_speed = speed
        
        return speeds
    
    def calculate_statistics(self, speeds: list) -> Dict[str, float]:
        """
        Calculate statistical measures for a speed profile.
        
        Args:
            speeds: List of speed values
            
        Returns:
            Dictionary with statistical measures
        """
        speeds_array = np.array(speeds)
        
        return {
            'mean': round(np.mean(speeds_array), 2),
            'std': round(np.std(speeds_array), 2),
            'min': round(np.min(speeds_array), 2),
            'max': round(np.max(speeds_array), 2),
            'median': round(np.median(speeds_array), 2),
            'q25': round(np.percentile(speeds_array, 25), 2),
            'q75': round(np.percentile(speeds_array, 75), 2)
        }
    
    def verify_gaussian_properties(self, speeds: list) -> Dict[str, bool]:
        """
        Verify that generated speeds follow Gaussian distribution properties.
        
        68-95-99.7 rule verification:
        - ~68% of values within 1 standard deviation
        - ~95% of values within 2 standard deviations
        - ~99.7% of values within 3 standard deviations
        
        Args:
            speeds: List of speed values
            
        Returns:
            Dictionary indicating if properties hold
        """
        speeds_array = np.array(speeds)
        mean = np.mean(speeds_array)
        std = np.std(speeds_array)
        
        # Calculate percentages within n standard deviations
        within_1std = np.sum(np.abs(speeds_array - mean) <= std) / len(speeds_array)
        within_2std = np.sum(np.abs(speeds_array - mean) <= 2*std) / len(speeds_array)
        within_3std = np.sum(np.abs(speeds_array - mean) <= 3*std) / len(speeds_array)
        
        return {
            '68_rule': 0.60 <= within_1std <= 0.75,  # Allow some tolerance
            '95_rule': 0.90 <= within_2std <= 0.98,
            '99_rule': 0.95 <= within_3std <= 1.00,
            'actual_1std': round(within_1std * 100, 2),
            'actual_2std': round(within_2std * 100, 2),
            'actual_3std': round(within_3std * 100, 2)
        }


if __name__ == "__main__":
    # Test the Gaussian Speed Generator
    generator = GaussianSpeedGenerator()
    
    logger.info("Testing Gaussian Speed Generator...")
    
    # Test 1: Generate single speed values
    logger.info("\n=== Test 1: Single Speed Generation ===")
    
    test_conditions = [
        ('highway', TimeOfDay.MIDDAY, 'clear', DriverBehavior.NORMAL),
        ('urban', TimeOfDay.MORNING_RUSH, 'rain', DriverBehavior.CAUTIOUS),
        ('highway', TimeOfDay.NIGHT, 'clear', DriverBehavior.AGGRESSIVE)
    ]
    
    for road_type, time, weather, behavior in test_conditions:
        speed = generator.generate_speed(road_type, time, weather, behavior)
        logger.info(f"{road_type}, {time.value}, {weather}, {behavior.value}: {speed} km/h")
    
    # Test 2: Generate speed profile
    logger.info("\n=== Test 2: Speed Profile Generation ===")
    
    speeds_highway = generator.generate_speed_profile('highway', duration_minutes=10)
    speeds_urban = generator.generate_speed_profile('urban', duration_minutes=10)
    
    # Calculate statistics
    stats_highway = generator.calculate_statistics(speeds_highway)
    stats_urban = generator.calculate_statistics(speeds_urban)
    
    logger.info(f"Highway speeds (n={len(speeds_highway)}): {stats_highway}")
    logger.info(f"Urban speeds (n={len(speeds_urban)}): {stats_urban}")
    
    # Test 3: Verify Gaussian properties
    logger.info("\n=== Test 3: Gaussian Distribution Verification ===")
    
    # Generate large sample for better statistical verification
    large_sample = generator.generate_speed_profile('highway', duration_minutes=60)
    verification = generator.verify_gaussian_properties(large_sample)
    
    logger.info(f"Sample size: {len(large_sample)}")
    logger.info(f"68% rule (±1σ): {verification['68_rule']} (actual: {verification['actual_1std']}%)")
    logger.info(f"95% rule (±2σ): {verification['95_rule']} (actual: {verification['actual_2std']}%)")
    logger.info(f"99.7% rule (±3σ): {verification['99_rule']} (actual: {verification['actual_3std']}%)")
    
    if all([verification['68_rule'], verification['95_rule'], verification['99_rule']]):
        logger.info("✓ Distribution follows Gaussian properties!")
    else:
        logger.warning("⚠ Distribution may not perfectly follow Gaussian properties (adjust parameters)")