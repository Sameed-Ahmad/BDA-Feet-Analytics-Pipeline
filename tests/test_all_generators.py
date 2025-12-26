"""
Comprehensive test suite for all statistical models.
"""

import sys
sys.path.append('.')

from data_generators.utils.base_generator import BaseGenerator
from data_generators.models.markov_route import MarkovRouteGenerator
from data_generators.models.gaussian_speed import GaussianSpeedGenerator, TimeOfDay, DriverBehavior
from data_generators.models.poisson_incidents import PoissonIncidentGenerator
from data_generators.models.ar_telemetry import ARTelemetryGenerator
from data_generators.models.hmm_driver import HMMDriverBehavior, DriverState
from data_generators.main_generator import FleetDataGenerator

from loguru import logger


def test_all_models():
    """Test all statistical models."""
    
    logger.info("="*60)
    logger.info("COMPREHENSIVE STATISTICAL MODELS TEST")
    logger.info("="*60)
    
    # Test 1: Markov Chain
    logger.info("\n1. Testing Markov Chain Route Generator...")
    route_gen = MarkovRouteGenerator()
    route = route_gen.generate_route("Karachi", (24.9, 67.1), 30)
    distance = route_gen.calculate_route_distance(route)
    logger.info(f"   ✓ Generated route with {len(route)} waypoints, {distance:.2f} km")
    
    # Test 2: Gaussian Distribution
    logger.info("\n2. Testing Gaussian Speed Generator...")
    speed_gen = GaussianSpeedGenerator()
    speeds = speed_gen.generate_speed_profile('highway', 10)
    stats = speed_gen.calculate_statistics(speeds)
    logger.info(f"   ✓ Generated {len(speeds)} speed readings, mean: {stats['mean']} km/h")
    
    # Test 3: Poisson Distribution
    logger.info("\n3. Testing Poisson Incident Generator...")
    incident_gen = PoissonIncidentGenerator()
    lambda_val = incident_gen.calculate_lambda(
        'Novice', DriverBehavior.AGGRESSIVE, 'rain', 'heavy', TimeOfDay.EVENING_RUSH
    )
    logger.info(f"   ✓ Calculated λ = {lambda_val:.3f} incidents/hour")
    
    # Test 4: Autoregressive AR(1)
    logger.info("\n4. Testing AR(1) Telemetry Generator...")
    telemetry_gen = ARTelemetryGenerator()
    temps = telemetry_gen.generate_temperature_series(100)
    logger.info(f"   ✓ Generated {len(temps)} temperature readings")
    
    # Test 5: Hidden Markov Model
    logger.info("\n5. Testing HMM Driver Behavior...")
    hmm_gen = HMMDriverBehavior()
    states, obs = hmm_gen.generate_behavior_sequence(50)
    logger.info(f"   ✓ Generated {len(states)} state transitions")
    
    # Test 6: Main Generator
    logger.info("\n6. Testing Main Fleet Data Generator...")
    fleet_gen = FleetDataGenerator()
    fleet_gen.generate_dimension_data()
    logger.info(f"   ✓ Generated dimension data:")
    logger.info(f"      - {len(fleet_gen.vehicles)} vehicles")
    logger.info(f"      - {len(fleet_gen.drivers)} drivers")
    logger.info(f"      - {len(fleet_gen.warehouses)} warehouses")
    logger.info(f"      - {len(fleet_gen.customers)} customers")
    
    logger.info("\n" + "="*60)
    logger.info("ALL TESTS PASSED! ✓")
    logger.info("="*60)


if __name__ == "__main__":
    test_all_models()
