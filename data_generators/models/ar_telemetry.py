"""
Fleet Analytics Pipeline - Autoregressive Engine Telemetry Generator
Step 2.5: Generate engine temperature and fuel consumption using AR(1) model

Mathematical Model - Autoregressive Model of Order 1: AR(1)
X_t = φ × X_{t-1} + ε_t

where:
- X_t = current temperature
- X_{t-1} = previous temperature
- φ (phi) = autocorrelation coefficient (0 < φ < 1)
- ε_t = random shock/innovation ~ N(0, σ²)

Physical meaning:
- Current engine temp depends on previous temp (thermal inertia)
- φ close to 1 = high persistence (slow temperature changes)
- φ close to 0 = low persistence (rapid temperature changes)
"""

import numpy as np
from typing import List, Dict, Tuple
from dataclasses import dataclass
import sys
sys.path.append('.')
from data_generators.utils.base_generator import BaseGenerator
from loguru import logger


@dataclass
class EngineTelemetry:
    """Represents engine sensor readings."""
    vehicle_id: str
    timestamp: str
    engine_temp_celsius: float
    coolant_temp_celsius: float
    oil_pressure_psi: float
    fuel_level_percent: float
    fuel_consumption_lph: float  # Liters per hour
    rpm: int
    throttle_position_percent: float


class ARTelemetryGenerator(BaseGenerator):
    """
    Generate engine telemetry using Autoregressive AR(1) model.
    
    Mathematical Foundation:
    An AR(1) process is a first-order autoregressive model where the current
    value depends linearly on the previous value plus random noise.
    
    General form: X_t = c + φX_{t-1} + ε_t
    
    For mean-centered: X_t = μ + φ(X_{t-1} - μ) + ε_t
    
    Properties:
    - Mean: E[X_t] = μ
    - Variance: Var(X_t) = σ²/(1-φ²)
    - Autocorrelation at lag k: ρ(k) = φ^k
    - Stationary if |φ| < 1
    
    Application to Engine Temperature:
    - Engine heats up gradually (not instant changes)
    - Previous temperature strongly predicts next temperature
    - Random fluctuations from driving conditions
    """
    
    def __init__(self):
        super().__init__()
        
        # AR(1) parameters for engine temperature from config
        ar_config = self.config['statistical_models']['ar_temperature']
        
        # φ (phi) - autocorrelation coefficient
        # High φ (0.9-0.99) = slow temperature changes (thermal inertia)
        self.phi = ar_config['phi']  # 0.95
        
        # Mean operating temperature (μ)
        self.mean_temp = ar_config['mean_temp']  # 85°C (optimal)
        
        # Innovation standard deviation (σ)
        self.innovation_std = ar_config['std']  # 5°C
        
        # Normal operating ranges
        self.temp_ranges = {
            'normal': (75, 95),      # Optimal: 75-95°C
            'warning': (95, 105),    # Getting hot: 95-105°C
            'critical': (105, 120)   # Overheating: >105°C
        }
        
        # Fuel consumption parameters
        # Fuel consumption depends on speed, RPM, throttle
        self.base_fuel_rate = 8.0  # Base: 8 L/h at idle
        
        logger.info("AR(1) Telemetry Generator initialized")
        logger.info(f"φ = {self.phi}, μ = {self.mean_temp}°C, σ = {self.innovation_std}°C")
    
    def generate_temperature_ar1(self, 
                                previous_temp: float,
                                external_load: float = 1.0) -> float:
        """
        Generate next temperature value using AR(1) model.
        
        Formula:
        T_t = μ + φ(T_{t-1} - μ) + ε_t
        
        where:
        - T_t = temperature at time t
        - T_{t-1} = temperature at time t-1
        - μ = long-run mean temperature
        - φ = persistence coefficient
        - ε_t ~ N(0, σ²) = random innovation
        
        Args:
            previous_temp: Temperature at previous time step (°C)
            external_load: Load factor (1.0 = normal, >1.0 = high load)
            
        Returns:
            Temperature at current time step (°C)
        """
        # Generate random innovation (shock)
        # ε_t ~ N(0, σ²)
        epsilon = np.random.normal(0, self.innovation_std)
        
        # Adjust mean for external load
        # High load (uphill, acceleration) increases mean temp
        adjusted_mean = self.mean_temp + (external_load - 1.0) * 15
        
        # AR(1) formula
        # X_t = μ + φ(X_{t-1} - μ) + ε_t
        temp = adjusted_mean + self.phi * (previous_temp - adjusted_mean) + epsilon
        
        # Physical constraints
        # Temperature can't be below ambient or above critical
        temp = np.clip(temp, 30, 125)
        
        return round(temp, 2)
    
    def generate_temperature_series(self,
                                   duration_steps: int,
                                   initial_temp: float = None,
                                   load_profile: List[float] = None) -> List[float]:
        """
        Generate a time series of temperatures.
        
        Args:
            duration_steps: Number of time steps
            initial_temp: Starting temperature (default: mean temp)
            load_profile: External load at each step (default: constant 1.0)
            
        Returns:
            List of temperature values
        """
        if initial_temp is None:
            # Start near operating temperature
            initial_temp = self.mean_temp + np.random.normal(0, 5)
        
        if load_profile is None:
            load_profile = [1.0] * duration_steps
        
        temperatures = [initial_temp]
        
        for step in range(1, duration_steps):
            next_temp = self.generate_temperature_ar1(
                previous_temp=temperatures[-1],
                external_load=load_profile[step]
            )
            temperatures.append(next_temp)
        
        return temperatures
    
    def calculate_fuel_consumption(self,
                                  speed_kmh: float,
                                  rpm: int,
                                  throttle_percent: float,
                                  engine_temp: float) -> float:
        """
        Calculate fuel consumption based on driving conditions.
        
        Fuel consumption model (simplified):
        Fuel = base_rate + speed_factor + rpm_factor + throttle_factor - temp_factor
        
        Args:
            speed_kmh: Vehicle speed (km/h)
            rpm: Engine RPM
            throttle_percent: Throttle position (0-100%)
            engine_temp: Engine temperature (°C)
            
        Returns:
            Fuel consumption in liters per hour
        """
        # Base fuel rate
        fuel = self.base_fuel_rate
        
        # Speed contribution (higher speed = more fuel)
        # Quadratic relationship (air resistance)
        speed_factor = (speed_kmh / 100) ** 2 * 3.0
        
        # RPM contribution
        rpm_factor = (rpm / 1000) * 0.5
        
        # Throttle contribution (heavy acceleration = more fuel)
        throttle_factor = (throttle_percent / 100) * 4.0
        
        # Temperature factor (cold engine = more fuel)
        # Optimal at 85°C, higher consumption when cold
        if engine_temp < self.mean_temp:
            temp_penalty = (self.mean_temp - engine_temp) * 0.05
        else:
            temp_penalty = 0
        
        fuel = fuel + speed_factor + rpm_factor + throttle_factor + temp_penalty
        
        # Realistic bounds
        fuel = np.clip(fuel, 2.0, 30.0)  # 2-30 L/h
        
        return round(fuel, 2)
    
    def generate_full_telemetry(self,
                               vehicle_id: str,
                               speed_kmh: float,
                               previous_telemetry: EngineTelemetry = None,
                               external_load: float = 1.0) -> EngineTelemetry:
        """
        Generate complete engine telemetry snapshot.
        
        Args:
            vehicle_id: Vehicle identifier
            speed_kmh: Current vehicle speed
            previous_telemetry: Previous telemetry reading (for AR model)
            external_load: External load factor
            
        Returns:
            EngineTelemetry object with all sensor readings
        """
        # Generate engine temperature using AR(1)
        if previous_telemetry is None:
            # First reading - start near operating temp
            engine_temp = self.mean_temp + np.random.normal(0, 10)
            coolant_temp = engine_temp - 5
        else:
            engine_temp = self.generate_temperature_ar1(
                previous_telemetry.engine_temp_celsius,
                external_load
            )
            # Coolant temp follows engine temp with lag
            coolant_temp = previous_telemetry.coolant_temp_celsius + \
                          0.3 * (engine_temp - previous_telemetry.engine_temp_celsius)
        
        # Calculate RPM based on speed
        # Simplified: RPM ≈ speed × gear_ratio
        # Assume automatic transmission in appropriate gear
        if speed_kmh < 20:
            rpm = int(800 + speed_kmh * 40)  # Low gear
        elif speed_kmh < 60:
            rpm = int(1500 + (speed_kmh - 20) * 25)  # Medium gear
        else:
            rpm = int(2500 + (speed_kmh - 60) * 15)  # High gear
        rpm = min(rpm, 4500)  # RPM limit
        
        # Throttle position correlates with load
        throttle = min(100, external_load * 50 + np.random.uniform(0, 20))
        
        # Oil pressure (normal range: 25-65 psi)
        oil_pressure = 30 + (rpm / 100) + np.random.normal(0, 3)
        oil_pressure = np.clip(oil_pressure, 20, 70)
        
        # Fuel consumption
        fuel_consumption = self.calculate_fuel_consumption(
            speed_kmh, rpm, throttle, engine_temp
        )
        
        # Fuel level (decreases based on consumption)
        if previous_telemetry is None:
            fuel_level = np.random.uniform(50, 100)
        else:
            # Decrease fuel level (assuming 3-second intervals)
            fuel_decrease = (fuel_consumption / 3600) * 3 * (100 / 80)  # 80L tank
            fuel_level = max(0, previous_telemetry.fuel_level_percent - fuel_decrease)
        
        return EngineTelemetry(
            vehicle_id=vehicle_id,
            timestamp=self.get_timestamp(),
            engine_temp_celsius=round(engine_temp, 2),
            coolant_temp_celsius=round(coolant_temp, 2),
            oil_pressure_psi=round(oil_pressure, 2),
            fuel_level_percent=round(fuel_level, 2),
            fuel_consumption_lph=fuel_consumption,
            rpm=rpm,
            throttle_position_percent=round(throttle, 2)
        )
    
    def verify_ar1_properties(self, temperatures: List[float]) -> Dict[str, float]:
        """
        Verify that generated temperatures follow AR(1) properties.
        
        Key property: Autocorrelation at lag 1 should equal φ
        
        Args:
            temperatures: Time series of temperatures
            
        Returns:
            Dictionary with AR(1) verification metrics
        """
        temps_array = np.array(temperatures)
        
        # Calculate sample mean and variance
        sample_mean = np.mean(temps_array)
        sample_var = np.var(temps_array)
        
        # Calculate autocorrelation at lag 1
        # Cor(X_t, X_{t-1})
        lag1_correlation = np.corrcoef(temps_array[:-1], temps_array[1:])[0, 1]
        
        # Theoretical variance for AR(1): σ²/(1-φ²)
        theoretical_var = (self.innovation_std ** 2) / (1 - self.phi ** 2)
        
        return {
            'sample_mean': round(sample_mean, 2),
            'theoretical_mean': self.mean_temp,
            'sample_variance': round(sample_var, 2),
            'theoretical_variance': round(theoretical_var, 2),
            'lag1_autocorrelation': round(lag1_correlation, 3),
            'theoretical_phi': self.phi,
            'ar1_property_holds': abs(lag1_correlation - self.phi) < 0.1
        }


if __name__ == "__main__":
    # Test the AR(1) Telemetry Generator
    generator = ARTelemetryGenerator()
    
    logger.info("Testing AR(1) Engine Telemetry Generator...")
    
    # Test 1: Generate temperature series
    logger.info("\n=== Test 1: Temperature Time Series ===")
    
    # Generate 200 time steps (10 minutes at 3-second intervals)
    temps = generator.generate_temperature_series(duration_steps=200)
    
    logger.info(f"Generated {len(temps)} temperature readings")
    logger.info(f"Initial temp: {temps[0]:.2f}°C")
    logger.info(f"Final temp: {temps[-1]:.2f}°C")
    logger.info(f"Mean temp: {np.mean(temps):.2f}°C (target: {generator.mean_temp}°C)")
    logger.info(f"Std dev: {np.std(temps):.2f}°C")
    
    # Test 2: Verify AR(1) properties
    logger.info("\n=== Test 2: AR(1) Property Verification ===")
    
    verification = generator.verify_ar1_properties(temps)
    logger.info(f"Verification results: {verification}")
    
    if verification['ar1_property_holds']:
        logger.info("✓ Temperature series follows AR(1) properties!")
    else:
        logger.warning("⚠ AR(1) properties may not hold perfectly (larger sample needed)")
    
    # Test 3: Generate complete telemetry sequence
    logger.info("\n=== Test 3: Complete Telemetry Sequence ===")
    
    vehicle_id = "VEH-00001"
    speeds = [0, 30, 60, 80, 90, 85, 70, 50, 30, 0]  # Speed profile
    loads = [1.0, 1.2, 1.5, 1.3, 1.1, 1.0, 0.9, 0.8, 0.7, 0.6]  # Load profile
    
    telemetry_sequence = []
    previous = None
    
    for speed, load in zip(speeds, loads):
        telemetry = generator.generate_full_telemetry(
            vehicle_id=vehicle_id,
            speed_kmh=speed,
            previous_telemetry=previous,
            external_load=load
        )
        telemetry_sequence.append(telemetry)
        previous = telemetry
    
    logger.info(f"\nGenerated {len(telemetry_sequence)} telemetry snapshots")
    logger.info("\nSample telemetry readings:")
    for i, t in enumerate([telemetry_sequence[0], telemetry_sequence[4], telemetry_sequence[-1]]):
        logger.info(f"\nReading {i+1}:")
        logger.info(f"  Engine Temp: {t.engine_temp_celsius}°C")
        logger.info(f"  RPM: {t.rpm}")
        logger.info(f"  Fuel Consumption: {t.fuel_consumption_lph} L/h")
        logger.info(f"  Fuel Level: {t.fuel_level_percent}%")