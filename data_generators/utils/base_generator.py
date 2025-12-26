"""
Fleet Analytics Pipeline - Base Generator Class
Step 2.1: Common utilities for all data generators

This module provides the foundation for all statistical model generators.
"""

import logging
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
import sys


class BaseGenerator:
    """
    Base class for all data generators.
    Provides common functionality: logging, config loading, validation.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize base generator with configuration.
        
        Args:
            config_path: Path to configuration file (default: config/config.yaml)
        """
        self.config_path = config_path or "config/config.yaml"
        self.config = self._load_config()
        self._setup_logging()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dictionary containing configuration
        """
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {self.config_path}")
            
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            return config
        except Exception as e:
            print(f"Error loading config: {e}")
            raise
    
    def _setup_logging(self):
        """
        Setup logging configuration using loguru.
        """
        # Remove default handler
        logger.remove()
        
        # Add console handler
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
            level="INFO"
        )
        
        # Add file handler
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        logger.add(
            f"logs/{self.__class__.__name__.lower()}_{datetime.now().strftime('%Y%m%d')}.log",
            rotation="100 MB",
            retention="30 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function} - {message}"
        )
    
    def validate_data(self, data: Dict[str, Any], required_fields: list) -> bool:
        """
        Validate that data contains all required fields.
        
        Args:
            data: Dictionary to validate
            required_fields: List of required field names
            
        Returns:
            True if valid, raises ValueError if invalid
        """
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return True
    
    def validate_range(self, value: float, min_val: float, max_val: float, 
                       field_name: str) -> bool:
        """
        Validate that a numeric value is within specified range.
        
        Args:
            value: Value to validate
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            field_name: Name of field (for error messages)
            
        Returns:
            True if valid, raises ValueError if invalid
        """
        if not (min_val <= value <= max_val):
            error_msg = f"{field_name} value {value} outside valid range [{min_val}, {max_val}]"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return True
    
    def to_json(self, data: Dict[str, Any]) -> str:
        """
        Convert dictionary to JSON string.
        
        Args:
            data: Dictionary to convert
            
        Returns:
            JSON string
        """
        return json.dumps(data, default=str)
    
    def from_json(self, json_str: str) -> Dict[str, Any]:
        """
        Parse JSON string to dictionary.
        
        Args:
            json_str: JSON string
            
        Returns:
            Dictionary
        """
        return json.loads(json_str)
    
    def get_timestamp(self) -> str:
        """
        Get current timestamp in ISO format.
        
        Returns:
            ISO formatted timestamp string
        """
        return datetime.utcnow().isoformat()
    
    def log_statistics(self, stats: Dict[str, Any]):
        """
        Log statistical information.
        
        Args:
            stats: Dictionary containing statistics
        """
        logger.info(f"Statistics: {json.dumps(stats, indent=2)}")


# Utility functions for data generation
def generate_vehicle_id(index: int) -> str:
    """Generate unique vehicle ID."""
    return f"VEH-{index:05d}"


def generate_driver_id(index: int) -> str:
    """Generate unique driver ID."""
    return f"DRV-{index:05d}"


def generate_warehouse_id(index: int) -> str:
    """Generate unique warehouse ID."""
    return f"WH-{index:03d}"


def generate_customer_id(index: int) -> str:
    """Generate unique customer ID."""
    return f"CUST-{index:06d}"


def generate_delivery_id(index: int) -> str:
    """Generate unique delivery ID."""
    return f"DEL-{index:08d}"


def generate_incident_id(index: int) -> str:
    """Generate unique incident ID."""
    return f"INC-{index:08d}"


# Constants for data generation
VEHICLE_TYPES = ["Van", "Truck", "Heavy Truck", "Refrigerated Truck"]
VEHICLE_MAKES = ["Volvo", "Mercedes", "MAN", "Scania", "DAF", "Iveco"]

DRIVER_EXPERIENCE_LEVELS = ["Novice", "Intermediate", "Expert", "Master"]

WAREHOUSE_CITIES = [
    "Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad",
    "Multan", "Hyderabad", "Gujranwala", "Peshawar", "Quetta",
    "Sialkot", "Bahawalpur", "Sargodha", "Sukkur", "Larkana",
    "Mardan", "Kasur", "Rahim Yar Khan", "Sahiwal", "Okara"
]

ROAD_TYPES = ["highway", "urban", "rural"]

WEATHER_CONDITIONS = ["clear", "rain", "fog", "dust"]

INCIDENT_TYPES = [
    "harsh_braking", 
    "harsh_acceleration", 
    "speeding", 
    "sharp_turn",
    "sudden_lane_change"
]

if __name__ == "__main__":
    # Test the base generator
    generator = BaseGenerator()
    
    # Test validation
    test_data = {
        "vehicle_id": "VEH-00001",
        "speed": 75.5,
        "timestamp": generator.get_timestamp()
    }
    
    try:
        generator.validate_data(test_data, ["vehicle_id", "speed", "timestamp"])
        generator.validate_range(test_data["speed"], 0, 150, "speed")
        logger.info("Validation tests passed!")
        
        # Test JSON conversion
        json_str = generator.to_json(test_data)
        logger.info(f"JSON: {json_str}")
        
        # Test statistics logging
        generator.log_statistics({
            "total_records": 1000,
            "avg_speed": 65.3,
            "max_speed": 120.5
        })
        
    except ValueError as e:
        logger.error(f"Validation failed: {e}")