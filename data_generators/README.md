# Data Generators

This module contains all statistical models for generating realistic fleet data.

## Statistical Models Used:
1. **Markov Chain** - Route generation
2. **Gaussian Distribution** - Speed modeling
3. **Poisson Distribution** - Incident generation
4. **Autoregressive AR(1)** - Engine telemetry
5. **Hidden Markov Model** - Driver behavior

## Structure:
- `models/` - Statistical model implementations
- `utils/` - Helper functions and base classes
- `main_generator.py` - Main data generation orchestrator
