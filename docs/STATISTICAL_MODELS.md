# Statistical Models Documentation

## Overview
This project uses 5 sophisticated statistical models to generate realistic fleet data, NOT random number generators.

## 1. Markov Chain (Route Generation)

**Mathematical Model:**
- States: S = {warehouse, highway, urban, customer}
- Transition Matrix: P(s_t | s_{t-1})
- Next state sampled from categorical distribution

**Implementation:** `data_generators/models/markov_route.py`

**Purpose:** Generate realistic GPS routes that follow natural progression patterns.

**Key Parameters:**
- φ (transition probabilities) defined in config.yaml
- Haversine distance calculation for GPS coordinates

## 2. Gaussian/Normal Distribution (Speed Modeling)

**Mathematical Model:**
- Speed ~ N(μ, σ²)
- μ = adjusted based on road type, time, weather, driver
- Formula: X = μ + σZ, where Z ~ N(0,1)

**Implementation:** `data_generators/models/gaussian_speed.py`

**Purpose:** Model vehicle speeds based on real-world driving conditions.

**Key Properties:**
- 68% of values within μ ± σ
- 95% of values within μ ± 2σ
- Not random - follows statistical distribution

## 3. Poisson Distribution (Incident Generation)

**Mathematical Model:**
- Number of incidents ~ Poisson(λ)
- PMF: P(X=k) = (λ^k × e^(-λ)) / k!
- λ = adjusted based on driver risk factors

**Implementation:** `data_generators/models/poisson_incidents.py`

**Purpose:** Model safety incidents as rare events occurring at an average rate.

**Key Properties:**
- Mean = Variance = λ
- Discrete distribution (counts)
- λ depends on experience, weather, traffic, behavior

## 4. Autoregressive AR(1) (Engine Telemetry)

**Mathematical Model:**
- X_t = φX_{t-1} + ε_t
- φ = autocorrelation coefficient (0.95)
- ε_t ~ N(0, σ²) = random innovation

**Implementation:** `data_generators/models/ar_telemetry.py`

**Purpose:** Model engine temperature with thermal inertia (current temp depends on previous).

**Key Properties:**
- Stationary when |φ| < 1
- High φ (0.95) = slow temperature changes
- Autocorrelation at lag k: ρ(k) = φ^k

## 5. Hidden Markov Model (Driver Behavior)

**Mathematical Model:**
- λ = (A, B, π)
- A = state transition matrix
- B = emission probabilities
- π = initial state distribution

**Implementation:** `data_generators/models/hmm_driver.py`

**Purpose:** Model hidden driver states (normal/aggressive/tired) that affect observable behavior.

**Key Properties:**
- Hidden states: {Normal, Aggressive, Tired}
- Observations: speed deviation, braking, reaction time
- State transitions follow Markov property

## Statistical Rigor

All models are:
1. **Mathematically sound** - Based on established probability theory
2. **Parameterized** - Not random, but based on realistic parameters
3. **Validated** - Include verification methods
4. **Documented** - With mathematical formulas in code

## References

- Markov Chains: Transition probability matrices
- Gaussian Distribution: Central Limit Theorem
- Poisson Process: Rare events modeling
- Autoregressive Models: Time series analysis
- Hidden Markov Models: State space models
