"""
Fleet Analytics Pipeline - Hidden Markov Model for Driver Behavior
Step 2.6: Model driver behavior states using HMM

Mathematical Model - Hidden Markov Model (HMM):

Hidden States (S): {Normal, Aggressive, Tired}
Observable Outputs (O): Speed, acceleration, braking patterns

Components:
1. State Transition Matrix (A): P(s_t | s_{t-1})
2. Emission Probabilities (B): P(o_t | s_t)
3. Initial State Distribution (π): P(s_0)

Forward Algorithm:
α_t(i) = P(o_1,...,o_t, s_t=i | λ)

This models the hidden psychological/physical state of the driver
that we cannot observe directly, only through driving patterns.
"""

import numpy as np
from typing import List, Dict, Tuple
from enum import Enum
from dataclasses import dataclass
import sys
sys.path.append('.')
from data_generators.utils.base_generator import BaseGenerator
from loguru import logger


class DriverState(Enum):
    """Hidden states representing driver condition."""
    NORMAL = "normal"          # Alert, safe driving
    AGGRESSIVE = "aggressive"  # Speeding, harsh maneuvers
    TIRED = "tired"           # Fatigue, slower reactions


@dataclass
class DriverBehaviorObservation:
    """Observable driving patterns (emissions)."""
    speed_deviation: float      # Deviation from speed limit (positive = speeding)
    acceleration_intensity: float  # How aggressively accelerating (0-1)
    braking_intensity: float    # How aggressively braking (0-1)
    steering_smoothness: float  # How smooth steering is (0-1, 1=smooth)
    reaction_time_ms: int       # Estimated reaction time
    lane_keeping: float         # Lane keeping quality (0-1, 1=perfect)


class HMMDriverBehavior(BaseGenerator):
    """
    Model driver behavior using Hidden Markov Model.
    
    Mathematical Foundation:
    An HMM is characterized by:
    - λ = (A, B, π)
    
    Where:
    - A = State transition matrix [N×N]
      A[i][j] = P(state_j at t+1 | state_i at t)
    
    - B = Emission probability distribution
      B[i] = P(observation | state_i)
    
    - π = Initial state distribution
      π[i] = P(state_i at t=0)
    
    Key Assumptions:
    1. Markov property: Future state depends only on current state
    2. Output independence: Observation depends only on current state
    
    Application:
    - We observe driving patterns (speed, braking, etc.)
    - We infer hidden driver state (normal, aggressive, tired)
    - State transitions model how driver condition changes over time
    """
    
    def __init__(self):
        super().__init__()
        
        # Define states
        self.states = [DriverState.NORMAL, DriverState.AGGRESSIVE, DriverState.TIRED]
        self.num_states = len(self.states)
        
        # State Transition Matrix (A)
        # Rows = current state, Columns = next state
        # A[i][j] = P(next_state=j | current_state=i)
        self.transition_matrix = np.array([
            # To:   Normal  Aggressive  Tired
            [0.85,  0.10,      0.05],   # From: Normal
            [0.30,  0.60,      0.10],   # From: Aggressive
            [0.20,  0.05,      0.75]    # From: Tired
        ])
        
        # Verify transition matrix is stochastic
        assert np.allclose(self.transition_matrix.sum(axis=1), 1.0)
        
        # Initial State Distribution (π)
        # Most drivers start in normal state
        self.initial_distribution = np.array([0.80, 0.15, 0.05])
        
        # Emission Parameters (B)
        # For each state, define the distribution of observable behavior
        # Using Gaussian distributions for continuous observations
        
        self.emission_params = {
            DriverState.NORMAL: {
                'speed_deviation': {'mean': 0, 'std': 5},  # Near speed limit
                'acceleration_intensity': {'mean': 0.3, 'std': 0.15},  # Moderate
                'braking_intensity': {'mean': 0.2, 'std': 0.1},  # Gentle
                'steering_smoothness': {'mean': 0.8, 'std': 0.1},  # Smooth
                'reaction_time_ms': {'mean': 700, 'std': 100},  # Normal
                'lane_keeping': {'mean': 0.9, 'std': 0.1}  # Good
            },
            DriverState.AGGRESSIVE: {
                'speed_deviation': {'mean': 15, 'std': 8},  # Speeding
                'acceleration_intensity': {'mean': 0.8, 'std': 0.15},  # Hard
                'braking_intensity': {'mean': 0.7, 'std': 0.2},  # Hard
                'steering_smoothness': {'mean': 0.4, 'std': 0.15},  # Jerky
                'reaction_time_ms': {'mean': 500, 'std': 80},  # Quick but risky
                'lane_keeping': {'mean': 0.6, 'std': 0.2}  # Poor (lane changes)
            },
            DriverState.TIRED: {
                'speed_deviation': {'mean': -5, 'std': 5},  # Below speed limit
                'acceleration_intensity': {'mean': 0.2, 'std': 0.1},  # Gentle
                'braking_intensity': {'mean': 0.3, 'std': 0.15},  # Moderate
                'steering_smoothness': {'mean': 0.6, 'std': 0.2},  # Wavering
                'reaction_time_ms': {'mean': 1200, 'std': 200},  # Slow
                'lane_keeping': {'mean': 0.5, 'std': 0.25}  # Poor (drifting)
            }
        }
        
        logger.info("HMM Driver Behavior Model initialized")
        logger.info(f"States: {[s.value for s in self.states]}")
        logger.info(f"Transition Matrix:\n{self.transition_matrix}")
    
    def get_initial_state(self) -> DriverState:
        """
        Sample initial driver state from initial distribution π.
        
        Returns:
            Initial DriverState
        """
        state_idx = np.random.choice(
            self.num_states,
            p=self.initial_distribution
        )
        return self.states[state_idx]
    
    def transition_state(self, current_state: DriverState) -> DriverState:
        """
        Transition to next state based on transition matrix A.
        
        Formula:
        P(S_t = j | S_{t-1} = i) = A[i][j]
        
        Args:
            current_state: Current hidden state
            
        Returns:
            Next hidden state
        """
        current_idx = self.states.index(current_state)
        
        # Sample next state from transition probabilities
        next_idx = np.random.choice(
            self.num_states,
            p=self.transition_matrix[current_idx]
        )
        
        return self.states[next_idx]
    
    def emit_observation(self, state: DriverState) -> DriverBehaviorObservation:
        """
        Generate observable behavior given hidden state (Emission B).
        
        For each state, observations are sampled from Gaussian distributions.
        
        Formula:
        P(O_t | S_t = i) ~ N(μ_i, σ_i²)
        
        Args:
            state: Current hidden state
            
        Returns:
            DriverBehaviorObservation with observable metrics
        """
        params = self.emission_params[state]
        
        # Sample each observable from its distribution
        speed_dev = np.random.normal(
            params['speed_deviation']['mean'],
            params['speed_deviation']['std']
        )
        
        accel = np.random.normal(
            params['acceleration_intensity']['mean'],
            params['acceleration_intensity']['std']
        )
        accel = np.clip(accel, 0, 1)
        
        brake = np.random.normal(
            params['braking_intensity']['mean'],
            params['braking_intensity']['std']
        )
        brake = np.clip(brake, 0, 1)
        
        steering = np.random.normal(
            params['steering_smoothness']['mean'],
            params['steering_smoothness']['std']
        )
        steering = np.clip(steering, 0, 1)
        
        reaction = int(np.random.normal(
            params['reaction_time_ms']['mean'],
            params['reaction_time_ms']['std']
        ))
        reaction = max(300, min(reaction, 2000))
        
        lane = np.random.normal(
            params['lane_keeping']['mean'],
            params['lane_keeping']['std']
        )
        lane = np.clip(lane, 0, 1)
        
        return DriverBehaviorObservation(
            speed_deviation=round(speed_dev, 2),
            acceleration_intensity=round(accel, 3),
            braking_intensity=round(brake, 3),
            steering_smoothness=round(steering, 3),
            reaction_time_ms=reaction,
            lane_keeping=round(lane, 3)
        )
    
    def generate_behavior_sequence(self, 
                                  duration_steps: int) -> Tuple[List[DriverState], 
                                                                 List[DriverBehaviorObservation]]:
        """
        Generate a complete sequence of hidden states and observations.
        
        This simulates a driver's journey where their internal state
        (which we can't see) affects their observable driving behavior.
        
        Args:
            duration_steps: Number of time steps to simulate
            
        Returns:
            Tuple of (hidden_states, observations)
        """
        hidden_states = []
        observations = []
        
        # Initial state
        current_state = self.get_initial_state()
        
        for step in range(duration_steps):
            # Record current state (in reality, this is hidden!)
            hidden_states.append(current_state)
            
            # Generate observable behavior based on current state
            observation = self.emit_observation(current_state)
            observations.append(observation)
            
            # Transition to next state
            current_state = self.transition_state(current_state)
        
        return hidden_states, observations
    
    def infer_state_from_observation(self, 
                                    observation: DriverBehaviorObservation) -> Dict[DriverState, float]:
        """
        Infer probability of each state given an observation (simplified).
        
        In full HMM, this would use forward-backward algorithm.
        Here we use a simplified likelihood approach.
        
        Formula:
        P(S | O) ∝ P(O | S) × P(S)
        
        Args:
            observation: Observed driving behavior
            
        Returns:
            Dictionary mapping states to probabilities
        """
        likelihoods = {}
        
        for state in self.states:
            params = self.emission_params[state]
            
            # Calculate likelihood using Gaussian PDF
            # Simplified: multiply independent likelihoods
            likelihood = 1.0
            
            # Speed deviation likelihood
            speed_like = self._gaussian_pdf(
                observation.speed_deviation,
                params['speed_deviation']['mean'],
                params['speed_deviation']['std']
            )
            likelihood *= speed_like
            
            # Reaction time likelihood
            reaction_like = self._gaussian_pdf(
                observation.reaction_time_ms,
                params['reaction_time_ms']['mean'],
                params['reaction_time_ms']['std']
            )
            likelihood *= reaction_like
            
            likelihoods[state] = likelihood
        
        # Normalize to get probabilities
        total = sum(likelihoods.values())
        probabilities = {state: lik/total for state, lik in likelihoods.items()}
        
        return probabilities
    
    def _gaussian_pdf(self, x: float, mean: float, std: float) -> float:
        """
        Calculate Gaussian probability density function.
        
        Formula:
        f(x) = (1/(σ√(2π))) × exp(-(x-μ)²/(2σ²))
        
        Args:
            x: Value to evaluate
            mean: Mean (μ)
            std: Standard deviation (σ)
            
        Returns:
            Probability density
        """
        variance = std ** 2
        numerator = np.exp(-((x - mean) ** 2) / (2 * variance))
        denominator = np.sqrt(2 * np.pi * variance)
        return numerator / denominator
    
    def calculate_state_statistics(self, 
                                   states: List[DriverState]) -> Dict[str, any]:
        """
        Calculate statistics about state sequence.
        
        Args:
            states: List of driver states
            
        Returns:
            Dictionary with state statistics
        """
        state_counts = {state: states.count(state) for state in set(states)}
        total = len(states)
        
        return {
            'counts': state_counts,
            'percentages': {
                state: round(count/total * 100, 2) 
                for state, count in state_counts.items()
            },
            'total_steps': total
        }


if __name__ == "__main__":
    # Test the HMM Driver Behavior Model
    generator = HMMDriverBehavior()
    
    logger.info("Testing HMM Driver Behavior Model...")
    
    # Test 1: Generate behavior sequence
    logger.info("\n=== Test 1: Behavior Sequence Generation ===")
    
    duration = 100  # 5 minutes at 3-second intervals
    hidden_states, observations = generator.generate_behavior_sequence(duration)
    
    logger.info(f"Generated {len(hidden_states)} time steps")
    
    # Calculate state statistics
    stats = generator.calculate_state_statistics(hidden_states)
    logger.info(f"State distribution: {stats['percentages']}")
    
    # Test 2: Show sample observations for each state
    logger.info("\n=== Test 2: Sample Observations by State ===")
    
    for state in [DriverState.NORMAL, DriverState.AGGRESSIVE, DriverState.TIRED]:
        state_indices = [i for i, s in enumerate(hidden_states) if s == state]
        if state_indices:
            idx = state_indices[0]
            obs = observations[idx]
            logger.info(f"\n{state.value.upper()} state observation:")
            logger.info(f"  Speed deviation: {obs.speed_deviation:+.1f} km/h")
            logger.info(f"  Acceleration: {obs.acceleration_intensity:.2f}")
            logger.info(f"  Braking: {obs.braking_intensity:.2f}")
            logger.info(f"  Reaction time: {obs.reaction_time_ms} ms")
            logger.info(f"  Lane keeping: {obs.lane_keeping:.2f}")
    
    # Test 3: State inference from observation
    logger.info("\n=== Test 3: State Inference ===")
    
    test_obs = observations[10]
    true_state = hidden_states[10]
    inferred_probs = generator.infer_state_from_observation(test_obs)
    
    logger.info(f"True state: {true_state.value}")
    logger.info("Inferred probabilities:")
    for state, prob in sorted(inferred_probs.items(), key=lambda x: -x[1]):
        logger.info(f"  {state.value}: {prob:.4f}")