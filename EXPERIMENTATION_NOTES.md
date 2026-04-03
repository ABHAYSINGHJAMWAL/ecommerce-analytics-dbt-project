## A/B testing core concepts

Control = current experience, Treatment = new experience
Random assignment ensures groups are equivalent before experiment
Any post-experiment difference is attributable to the change

Type I error (false positive): conclude treatment works when it doesn't
- Alpha = 0.05 means 5% chance of this error
- Business impact: ship a bad change

Type II error (false negative): conclude treatment doesn't work when it does  
- Beta = 0.20 means 20% chance of this error
- Business impact: miss a real improvement

Statistical significance: p-value < 0.05 means less than 5% chance
the observed difference happened by random chance alone

## The peeking problem

Checking results early and stopping when p < 0.05 inflates false positive rate
Real false positive rate can reach 20-30% instead of the intended 5%
Cause: p-values fluctuate with small samples, dip below 0.05 by chance

Fix 1: Pre-register sample size, don't check until reached
Fix 2: Sequential testing methods designed for continuous monitoring

Interview signal: most companies peek. Knowing this is wrong and why
puts you in the top 10% of experimentation candidates.

## Sample size and power

Required before starting any experiment — not after.

Inputs:
- Baseline conversion rate (current metric value)
- MDE: minimum detectable effect (smallest improvement worth detecting)
- Power: 1 - beta, typically 0.80
- Alpha: significance level, typically 0.05

Key insight: smaller MDE = larger sample needed = longer experiment
Key insight: higher power = larger sample needed
Key insight: high variance metrics need more samples

Running an underpowered experiment wastes time and produces unreliable results.