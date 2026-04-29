## Metric decomposition framework

Step 1: Is it a data pipeline issue? Check raw table row counts first.
Step 2: Which segment is driving it? Platform, geography, cohort, user type.
Step 3: New, retained, or resurrected users?
Step 4: Correlated with recent release, experiment, or external event?
Step 5: What is the recommended action?

DAU decomposition:
DAU = New + Retained + Resurrected users
Each component points to a different team and solution.

## Growth accounting framework

DAU = New + Retained + Resurrected users
Churned = users active yesterday but not today

Why it matters:
DAU can stay flat while product is dying
If new users replace churned users at same rate DAU looks stable
But retention is deteriorating - a leading indicator of future DAU drop
Growth accounting reveals what DAU hides

## North Star Metric framework

One metric that captures the core value delivered to users
All other metrics are inputs that drive the North Star
When North Star drops - decompose into inputs to find root cause

## Metric decomposition rule
Never report a metric movement without segmenting it first
Aggregate metrics hide the story - segments tell it