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

Before concluding DAU dropped I verified the pipeline is clean — raw events table has normal volume for yesterday. The drop is real. Segmenting by platform shows the drop is entirely on Android — iOS DAU is normal. Growth accounting shows retained users dropped, not new users — so this is an engagement problem not an acquisition problem. Android retained users dropped 35%. Engineering shipped an Android update 2 days ago. My recommendation is to check the Android release for bugs and consider a rollback while investigating.


revenue = order * average order value(aov)
order = active users * order frequency
average order value (aov) = item price * item per order 

Revenue dropped 8% but order count is actually flat. AOV dropped 12%. Digging into AOV — average items per order is unchanged, but item prices dropped because we ran a discount campaign last month that wasn't run this month. The revenue drop is a baseline comparison issue — last month was artificially inflated by the campaign. On a like-for-like basis revenue is actually up 3%.


Memorize this structure. It works for any metric question:
1. Verify it's real (pipeline check)
2. Segment by platform, geography, user type
3. Decompose into components (DAU = New + Retained + Resurrected)
4. Correlate with recent changes (releases, experiments, campaigns)
5. Give a recommendation with confidence level