1)semantic model translates technical ,complex data into business friendly terms , ensuring conistent metrics . it acts as single source of truth eliminating incosistent reports

my mart_growth_metrics table  - semantic model says  event_date is dimension ,dau is measure ,user_id is entity
Column      	Role	 Purpose
event_date	Dimension	Slices metrics by time (day/week/month) 
dau        	Measure	    Quantitative value to be aggregated/analyzed.
user_id 	Entity	        The unique subject (user) being measured.


2)metric -  metric: daily_active_users
           → takes the dau measure from the growth_metrics semantic model
           → aggregation: sum
           → queryable by any dimension in that semantic model

           Mart model approach: DAU is hardcoded SQL in mart_growth_metrics. If the definition changes, you update SQL in one place but every downstream model that copies this logic breaks.
Semantic layer approach: DAU is defined once in YAML. Every tool — Power BI, Metabase, any future tool — queries the semantic definition. Change it once, everything updates.

# Semantic layer notes

## Why MetricFlow exists
Before semantic layer: DAU defined in mart_growth_metrics SQL.
If a PM queries mart_growth_metrics and an analyst queries
mart_product_metrics they might get different DAU numbers.
This destroys data trust.

After semantic layer: DAU defined once in metrics.yml.
Every tool queries the same definition. One change updates everything.

## What I defined
Semantic models: growth_metrics (on mart_growth_metrics), revenue (on fct_revenue)
Metrics: DAU, WAU, MAU, stickiness ratio, net revenue, AOV

## Stickiness ratio is a derived metric
It references two other metrics (DAU and MAU) rather than raw measures.
This is the power of the semantic layer — you can build metrics on metrics.

## Interview answer
"I implemented dbt's MetricFlow semantic layer to define core metrics
like DAU, stickiness, and AOV as single-source definitions. Instead of
DAU being hardcoded SQL in multiple models, it's defined once in YAML
and any BI tool gets the same number. This is how companies like Airbnb
solve the problem of metric inconsistency across teams."