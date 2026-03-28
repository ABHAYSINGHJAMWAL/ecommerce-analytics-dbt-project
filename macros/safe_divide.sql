{% macro safe_divide(numerator,denominator) %}
case when {{ denominator }} = 0 or {{ denominator }} is null
then 0
else round(cast({{ numerator }} as float64)/ {{ denominator }},4)
end
{% endmacro %}