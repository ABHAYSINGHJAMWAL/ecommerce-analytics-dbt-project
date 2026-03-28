{% macro date_spine(datepart, start_date, end_date) %}

    WITH date_spine AS (
        SELECT
            DATE_ADD(
                DATE('{{ start_date }}'),
                INTERVAL spine_number {{ datepart }}
            ) AS spine_date
        FROM
            UNNEST(
                GENERATE_ARRAY(
                    0,
                    DATE_DIFF(DATE('{{ end_date }}'), DATE('{{ start_date }}'), {{ datepart }})
                )
            ) AS spine_number
    )

    SELECT spine_date FROM date_spine

{% endmacro %}