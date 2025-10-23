{% macro latest_satellite(relation, key_column, order_column='load_datetime') %}
{%- set ranked_alias = 'ranked_latest_satellite' -%}
{%- set row_number_column = '__latest_satellite_rn' -%}
(
    SELECT {{ ranked_alias }}.*
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ key_column }}
                ORDER BY {{ order_column }} DESC
            ) AS {{ row_number_column }}
        FROM {{ relation }}
    ) AS {{ ranked_alias }}
    WHERE {{ row_number_column }} = 1
)
{% endmacro %}
