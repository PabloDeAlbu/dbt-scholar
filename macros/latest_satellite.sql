{% macro latest_satellite(relation, key_column, order_column='load_datetime') %}
{%- set ranked_alias = 'ranked_latest_satellite' -%}
{%- set row_number_column = '__latest_satellite_rn' -%}
{%- set order_columns = order_column.split(',') -%}
(
    -- Devuelve una fila vigente por clave de satélite.
    -- `key_column` no es el input del hash key: es la columna ya materializada
    -- usada para particionar el satélite, normalmente un *_hk. Puede ser una
    -- lista SQL si la vigencia se define por más de una columna.
    SELECT {{ ranked_alias }}.*
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ key_column }}
                -- Las columnas de `order_column` se aplican en orden y siempre
                -- descendentes: la primera define vigencia; las siguientes desempatan.
                ORDER BY
                    {%- for column in order_columns %}
                    {{ column | trim }} DESC{% if not loop.last %},{% endif %}
                    {%- endfor %}
            ) AS {{ row_number_column }}
        FROM {{ relation }}
    ) AS {{ ranked_alias }}
    WHERE {{ row_number_column }} = 1
)
{% endmacro %}
