{# WITH base AS (
    SELECT 
        record_col_hk,
        record_hk,
        col_hk,
        load_datetime
    FROM {{ ref('link_oai_record_col') }} 
)
 #}
SELECT 2
 {# * FROM base #}
