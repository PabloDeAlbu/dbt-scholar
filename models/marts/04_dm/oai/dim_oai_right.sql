WITH base AS (
    SELECT 
        hub_right.rights,
        hub_right.right_hk
    FROM {{ ref('hub_oai_right') }} hub_right
)

SELECT * FROM base
