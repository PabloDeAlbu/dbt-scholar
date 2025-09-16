WITH base AS (
    SELECT 
        researchproduct_hk,
        type,
        ROW_NUMBER() OVER (
          PARTITION BY researchproduct_hk
          ORDER BY load_datetime DESC
        ) AS rn
    FROM {{ref('sat_openaire_instance')}}
)

SELECT * FROM base