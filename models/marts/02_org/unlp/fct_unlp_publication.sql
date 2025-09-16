WITH base AS (
    SELECT DISTINCT id 
    FROM {{ref('fct_sedici_item')}}
    INNER JOIN {{ref('brg_sedici_item_author')}} USING (item_hk)
    WHERE authority is not NULL
),

openaire AS (
    SELECT * 
    FROM {{'hub_openaire_pid'}} 
 
)

SELECT * FROM base