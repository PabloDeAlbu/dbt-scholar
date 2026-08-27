WITH item_universe AS (
    SELECT handle AS handle_publicacion
    FROM {{ ref('fct_unlp_sedici_revista_publicacion') }}
),

dashboard_item AS (
    SELECT DISTINCT handle_publicacion
    FROM {{ ref('unlp_sedici_revistas_dashboard') }}
)

SELECT item_universe.handle_publicacion
FROM item_universe
LEFT JOIN dashboard_item
    USING (handle_publicacion)
WHERE dashboard_item.handle_publicacion IS NULL
