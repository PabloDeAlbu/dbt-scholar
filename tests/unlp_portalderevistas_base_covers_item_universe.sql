WITH item_universe AS (
    SELECT item_id
    FROM {{ ref('fct_unlp_portalderevistas_journal_item') }}
),

dashboard_item AS (
    SELECT DISTINCT item_id
    FROM {{ ref('unlp_portalderevistas_00_base') }}
)

SELECT item_universe.item_id
FROM item_universe
LEFT JOIN dashboard_item
    USING (item_id)
WHERE dashboard_item.item_id IS NULL
