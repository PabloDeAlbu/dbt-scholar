-- fct_unlp_item_materia.sql
{{ config(materialized='table') }}

SELECT
    bm.materia::text,
    COUNT(DISTINCT bm.item_hk)::int AS items_count
FROM {{ ref('brg_sedici_item_materia') }} bm
GROUP BY 1
ORDER BY items_count DESC