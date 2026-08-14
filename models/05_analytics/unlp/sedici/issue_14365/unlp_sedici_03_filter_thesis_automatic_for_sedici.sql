{{ config(materialized='view') }}

SELECT
    sedici_item_id AS id,
    koha_url AS "mods.recordInfo.recordContentSource[es]"
FROM {{ ref('unlp_sedici_03_base_thesis_koha_sedici_confirmed') }}
WHERE match_source = 'automatic'
