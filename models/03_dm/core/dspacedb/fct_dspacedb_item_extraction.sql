{{ config(
    materialized='incremental',
    unique_key='extract_cdk',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH item_hub AS (
    SELECT
        item_hk,
        SPLIT_PART(item_bk, '||', 3)::uuid AS item_uuid
    FROM {{ ref('hub_dspacedb_item') }}
),

latest_item AS (
    SELECT
        item_hk,
        item_id
    FROM {{ ref('latest_stg_dspacedb_item') }}
),

base AS (
    SELECT
        sat.extract_cdk,
        sat.item_hk,
        latest_item.item_id,
        item_hub.item_uuid,
        sat.source_label,
        sat.institution_ror,
        sat.extract_datetime,
        sat.load_datetime,
        sat.source
    FROM {{ ref('sat_dspacedb_item__extract') }} sat
    INNER JOIN item_hub USING (item_hk)
    LEFT JOIN latest_item USING (item_hk)
    {% if is_incremental() %}
    WHERE sat.load_datetime > (SELECT COALESCE(MAX(load_datetime), '1900-01-01'::timestamp) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
