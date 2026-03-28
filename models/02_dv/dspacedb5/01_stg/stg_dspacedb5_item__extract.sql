{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        item_id::text AS item_id,
        item_bk,
        _source_label::text AS source_label,
        _institution_ror::text AS institution_ror,
        COALESCE(_extract_datetime::timestamp, _load_datetime::timestamp) AS extract_datetime,
        _load_datetime::timestamp AS load_datetime
    FROM {{ ref('ldg_dspacedb5_item') }}
),

final AS (
    SELECT
        item_hk,
        item_bk,
        item_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source,
        {{ automate_dv.hash(
            columns=['item_hk', 'extract_datetime'],
            alias='extract_cdk'
        ) }},
        {{ automate_dv.hash(
            columns=['extract_datetime'],
            alias='item_extract_hashdiff',
            is_hashdiff=true
        ) }}
    FROM (
        SELECT
            item_id,
            item_bk,
            source_label,
            institution_ror,
            extract_datetime,
            load_datetime,
            source_label AS source
        FROM raw_source
    ) s0
    CROSS JOIN LATERAL (
        SELECT {{ automate_dv.hash(columns='item_bk', alias='item_hk') }}
    ) s1
)

SELECT * FROM final
