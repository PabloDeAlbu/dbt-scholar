{{ config(materialized='view') }}

WITH source AS (
    SELECT
        collection_id,
        logo_bitstream_id,
        template_item_id,
        workflow_step_1,
        workflow_step_2,
        workflow_step_3,
        submitter,
        admin,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || collection_id::text AS collection_bk
    FROM {{ ref('ldg_dspacedb5_collection') }}
),
final AS (
    SELECT
        collection_hk,
        collection_bk,
        collection_id,
        logo_bitstream_id,
        template_item_id,
        workflow_step_1,
        workflow_step_2,
        workflow_step_3,
        submitter,
        admin,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM source s0
    CROSS JOIN LATERAL (
        SELECT {{ automate_dv.hash(columns='collection_bk', alias='collection_hk') }}
    ) s1
)

SELECT * FROM final
