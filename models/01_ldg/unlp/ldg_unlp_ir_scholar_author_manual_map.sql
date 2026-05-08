{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('unlp_ir_scholar_author_manual_map') }}
),

renamed AS (
    SELECT
        NULLIF(TRIM(authority), '')::text AS authority,
        NULLIF(TRIM(ir_author_hk), '')::text AS ir_author_hk,
        NULLIF(TRIM(scholar_user_id), '')::text AS scholar_user_id,
        LOWER(COALESCE(NULLIF(TRIM(review_status), ''), 'pending'))::text AS review_status,
        NULLIF(TRIM(review_note), '')::text AS review_note,
        NULLIF(TRIM(reviewer), '')::text AS reviewer
    FROM source_data
),

filtered AS (
    SELECT *
    FROM renamed
    WHERE scholar_user_id IS NOT NULL
      AND (authority IS NOT NULL OR ir_author_hk IS NOT NULL)
),

final AS (
    SELECT
        authority,
        ir_author_hk,
        scholar_user_id,
        review_status,
        review_note,
        reviewer
    FROM filtered
)

SELECT * FROM final
