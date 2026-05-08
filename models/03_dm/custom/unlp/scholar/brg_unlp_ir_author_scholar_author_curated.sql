{{ config(materialized='table') }}

WITH manual_map AS (
    SELECT *
    FROM {{ ref('ldg_unlp_ir_scholar_author_manual_map') }}
    WHERE review_status = 'approved'
),

ir_author AS (
    SELECT
        ir_author_hk,
        authority,
        author_name_preferred,
        author_name_normalized,
        has_authority_control,
        item_count
    FROM {{ ref('dim_unlp_ir_author') }}
),

scholar_author AS (
    SELECT
        scholar_author_hk,
        scholar_user_id,
        author_name,
        profile_url,
        affiliation,
        verified_email_domain,
        has_unlp_verified_email,
        mentions_unlp_affiliation,
        is_unlp_profile,
        current_cited_by_count
    FROM {{ ref('dim_unlp_scholar_author') }}
),

resolved_ir_author AS (
    SELECT
        manual_map.authority AS seed_authority,
        manual_map.ir_author_hk AS seed_ir_author_hk,
        manual_map.scholar_user_id,
        manual_map.review_status,
        manual_map.review_note,
        manual_map.reviewer,
        COALESCE(ir_by_hk.ir_author_hk, ir_by_authority.ir_author_hk) AS ir_author_hk
    FROM manual_map
    LEFT JOIN ir_author AS ir_by_hk
        ON manual_map.ir_author_hk IS NOT NULL
       AND ir_by_hk.ir_author_hk::text = manual_map.ir_author_hk
    LEFT JOIN ir_author AS ir_by_authority
        ON manual_map.authority IS NOT NULL
       AND ir_by_authority.authority = manual_map.authority
),

final AS (
    SELECT
        resolved.ir_author_hk,
        scholar.scholar_author_hk,
        ir.authority,
        ir.author_name_preferred AS ir_author_name,
        ir.author_name_normalized AS ir_author_name_normalized,
        ir.has_authority_control,
        ir.item_count AS ir_item_count,
        scholar.scholar_user_id,
        scholar.author_name AS scholar_author_name,
        scholar.profile_url,
        scholar.affiliation,
        scholar.verified_email_domain,
        scholar.has_unlp_verified_email,
        scholar.mentions_unlp_affiliation,
        scholar.is_unlp_profile,
        scholar.current_cited_by_count,
        resolved.review_status,
        resolved.review_note,
        resolved.reviewer,
        'manual_seed'::text AS match_method,
        1.0::numeric(6, 4) AS match_score,
        TRUE AS is_unique_match
    FROM resolved_ir_author AS resolved
    INNER JOIN ir_author AS ir
        USING (ir_author_hk)
    INNER JOIN scholar_author AS scholar
        USING (scholar_user_id)
)

SELECT * FROM final
