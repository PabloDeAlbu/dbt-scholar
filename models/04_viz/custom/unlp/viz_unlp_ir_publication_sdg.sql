WITH base AS (
    SELECT
        publication_hk,
        has_sdg,
        item_hk,
        item_id,
        dc_identifier_uri,
        researchproduct_hk,
        researchproduct_id,
        match_scheme,
        match_pid,
        title,
        ir_type,
        publication_date_best,
        publication_type_best,
        publisher,
        best_access_right,
        organization_ror,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime,
        COALESCE(sdg_count, 0) AS sdg_count,
        sdg_values
    FROM {{ ref('fct_unlp_openaire_publication_ir_match_by_pid') }}
    WHERE has_ir_pid_match = TRUE
      AND item_hk IS NOT NULL
),

final AS (
    SELECT
        base.publication_hk,
        TRUE AS has_ir,
        base.has_sdg,
        base.item_hk,
        base.item_id,
        base.dc_identifier_uri,
        base.researchproduct_hk,
        base.researchproduct_id,
        base.match_scheme,
        base.match_pid,
        base.title,
        base.ir_type,
        base.publication_date_best,
        base.publication_type_best,
        base.publisher,
        base.best_access_right,
        base.organization_ror,
        base.unlp_first_extract_datetime,
        base.unlp_last_extract_datetime,
        base.sdg_count,
        CASE
            WHEN NULLIF(BTRIM(sdg.sdg), '') ~ '^[0-9](\D|$)' THEN
                REGEXP_REPLACE(NULLIF(BTRIM(sdg.sdg), ''), '^([0-9])(\D|$)', '0\1\2')
            ELSE NULLIF(BTRIM(sdg.sdg), '')
        END AS sdg
    FROM base
    LEFT JOIN LATERAL REGEXP_SPLIT_TO_TABLE(COALESCE(base.sdg_values, ''), '[|]') AS sdg(sdg) ON TRUE
)

SELECT * FROM final
