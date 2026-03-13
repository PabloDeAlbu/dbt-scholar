WITH base AS (
    SELECT
        publication_hk,
        item_hk,
        item_id,
        researchproduct_hk,
        researchproduct_id,
        match_scheme,
        match_pid,
        title,
        publication_date_best,
        publication_type_best,
        publisher,
        best_access_right,
        organization_ror,
        unlp_first_extract_datetime,
        unlp_last_extract_datetime,
        sdg_values
    FROM {{ ref('fct_unlp_publication') }}
    WHERE publication_source = 'matched'
      AND has_sdg = TRUE
      AND sdg_values IS NOT NULL
),

sdg AS (
    SELECT
        publication_hk,
        NULLIF(BTRIM(REGEXP_SPLIT_TO_TABLE(sdg_values, '[|]')), '') AS sdg
    FROM base
),

final AS (
    SELECT
        base.publication_hk,
        base.item_hk,
        base.item_id,
        base.researchproduct_hk,
        base.researchproduct_id,
        base.match_scheme,
        base.match_pid,
        base.title,
        base.publication_date_best,
        base.publication_type_best,
        base.publisher,
        base.best_access_right,
        base.organization_ror,
        base.unlp_first_extract_datetime,
        base.unlp_last_extract_datetime,
        sdg.sdg
    FROM base
    INNER JOIN sdg USING (publication_hk)
    WHERE sdg.sdg IS NOT NULL
)

SELECT * FROM final
