WITH base AS (
    SELECT
        set_spec,
        set_name,
        sedici_collection_uri,
        faculty,
        publication_year,
        is_open_access_candidate,
        pdf_share_pct,
        sedici_article_count
    FROM {{ ref('fct_unlp_conicet_publication_candidate') }}
),

sedici_center_handle AS (
    SELECT
        centers.set_spec,
        centers.sedici_collection_uri,
        SUBSTRING(centers.sedici_collection_uri FROM '(10915/[0-9]+)') AS sedici_handle
    FROM {{ ref('ldg_unlp_centros_conicet') }} AS centers
),

sedici_center_community AS (
    SELECT
        handle_map.set_spec,
        handle_map.sedici_collection_uri,
        handle.resource_id::integer AS sedici_community_id
    FROM sedici_center_handle AS handle_map
    INNER JOIN {{ ref('ldg_dspacedb5_handle') }} AS handle
        ON handle.handle = handle_map.sedici_handle
    WHERE handle.resource_type_id = 4
),

sedici_publication_count AS (
    SELECT
        center.set_spec,
        COUNT(DISTINCT pub.item_hk) AS calculated_sedici_publication_count
    FROM sedici_center_community AS center
    INNER JOIN {{ ref('fct_unlp_publication') }} AS pub
        ON CONCAT(' > ', COALESCE(pub.owning_community_path_ids, ''), ' > ')
           LIKE CONCAT('% > ', center.sedici_community_id::text, ' > %')
    GROUP BY center.set_spec
),

final AS (
    SELECT
        set_spec,
        set_name,
        sedici_collection_uri,
        faculty,
        COUNT(*) AS resource_count,
        COUNT(*) FILTER (WHERE is_open_access_candidate) AS open_access_count,
        ROUND(
            (
                COUNT(*) FILTER (WHERE is_open_access_candidate)::numeric
                / NULLIF(COUNT(*), 0)::numeric
            ) * 100,
            2
        ) AS open_access_ratio_pct,
        MIN(publication_year) AS first_publication_year,
        MAX(publication_year) AS last_publication_year,
        MIN(pdf_share_pct) AS seed_pdf_share_pct,
        MIN(sedici_article_count) AS seed_sedici_article_count,
        COALESCE(MIN(spc.calculated_sedici_publication_count), 0) AS calculated_sedici_publication_count
    FROM base
    LEFT JOIN sedici_publication_count AS spc
        USING (set_spec)
    GROUP BY
        set_spec,
        set_name,
        sedici_collection_uri,
        faculty
)

SELECT * FROM final
