WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
        publication_type_label_es,
        access_right_label_es,
        dc_relation_doi
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

record_set AS (
    SELECT DISTINCT
        bridge.record_hk,
        dim.set_id AS set_spec
    FROM {{ ref('brg_oai_record_set') }} AS bridge
    INNER JOIN {{ ref('dim_oai_set') }} AS dim
        USING (set_hk)
),

author AS (
    SELECT
        record_hk,
        STRING_AGG(
            DISTINCT NULLIF(REPLACE({{ clean_text('author') }}, '|', ' '), ''),
            '|'
            ORDER BY NULLIF(REPLACE({{ clean_text('author') }}, '|', ' '), '')
        ) AS author
    FROM {{ ref('brg_conicet_record_fil') }}
    GROUP BY record_hk
),

candidate_observed AS (
    SELECT
        base.record_hk,
        base.record_id,
        base.title,
        base.date_issued,
        base.valid_date_issued,
        base.publication_type_label_es,
        base.access_right_label_es,
        base.dc_relation_doi,
        author.author,
        target.set_spec,
        target.set_name,
        target.faculty,
        target.sedici_collection_uri,
        target.target_community_id,
        target.target_community_title,
        target.target_root_community_title,
        target.target_community_path_titles
    FROM base
    INNER JOIN record_set
        USING (record_hk)
    INNER JOIN {{ ref('conicet_unlp_repository_target') }} AS target
        USING (set_spec)
    LEFT JOIN author
        USING (record_hk)
),

candidate AS (
    SELECT
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
        publication_type_label_es,
        access_right_label_es,
        dc_relation_doi,
        author,
        STRING_AGG(DISTINCT set_spec, '|' ORDER BY set_spec) AS set_spec,
        STRING_AGG(DISTINCT set_name, '|' ORDER BY set_name) AS set_name,
        STRING_AGG(DISTINCT faculty, '|' ORDER BY faculty) AS faculty,
        MIN(sedici_collection_uri) AS sedici_collection_uri,
        target_community_id,
        MIN(target_community_title) AS target_community_title,
        MIN(target_root_community_title) AS target_root_community_title,
        MIN(target_community_path_titles) AS target_community_path_titles
    FROM candidate_observed
    GROUP BY
        record_hk,
        record_id,
        title,
        date_issued,
        valid_date_issued,
        publication_type_label_es,
        access_right_label_es,
        dc_relation_doi,
        author,
        target_community_id
),

prepared AS (
    SELECT
        'conicet'::text AS source,
        record_id::text AS id,
        NULLIF({{ clean_text('title') }}, '')::text AS title,
        NULL::text AS subtitle,
        CASE publication_type_label_es
            WHEN 'ARTÍCULO ORIGINAL' THEN 'ARTÍCULO'
            WHEN 'TESIS DOCTORAL' THEN 'TESIS'
            ELSE publication_type_label_es
        END::text AS type,
        author::text,
        CASE WHEN valid_date_issued THEN date_issued::date END AS date,
        dc_relation_doi::text AS doi,
        NULL::text AS isbn,
        NULL::text AS issn,
        NULL::text AS description,
        access_right_label_es AS access_right,
        set_spec AS target_set_spec,
        set_name AS target_set_name,
        faculty,
        sedici_collection_uri,
        target_community_id,
        target_community_title,
        target_root_community_title,
        target_community_path_titles
    FROM candidate
),

final AS (
    SELECT
        *,
        (
            title IS NOT NULL
            AND type IS NOT NULL
            AND author IS NOT NULL
            AND date IS NOT NULL
        ) AS dedup_eligible
    FROM prepared
)

SELECT *
FROM final
