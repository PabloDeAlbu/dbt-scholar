{{ config(materialized='table') }}

WITH
source_labels AS (
    SELECT 'dspacedb'::text AS source_system, 'DSpace 7+'::text AS source_label
    UNION ALL
    SELECT 'dspacedb5'::text AS source_system, 'DSpace 5'::text AS source_label
    UNION ALL
    SELECT 'oai'::text AS source_system, 'OAI-PMH'::text AS source_label
    UNION ALL
    SELECT 'openaire'::text AS source_system, 'OpenAIRE'::text AS source_label
    UNION ALL
    SELECT 'openalex'::text AS source_system, 'OpenAlex'::text AS source_label
),

dspacedb_item_title AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(text_value) AS title
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.title'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

dspacedb_item_type AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(text_value) AS publication_type
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.type'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

dspacedb_item_date_issued AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(
            CASE
                WHEN text_value ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'
                    OR text_value ~ '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$'
                    OR text_value ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}T.*$'
                THEN {{ str_to_date('text_value') }}
            END
        ) AS publication_date
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.date.issued'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

dspacedb_publication AS (
    SELECT
        'dspacedb'::text AS source_system,
        'publication'::text AS entity_type,
        pub.item_hk AS entity_hk,
        pub.item_uuid::text AS entity_id,
        pub.institution_ror::text AS institution_ror,
        CASE
            WHEN date_issued.publication_date = DATE '0001-01-01' THEN NULL
            ELSE date_issued.publication_date
        END AS publication_date,
        CASE
            WHEN date_issued.publication_date IS NOT NULL
                AND date_issued.publication_date <> DATE '0001-01-01'
            THEN EXTRACT(YEAR FROM date_issued.publication_date)::integer
        END AS publication_year,
        NULL::text AS doi,
        title.title::text AS title,
        item_type.publication_type::text AS publication_type,
        pub.last_extract_datetime AS extract_datetime,
        pub.last_load_datetime AS load_datetime
    FROM {{ ref('fct_dspacedb_item_publication') }} pub
    LEFT JOIN dspacedb_item_title AS title
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb_item_type AS item_type
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb_item_date_issued AS date_issued
        USING (item_hk, source_label, institution_ror)
),

dspacedb5_item_title AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(text_value) AS title
    FROM {{ ref('fct_dspacedb5_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.title'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

dspacedb5_item_type AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(text_value) AS publication_type
    FROM {{ ref('fct_dspacedb5_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.type'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

dspacedb5_item_date_issued AS (
    SELECT
        item_hk,
        source_label,
        institution_ror,
        MIN(
            CASE
                WHEN text_value ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$'
                    OR text_value ~ '^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$'
                    OR text_value ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}T.*$'
                THEN {{ str_to_date('text_value') }}
            END
        ) AS publication_date
    FROM {{ ref('fct_dspacedb5_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.date.issued'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

dspacedb5_publication AS (
    SELECT
        'dspacedb5'::text AS source_system,
        'publication'::text AS entity_type,
        pub.item_hk AS entity_hk,
        pub.item_id::text AS entity_id,
        pub.institution_ror::text AS institution_ror,
        CASE
            WHEN date_issued.publication_date = DATE '0001-01-01' THEN NULL
            ELSE date_issued.publication_date
        END AS publication_date,
        CASE
            WHEN date_issued.publication_date IS NOT NULL
                AND date_issued.publication_date <> DATE '0001-01-01'
            THEN EXTRACT(YEAR FROM date_issued.publication_date)::integer
        END AS publication_year,
        NULL::text AS doi,
        title.title::text AS title,
        item_type.publication_type::text AS publication_type,
        pub.last_extract_datetime AS extract_datetime,
        pub.last_load_datetime AS load_datetime
    FROM {{ ref('fct_dspacedb5_item_publication') }} pub
    LEFT JOIN dspacedb5_item_title AS title
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb5_item_type AS item_type
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb5_item_date_issued AS date_issued
        USING (item_hk, source_label, institution_ror)
),

oai_publication AS (
    SELECT
        'oai'::text AS source_system,
        'publication'::text AS entity_type,
        pub.record_hk AS entity_hk,
        pub.record_id::text AS entity_id,
        pub.institution_ror::text AS institution_ror,
        CASE
            WHEN pub.valid_date_issued THEN pub.date_issued
        END AS publication_date,
        CASE
            WHEN pub.valid_date_issued THEN EXTRACT(YEAR FROM pub.date_issued)::integer
        END AS publication_year,
        NULL::text AS doi,
        pub.title::text AS title,
        pub.dc_type::text AS publication_type,
        pub.extract_datetime AS extract_datetime,
        pub.load_datetime AS load_datetime
    FROM {{ ref('fct_oai_record_publication') }} pub
),

openaire_researchproduct_doi AS (
    SELECT
        researchproduct_hk,
        MIN(value) AS doi
    FROM {{ ref('brg_openaire_researchproduct_pid') }}
    WHERE LOWER(scheme) = 'doi'
      AND value IS NOT NULL
      AND value <> ''
    GROUP BY researchproduct_hk
),

openaire_extraction_context AS (
    SELECT
        entity_hk::bytea AS entity_hk,
        institution_ror,
        MAX(extract_datetime) AS extract_datetime
    FROM {{ ref('fct_entity_extract') }}
    WHERE source_system = 'openaire'
      AND institution_ror IS NOT NULL
      AND institution_ror <> ''
    GROUP BY entity_hk, institution_ror
),

openaire_publication AS (
    SELECT
        'openaire'::text AS source_system,
        'publication'::text AS entity_type,
        pub.researchproduct_hk AS entity_hk,
        pub.researchproduct_id::text AS entity_id,
        extract.institution_ror::text AS institution_ror,
        pub.publication_date AS publication_date,
        CASE
            WHEN pub.publication_date IS NOT NULL THEN EXTRACT(YEAR FROM pub.publication_date)::integer
        END AS publication_year,
        doi.doi::text AS doi,
        pub.main_title::text AS title,
        pub.type::text AS publication_type,
        extract.extract_datetime AS extract_datetime,
        pub.load_datetime AS load_datetime
    FROM {{ ref('fct_openaire_researchproduct_publication') }} pub
    LEFT JOIN openaire_extraction_context AS extract
        ON pub.researchproduct_hk = extract.entity_hk
    LEFT JOIN openaire_researchproduct_doi AS doi
        USING (researchproduct_hk)
),

openalex_extraction_context AS (
    SELECT
        entity_hk::bytea AS entity_hk,
        institution_ror,
        MAX(extract_datetime) AS extract_datetime
    FROM {{ ref('fct_entity_extract') }}
    WHERE source_system = 'openalex'
      AND institution_ror IS NOT NULL
      AND institution_ror <> ''
    GROUP BY entity_hk, institution_ror
),

openalex_publication AS (
    SELECT
        'openalex'::text AS source_system,
        'publication'::text AS entity_type,
        pub.work_hk AS entity_hk,
        pub.work_id::text AS entity_id,
        extract.institution_ror::text AS institution_ror,
        dim_work.publication_date AS publication_date,
        COALESCE(
            CASE
                WHEN dim_work.publication_date IS NOT NULL THEN EXTRACT(YEAR FROM dim_work.publication_date)::integer
            END,
            CASE
                WHEN dim_work.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM dim_work.publication_year)::integer
            END
        ) AS publication_year,
        NULLIF(dim_work.doi, '-')::text AS doi,
        dim_work.title::text AS title,
        dim_work.type::text AS publication_type,
        extract.extract_datetime AS extract_datetime,
        sat_work.load_datetime AS load_datetime
    FROM {{ ref('fct_openalex_work_publication') }} pub
    INNER JOIN {{ ref('dim_openalex_work') }} dim_work
        USING (work_hk)
    INNER JOIN {{ ref('latest_sat_openalex_work') }} sat_work
        USING (work_hk)
    LEFT JOIN openalex_extraction_context AS extract
        ON pub.work_hk = extract.entity_hk
),

organization AS (
    SELECT
        organization_ror,
        organization_name
    FROM {{ ref('dim_organization') }}
),

unioned AS (
    SELECT
        source_system,
        entity_type,
        entity_hk,
        entity_id,
        institution_ror,
        publication_date,
        publication_year,
        doi,
        title,
        publication_type,
        extract_datetime,
        load_datetime
    FROM dspacedb_publication

    UNION ALL

    SELECT
        source_system,
        entity_type,
        entity_hk,
        entity_id,
        institution_ror,
        publication_date,
        publication_year,
        doi,
        title,
        publication_type,
        extract_datetime,
        load_datetime
    FROM dspacedb5_publication

    UNION ALL

    SELECT
        source_system,
        entity_type,
        entity_hk,
        entity_id,
        institution_ror,
        publication_date,
        publication_year,
        doi,
        title,
        publication_type,
        extract_datetime,
        load_datetime
    FROM oai_publication

    UNION ALL

    SELECT
        source_system,
        entity_type,
        entity_hk,
        entity_id,
        institution_ror,
        publication_date,
        publication_year,
        doi,
        title,
        publication_type,
        extract_datetime,
        load_datetime
    FROM openaire_publication

    UNION ALL

    SELECT
        source_system,
        entity_type,
        entity_hk,
        entity_id,
        institution_ror,
        publication_date,
        publication_year,
        doi,
        title,
        publication_type,
        extract_datetime,
        load_datetime
    FROM openalex_publication
)

SELECT
    unioned.source_system,
    source_labels.source_label,
    unioned.entity_type,
    unioned.entity_hk,
    unioned.entity_id,
    unioned.institution_ror,
    COALESCE(organization.organization_name, unioned.institution_ror) AS institution_label,
    unioned.publication_date,
    unioned.publication_year,
    unioned.doi,
    unioned.title,
    unioned.publication_type,
    unioned.extract_datetime,
    unioned.load_datetime
FROM unioned
INNER JOIN source_labels
    USING (source_system)
LEFT JOIN organization
    ON unioned.institution_ror = organization.organization_ror
