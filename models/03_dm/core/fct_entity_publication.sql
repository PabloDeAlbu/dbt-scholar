{{ config(materialized='table') }}

WITH
-- Preferred DSpace metadata values used to enrich the publication fact with
-- publication_date, publication_year, title and publication_type.
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
        ) AS publication_date,
        MIN(
            CASE
                WHEN {{ str_to_date('text_value') }} <> DATE '9999-12-31'
                THEN EXTRACT(YEAR FROM {{ str_to_date('text_value') }})::integer
            END
        ) AS publication_year
    FROM {{ ref('fct_dspacedb_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.date.issued'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

-- DSpace publication fact is the base grain; metadata only completes analytical
-- fields already modeled in core without recalculating latest logic from raw satellites.
dspacedb_publication AS (
    SELECT
        'dspacedb'::text AS source_system,
        'publication'::text AS entity_type,
        pub.item_hk AS entity_hk,
        pub.item_uuid::text AS entity_id,
        pub.institution_ror,
        date_issued.publication_date,
        date_issued.publication_year,
        pub.last_load_datetime AS load_datetime,
        NULL::text AS doi,
        title.title,
        item_type.publication_type,
        pub.source_label::text AS source_label,
        pub.last_extract_datetime AS extract_datetime
    FROM {{ ref('fct_dspacedb_item_publication') }} pub
    LEFT JOIN dspacedb_item_title AS title
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb_item_type AS item_type
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb_item_date_issued AS date_issued
        USING (item_hk, source_label, institution_ror)
),

-- Preferred DSpace 5 metadata values used to enrich the publication fact with
-- publication_date, publication_year, title and publication_type.
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
        ) AS publication_date,
        MIN(
            CASE
                WHEN {{ str_to_date('text_value') }} <> DATE '9999-12-31'
                THEN EXTRACT(YEAR FROM {{ str_to_date('text_value') }})::integer
            END
        ) AS publication_year
    FROM {{ ref('fct_dspacedb5_item_metadata') }}
    WHERE metadatafield_fullname = 'dc.date.issued'
      AND text_value IS NOT NULL
    GROUP BY item_hk, source_label, institution_ror
),

-- DSpace 5 publication fact is the base grain; metadata only completes analytical
-- fields already available in core, using dc.date.issued as the preferred date source.
dspacedb5_publication AS (
    SELECT
        'dspacedb5'::text AS source_system,
        'publication'::text AS entity_type,
        pub.item_hk AS entity_hk,
        pub.item_id::text AS entity_id,
        pub.institution_ror,
        date_issued.publication_date,
        date_issued.publication_year,
        pub.last_load_datetime AS load_datetime,
        NULL::text AS doi,
        title.title,
        item_type.publication_type,
        pub.source_label::text AS source_label,
        pub.last_extract_datetime AS extract_datetime
    FROM {{ ref('fct_dspacedb5_item_publication') }} pub
    LEFT JOIN dspacedb5_item_title AS title
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb5_item_type AS item_type
        USING (item_hk, source_label, institution_ror)
    LEFT JOIN dspacedb5_item_date_issued AS date_issued
        USING (item_hk, source_label, institution_ror)
),

-- OAI publication fact already exposes record title, date_issued and extraction
-- context. We reuse valid_date_issued and derive publication_year from that date.
oai_publication AS (
    SELECT
        'oai'::text AS source_system,
        'publication'::text AS entity_type,
        pub.record_hk AS entity_hk,
        pub.record_id::text AS entity_id,
        pub.institution_ror,
        CASE
            WHEN pub.valid_date_issued THEN pub.date_issued
        END AS publication_date,
        CASE
            WHEN pub.valid_date_issued THEN EXTRACT(YEAR FROM pub.date_issued)::integer
        END AS publication_year,
        pub.load_datetime,
        NULL::text AS doi,
        pub.title,
        pub.dc_type::text AS publication_type,
        NULL::text AS source_label,
        pub.extract_datetime
    FROM {{ ref('fct_oai_record_publication') }} pub
),

-- OpenAIRE publication fact is enriched with the extraction-context institution
-- recorded in fct_entity_extract and one DOI candidate from the PID bridge.
-- publication_date is reused as provided by the publication fact.
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
        extract.institution_ror,
        pub.publication_date,
        CASE
            WHEN pub.publication_date IS NOT NULL THEN EXTRACT(YEAR FROM pub.publication_date)::integer
        END AS publication_year,
        pub.load_datetime,
        doi.doi,
        pub.main_title::text AS title,
        pub.type::text AS publication_type,
        pub.source::text AS source_label,
        extract.extract_datetime
    FROM {{ ref('fct_openaire_researchproduct_publication') }} pub
    LEFT JOIN openaire_extraction_context AS extract
        ON pub.researchproduct_hk = extract.entity_hk
    LEFT JOIN openaire_researchproduct_doi AS doi
        USING (researchproduct_hk)
),

-- OpenAlex publication fact is enriched with bibliographic fields from dim_openalex_work
-- and the extraction-context institution recorded in fct_entity_extract.
-- publication_year falls back to the year-only field when publication_date is missing.
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
        extract.institution_ror,
        dim_work.publication_date,
        COALESCE(
            CASE
                WHEN dim_work.publication_date IS NOT NULL THEN EXTRACT(YEAR FROM dim_work.publication_date)::integer
            END,
            CASE
                WHEN dim_work.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM dim_work.publication_year)::integer
            END
        ) AS publication_year,
        sat_work.load_datetime,
        NULLIF(dim_work.doi, '-') AS doi,
        dim_work.title::text AS title,
        dim_work.type::text AS publication_type,
        sat_work.source::text AS source_label,
        extract.extract_datetime
    FROM {{ ref('fct_openalex_work_publication') }} pub
    INNER JOIN {{ ref('dim_openalex_work') }} dim_work
        USING (work_hk)
    INNER JOIN {{ ref('latest_sat_openalex_work') }} sat_work
        USING (work_hk)
    LEFT JOIN openalex_extraction_context AS extract
        ON pub.work_hk = extract.entity_hk
),

unioned AS (
    SELECT * FROM dspacedb_publication
    UNION ALL
    SELECT * FROM dspacedb5_publication
    UNION ALL
    SELECT * FROM oai_publication
    UNION ALL
    SELECT * FROM openaire_publication
    UNION ALL
    SELECT * FROM openalex_publication
),

final AS (
    SELECT
        source_system,
        entity_type,
        entity_hk,
        entity_id,
        institution_ror,
        CASE
            WHEN publication_date = DATE '0001-01-01' THEN NULL
            ELSE publication_date
        END AS publication_date,
        CASE
            WHEN publication_date = DATE '0001-01-01' THEN NULL
            ELSE publication_year
        END AS publication_year,
        load_datetime,
        doi,
        title,
        publication_type,
        source_label,
        extract_datetime
    FROM unioned
)

SELECT * FROM final
