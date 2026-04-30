{{ config(materialized='view') }}

WITH base AS (
    SELECT
        item_hk,
        dc_identifier_uri,
        date_issued,
        subject,
        author,
        type,
        title,
        subtitle,
        issn,
        doi,
        handle,
        isbn
    FROM {{ ref('fct_unlp_ir_item_publication') }}
)

SELECT
    base.date_issued::text AS date,
    base.subject,
    base.author,
    base.dc_identifier_uri AS id,
    base.type,
    base.title,
    base.subtitle,
    base.issn,
    base.doi,
    NULL::text AS eid,
    NULL::text AS pmid,
    base.dc_identifier_uri AS handle,
    CASE
        WHEN NULLIF(base.doi, '') IS NOT NULL
            THEN 'https://doi.org/' || REPLACE(base.doi, '|', '|https://doi.org/')
    END AS doi_,
    base.isbn,
    NULL::text AS citation
FROM base
