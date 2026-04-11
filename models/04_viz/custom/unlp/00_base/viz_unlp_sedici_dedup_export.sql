{{ config(materialized='view') }}

WITH base AS (
    SELECT
        item_hk,
        item_id,
        title,
        subtitle,
        type,
        author,
        date_issued,
        doi,
        isbn,
        issn,
        description,
        dc_identifier_uri AS institutional_uri
    FROM {{ ref('fct_unlp_ir_item_publication') }}
)

SELECT
    base.title,
    base.subtitle,
    base.type,
    base.author,
    base.date_issued::text AS date,
    base.doi,
    base.isbn,
    base.issn,
    base.description,
    base.item_id::text AS source_id,
    'sedici'::text AS source_system,
    EXTRACT(YEAR FROM base.date_issued)::int AS publication_year,
    base.institutional_uri
FROM base
