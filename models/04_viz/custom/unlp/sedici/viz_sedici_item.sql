{{ config(materialized='table') }}

SELECT
    item_hk,
    item_id,
    title,
    dc_identifier_uri AS id,
    date_issued,
    date_accessioned,
    subject,
    type,
    subtype,
    author,
    author_count,
    issn,
    isbn,
    doi,
    discoverable
FROM {{ ref('fct_unlp_ir_item_publication') }}
