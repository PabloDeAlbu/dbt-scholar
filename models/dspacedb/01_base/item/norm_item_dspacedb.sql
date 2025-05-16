{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        i.uuid,
        doi.doi,
        handle.handle,
        dateissued.dateissued,
        type.type,
        title.title,
        title_lang,
        i.discoverable,
        i.in_archive,
        i.last_modified,
        i.load_datetime,
        owning_collection,
        submitter_id,
        withdrawn
    FROM {{ source('dspacedb', 'item') }} i
    INNER JOIN {{ ref('mid_item_title_dspacedb') }} title ON i.uuid = title.uuid
    INNER JOIN {{ ref('mid_item_dateissued_dspacedb') }} dateissued ON i.uuid = dateissued.uuid 
    INNER JOIN {{ ref('mid_item_dateaccessioned_dspacedb') }} dateaccessioned ON i.uuid = dateaccessioned.uuid 
    INNER JOIN {{ ref('mid_item_type_dspacedb') }} type ON i.uuid = type.uuid 
    LEFT JOIN {{ ref('mid_item_doi_dspacedb') }} doi ON i.uuid = doi.uuid 
    INNER JOIN {{ ref('mid_item_handle_dspacedb') }} handle ON i.uuid = handle.uuid 
    WHERE dateissued.dateissued > '1800-01-01'::timestamp
)

SELECT * FROM base