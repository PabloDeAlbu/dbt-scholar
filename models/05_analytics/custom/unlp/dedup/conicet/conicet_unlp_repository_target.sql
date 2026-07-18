{{ config(materialized = 'view') }}

WITH base AS (
    SELECT
        set_spec,
        set_name,
        sedici_collection_uri,
        faculty,
        SUBSTRING(sedici_collection_uri FROM '(10915/[0-9]+)') AS sedici_handle
    FROM {{ ref('ldg_unlp_centros_conicet') }}
),

handle AS (
    SELECT
        handle,
        resource_id AS community_id,
        source_label,
        institution_ror,
        base_url
    FROM {{ ref('ldg_dspacedb5_handle') }}
    WHERE resource_type_id = 4
),

community AS (
    SELECT
        community_id,
        community_title,
        root_community_id,
        root_community_title,
        community_path_ids,
        community_path_titles,
        source_label,
        institution_ror,
        base_url
    FROM {{ ref('dim_dspacedb5_community') }}
),

final AS (
    SELECT
        base.set_spec,
        base.set_name,
        base.faculty,
        base.sedici_collection_uri,
        base.sedici_handle,
        community.community_id AS target_community_id,
        community.community_title AS target_community_title,
        community.root_community_id AS target_root_community_id,
        community.root_community_title AS target_root_community_title,
        community.community_path_ids AS target_community_path_ids,
        community.community_path_titles AS target_community_path_titles
    FROM base
    INNER JOIN handle
        ON handle.handle = base.sedici_handle
    INNER JOIN community
        ON community.community_id = handle.community_id
       AND community.source_label = handle.source_label
       AND community.institution_ror = handle.institution_ror
       AND community.base_url = handle.base_url
)

SELECT *
FROM final
