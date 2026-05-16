{{ config(materialized='view') }}

with source as (
    select
        node_id as institution_node_id,
        revision_id,
        node_uuid,
        langcode,
        is_published,
        created_at,
        changed_at,
        _load_datetime as load_datetime
    from {{ ref('ldg_vocsedici_node') }}
    where node_type = 'instituci_n'
      and node_id <> -1
)

select * from source
