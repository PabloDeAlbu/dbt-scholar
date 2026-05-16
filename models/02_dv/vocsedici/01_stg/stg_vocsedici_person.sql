{{ config(materialized='view') }}

with source as (
    select distinct on (node_id)
        node_id as person_node_id,
        revision_id,
        node_uuid,
        langcode,
        is_published,
        created_at,
        changed_at,
        _load_datetime as load_datetime
    from {{ ref('ldg_vocsedici_node') }}
    where node_type = 'persona'
      and node_id <> -1
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        revision_id desc
)

select * from source
