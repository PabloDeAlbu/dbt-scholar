{{ config(materialized='view') }}

with institution as (
    select
        node_id as institution_node_id,
        revision_id,
        node_uuid,
        langcode,
        is_published,
        created_at,
        changed_at,
        _load_datetime
    from {{ ref('ldg_vocsedici_node') }}
    where node_type = 'instituci_n'
      and node_id <> -1
),
institution_name as (
    select distinct on (node_id)
        node_id,
        institution_name
    from {{ ref('ldg_vocsedici_node__field_nombre_institucion') }}
    where not is_deleted
      and node_id <> -1
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
abbreviation as (
    select distinct on (node_id)
        node_id,
        abbreviation
    from {{ ref('ldg_vocsedici_node__field_abreviatura') }}
    where not is_deleted
      and node_id <> -1
      and abbreviation <> '!UNKNOWN'
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
pidu as (
    select distinct on (node_id)
        node_id,
        pidu_id
    from {{ ref('ldg_vocsedici_node__field_id_pidu') }}
    where not is_deleted
      and node_id <> -1
      and pidu_id <> '!UNKNOWN'
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
raw_source as (
    select
        i.institution_node_id,
        i.revision_id,
        i.node_uuid,
        i.langcode,
        i.is_published,
        n.institution_name,
        a.abbreviation,
        p.pidu_id,
        i.created_at,
        i.changed_at,
        i._load_datetime as load_datetime,
        i.institution_node_id::text as institution_bk
    from institution i
    left join institution_name n
        on i.institution_node_id = n.node_id
    left join abbreviation a
        on i.institution_node_id = a.node_id
    left join pidu p
        on i.institution_node_id = p.node_id
),
final as (
    select
        institution_hk,
        institution_hashdiff,
        institution_bk,
        institution_node_id,
        revision_id,
        node_uuid,
        langcode,
        is_published,
        institution_name,
        abbreviation,
        pidu_id,
        created_at,
        changed_at,
        load_datetime,
        '!VOCSEDICI'::text as source,
        coalesce(changed_at, created_at, load_datetime) as effective_from
    from raw_source s0
    cross join lateral (
        select
            {{ automate_dv.hash(columns='institution_bk', alias='institution_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'institution_bk',
                    'revision_id',
                    'node_uuid',
                    'langcode',
                    'is_published',
                    'institution_name',
                    'abbreviation',
                    'pidu_id'
                ],
                alias='institution_hashdiff',
                is_hashdiff=true
            ) }}
    ) s1
)

select * from final
