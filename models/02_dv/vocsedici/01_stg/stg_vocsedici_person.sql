{{ config(materialized='view') }}

with person as (
    select
        node_id as person_node_id,
        revision_id,
        node_uuid,
        langcode,
        is_published,
        created_at,
        changed_at,
        _load_datetime
    from {{ ref('ldg_vocsedici_node') }}
    where node_type = 'persona'
      and node_id <> -1
),
nombre as (
    select distinct on (node_id)
        node_id,
        nombre
    from {{ ref('ldg_vocsedici_node__field_nombre') }}
    where not is_deleted
      and node_id <> -1
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
apellido as (
    select distinct on (node_id)
        node_id,
        apellido
    from {{ ref('ldg_vocsedici_node__field_apellido') }}
    where not is_deleted
      and node_id <> -1
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
orcid as (
    select distinct on (node_id)
        node_id,
        orcid
    from {{ ref('ldg_vocsedici_node__field_orcid') }}
    where not is_deleted
      and node_id <> -1
      and orcid <> '!UNKNOWN'
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
email as (
    select distinct on (node_id)
        node_id,
        email
    from {{ ref('ldg_vocsedici_node__field_mail') }}
    where not is_deleted
      and node_id <> -1
      and email <> '!UNKNOWN'
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
google_scholar as (
    select distinct on (node_id)
        node_id,
        google_scholar
    from {{ ref('ldg_vocsedici_node__field_google_scholar') }}
    where not is_deleted
      and node_id <> -1
      and google_scholar <> '!UNKNOWN'
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
researchgate as (
    select distinct on (node_id)
        node_id,
        researchgate
    from {{ ref('ldg_vocsedici_node__field_researchgate') }}
    where not is_deleted
      and node_id <> -1
      and researchgate <> '!UNKNOWN'
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
raw_source as (
    select
        p.person_node_id,
        p.revision_id,
        p.node_uuid,
        p.langcode,
        p.is_published,
        n.nombre,
        a.apellido,
        trim(concat_ws(' ', nullif(n.nombre, '!UNKNOWN'), nullif(a.apellido, '!UNKNOWN'))) as full_name,
        o.orcid,
        e.email,
        gs.google_scholar,
        rg.researchgate,
        p.created_at,
        p.changed_at,
        p._load_datetime as load_datetime,
        p.person_node_id::text as person_bk
    from person p
    left join nombre n
        on p.person_node_id = n.node_id
    left join apellido a
        on p.person_node_id = a.node_id
    left join orcid o
        on p.person_node_id = o.node_id
    left join email e
        on p.person_node_id = e.node_id
    left join google_scholar gs
        on p.person_node_id = gs.node_id
    left join researchgate rg
        on p.person_node_id = rg.node_id
),
final as (
    select
        person_hk,
        person_hashdiff,
        person_bk,
        person_node_id,
        revision_id,
        node_uuid,
        langcode,
        is_published,
        nullif(full_name, '') as full_name,
        nombre,
        apellido,
        orcid,
        email,
        google_scholar,
        researchgate,
        created_at,
        changed_at,
        load_datetime,
        '!VOCSEDICI'::text as source,
        coalesce(changed_at, created_at, load_datetime) as effective_from
    from raw_source s0
    cross join lateral (
        select
            {{ automate_dv.hash(columns='person_bk', alias='person_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'person_bk',
                    'revision_id',
                    'node_uuid',
                    'langcode',
                    'is_published',
                    'full_name',
                    'nombre',
                    'apellido',
                    'orcid',
                    'email',
                    'google_scholar',
                    'researchgate'
                ],
                alias='person_hashdiff',
                is_hashdiff=true
            ) }}
    ) s1
)

select * from final
