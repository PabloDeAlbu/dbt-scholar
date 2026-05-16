with source as (
    select *
    from {{ ref('stg_vocsedici_person') }}
),
final as (
    select
        s.person_node_id,
        revision_id,
        langcode,
        is_published as esta_publicada,
        n.nombre,
        a.apellido,
        trim(concat_ws(' ', n.nombre, a.apellido)) as nombre_completo,
        o.orcid,
        e.email,
        gs.google_scholar,
        rg.researchgate,
        created_at as fecha_creacion,
        changed_at as fecha_actualizacion,
        coalesce(changed_at, created_at, load_datetime) as vigente_desde,
        load_datetime
    from source s
    left join (
        select distinct on (node_id) node_id, nombre
        from {{ ref('ldg_vocsedici_node__field_nombre') }}
        where not is_deleted and node_id <> -1
        order by node_id,
            case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
            delta,
            revision_id desc
    ) n on s.person_node_id = n.node_id
    left join (
        select distinct on (node_id) node_id, apellido
        from {{ ref('ldg_vocsedici_node__field_apellido') }}
        where not is_deleted and node_id <> -1
        order by node_id,
            case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
            delta,
            revision_id desc
    ) a on s.person_node_id = a.node_id
    left join (
        select distinct on (node_id) node_id, orcid
        from {{ ref('ldg_vocsedici_node__field_orcid') }}
        where not is_deleted and node_id <> -1 and orcid <> '!UNKNOWN'
        order by node_id,
            case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
            delta,
            revision_id desc
    ) o on s.person_node_id = o.node_id
    left join (
        select distinct on (node_id) node_id, email
        from {{ ref('ldg_vocsedici_node__field_mail') }}
        where not is_deleted and node_id <> -1 and email <> '!UNKNOWN'
        order by node_id,
            case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
            delta,
            revision_id desc
    ) e on s.person_node_id = e.node_id
    left join (
        select distinct on (node_id) node_id, google_scholar
        from {{ ref('ldg_vocsedici_node__field_google_scholar') }}
        where not is_deleted and node_id <> -1 and google_scholar <> '!UNKNOWN'
        order by node_id,
            case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
            delta,
            revision_id desc
    ) gs on s.person_node_id = gs.node_id
    left join (
        select distinct on (node_id) node_id, researchgate
        from {{ ref('ldg_vocsedici_node__field_researchgate') }}
        where not is_deleted and node_id <> -1 and researchgate <> '!UNKNOWN'
        order by node_id,
            case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
            delta,
            revision_id desc
    ) rg on s.person_node_id = rg.node_id
)

select * from final
