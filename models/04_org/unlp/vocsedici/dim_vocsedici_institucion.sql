with recursive source as (
    select distinct on (institution_node_id)
        *
    from {{ ref('stg_vocsedici_institution') }}
    order by institution_node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        revision_id desc
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
parent_institution as (
    select distinct on (node_id)
        node_id,
        parent_institution_node_id
    from {{ ref('ldg_vocsedici_node__field_padre') }}
    where not is_deleted
      and node_id <> -1
      and parent_institution_node_id <> -1
    order by node_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
base as (
    select
        s.institution_node_id,
        s.revision_id,
        s.langcode,
        s.is_published as esta_publicada,
        n.institution_name as nombre_institucion,
        a.abbreviation as abreviatura,
        p.pidu_id,
        pi.parent_institution_node_id,
        s.created_at as fecha_creacion,
        s.changed_at as fecha_actualizacion,
        coalesce(s.changed_at, s.created_at, s.load_datetime) as vigente_desde,
        s.load_datetime
    from source s
    left join institution_name n
        on s.institution_node_id = n.node_id
    left join abbreviation a
        on s.institution_node_id = a.node_id
    left join pidu p
        on s.institution_node_id = p.node_id
    left join parent_institution pi
        on s.institution_node_id = pi.node_id
),
institution_parent as (
    select
        child.institution_node_id,
        child.parent_institution_node_id,
        parent.nombre_institucion as nombre_institucion_padre
    from base child
    left join base parent
        on child.parent_institution_node_id = parent.institution_node_id
),
unlp_root as (
    select institution_node_id
    from base
    where lower(trim(nombre_institucion)) = 'universidad nacional de la plata'
),
unlp_tree as (
    select
        b.institution_node_id,
        0::int as nivel_arbol_unlp,
        b.institution_node_id as raiz_unlp_node_id,
        b.nombre_institucion as raiz_unlp_nombre,
        b.institution_node_id as unidad_principal_unlp_node_id,
        b.nombre_institucion as unidad_principal_unlp_nombre,
        b.nombre_institucion::text as jerarquia_unlp
    from base b
    join unlp_root r
        on b.institution_node_id = r.institution_node_id

    union all

    select
        child.institution_node_id,
        tree.nivel_arbol_unlp + 1 as nivel_arbol_unlp,
        tree.raiz_unlp_node_id,
        tree.raiz_unlp_nombre,
        case
            when tree.nivel_arbol_unlp = 0 then child.institution_node_id
            else tree.unidad_principal_unlp_node_id
        end as unidad_principal_unlp_node_id,
        case
            when tree.nivel_arbol_unlp = 0 then child.nombre_institucion
            else tree.unidad_principal_unlp_nombre
        end as unidad_principal_unlp_nombre,
        (tree.jerarquia_unlp || ' > ' || child.nombre_institucion)::text as jerarquia_unlp
    from base child
    join unlp_tree tree
        on child.parent_institution_node_id = tree.institution_node_id
),
final as (
    select
        b.institution_node_id,
        b.revision_id,
        b.langcode,
        b.esta_publicada,
        b.nombre_institucion,
        b.abreviatura,
        b.pidu_id,
        p.parent_institution_node_id,
        p.nombre_institucion_padre,
        (t.institution_node_id is not null) as pertenece_arbol_unlp,
        t.nivel_arbol_unlp,
        t.raiz_unlp_node_id,
        t.raiz_unlp_nombre,
        t.unidad_principal_unlp_node_id,
        t.unidad_principal_unlp_nombre,
        t.jerarquia_unlp,
        b.fecha_creacion,
        b.fecha_actualizacion,
        b.vigente_desde,
        b.load_datetime
    from base b
    left join institution_parent p
        on b.institution_node_id = p.institution_node_id
    left join unlp_tree t
        on b.institution_node_id = t.institution_node_id
)

select * from final
