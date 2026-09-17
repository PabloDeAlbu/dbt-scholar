{{ config(materialized='table') }}

with source as (
    select *
    from {{ ref('stg_vocsedici_person_affiliation') }}
),
persona as (
    select
        person_node_id,
        nombre,
        apellido,
        nombre_completo,
        orcid,
        email,
        google_scholar,
        researchgate,
        esta_publicada as person_esta_publicada
    from {{ ref('dim_vocsedici_persona') }}
),
institucion as (
    select
        institution_node_id,
        nombre_institucion,
        abreviatura,
        pidu_id,
        esta_publicada as institution_esta_publicada
    from {{ ref('dim_vocsedici_institucion') }}
),
final as (
    select
        s.paragraph_id,
        p.person_node_id,
        p.nombre,
        p.apellido,
        p.nombre_completo,
        p.orcid,
        p.email,
        p.google_scholar,
        p.researchgate,
        i.institution_node_id,
        i.nombre_institucion,
        i.abreviatura,
        i.pidu_id,
        s.person_parent_node_id,
        s.person_field_node_id,
        s.person_resolution_rule,
        s.revision_id,
        s.langcode,
        s.paragraph_type,
        s.is_published as afiliacion_esta_publicada,
        p.person_esta_publicada,
        i.institution_esta_publicada,
        s.fecha_inicio,
        s.fecha_fin,
        case
            when s.fecha_inicio is not null and s.fecha_inicio > current_date then false
            when s.fecha_fin is not null and s.fecha_fin < current_date then false
            else true
        end as esta_vigente_hoy,
        s.created_at as fecha_creacion_afiliacion,
        s.effective_from as vigente_desde,
        s.load_datetime
    from source s
    join persona p
        on s.person_node_id = p.person_node_id
    join institucion i
        on s.institution_node_id = i.institution_node_id
)

select * from final
