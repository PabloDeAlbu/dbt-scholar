with source as (
    select *
    from {{ ref('stg_vocsedici_person') }}
),
final as (
    select
        person_node_id as persona_id,
        person_bk as persona_bk,
        node_uuid as persona_uuid,
        revision_id,
        langcode,
        is_published as esta_publicada,
        nombre,
        apellido,
        full_name as nombre_completo,
        orcid,
        email,
        google_scholar,
        researchgate,
        created_at as fecha_creacion,
        changed_at as fecha_actualizacion,
        effective_from as vigente_desde,
        load_datetime
    from source
)

select * from final
