with source as (
    select *
    from {{ ref('stg_vocsedici_institution') }}
),
final as (
    select
        institution_node_id as institucion_id,
        institution_bk as institucion_bk,
        node_uuid as institucion_uuid,
        revision_id,
        langcode,
        is_published as esta_publicada,
        institution_name as nombre_institucion,
        abbreviation as abreviatura,
        pidu_id,
        created_at as fecha_creacion,
        changed_at as fecha_actualizacion,
        effective_from as vigente_desde,
        load_datetime
    from source
)

select * from final
