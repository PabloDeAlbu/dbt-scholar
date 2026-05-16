{{ config(materialized='view') }}

with paragraph as (
    select
        paragraph_id,
        revision_id,
        langcode,
        paragraph_type,
        parent_id as person_parent_node_id,
        parent_type,
        parent_field_name,
        is_published,
        created_at,
        _load_datetime as load_datetime
    from {{ ref('ldg_vocsedici_paragraphs_item_field_data') }}
    where paragraph_type = 'filiacion'
      and parent_type = 'node'
      and parent_field_name = 'field_filiacion'
      and paragraph_id <> -1
),
paragraph_persona as (
    select distinct on (paragraph_id)
        paragraph_id,
        persona_node_id as person_field_node_id
    from {{ ref('ldg_vocsedici_paragraph__field_persona_id') }}
    where not is_deleted
      and paragraph_id <> -1
      and persona_node_id is not null
      and persona_node_id <> -1
    order by paragraph_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
paragraph_institucion as (
    select distinct on (paragraph_id)
        paragraph_id,
        institution_node_id
    from {{ ref('ldg_vocsedici_paragraph__field_institucion') }}
    where not is_deleted
      and paragraph_id <> -1
      and institution_node_id is not null
      and institution_node_id <> -1
    order by paragraph_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
paragraph_fecha_inicio as (
    select distinct on (paragraph_id)
        paragraph_id,
        fecha_inicio
    from {{ ref('ldg_vocsedici_paragraph__field_fecha_inicio') }}
    where not is_deleted
      and paragraph_id <> -1
      and fecha_inicio is not null
      and fecha_inicio <> '1900-01-01'::date
    order by paragraph_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
paragraph_fecha_fin as (
    select distinct on (paragraph_id)
        paragraph_id,
        fecha_fin
    from {{ ref('ldg_vocsedici_paragraph__field_fecha_fin') }}
    where not is_deleted
      and paragraph_id <> -1
      and fecha_fin is not null
      and fecha_fin <> '1900-01-01'::date
    order by paragraph_id,
        case when langcode = 'es' then 0 when langcode = 'und' then 1 else 2 end,
        delta,
        revision_id desc
),
final as (
    select
        p.paragraph_id,
        coalesce(pp.person_field_node_id, p.person_parent_node_id) as person_node_id,
        p.person_parent_node_id,
        pp.person_field_node_id,
        pi.institution_node_id,
        p.revision_id,
        p.langcode,
        p.paragraph_type,
        p.is_published,
        case
            when pp.person_field_node_id is null then 'parent_id'
            when pp.person_field_node_id = p.person_parent_node_id then 'parent_id_and_field_persona_id'
            else 'field_persona_id'
        end as person_resolution_rule,
        fi.fecha_inicio,
        ff.fecha_fin,
        p.created_at,
        p.load_datetime,
        coalesce(fi.fecha_inicio::timestamp, p.created_at, p.load_datetime) as effective_from
    from paragraph p
    left join paragraph_persona pp
        using (paragraph_id)
    left join paragraph_institucion pi
        using (paragraph_id)
    left join paragraph_fecha_inicio fi
        using (paragraph_id)
    left join paragraph_fecha_fin ff
        using (paragraph_id)
    join {{ ref('ldg_vocsedici_node') }} np
        on coalesce(pp.person_field_node_id, p.person_parent_node_id) = np.node_id
       and np.node_type = 'persona'
    left join {{ ref('ldg_vocsedici_node') }} ni
        on pi.institution_node_id = ni.node_id
       and ni.node_type = 'instituci_n'
    where coalesce(pp.person_field_node_id, p.person_parent_node_id) <> -1
      and pi.institution_node_id is not null
      and pi.institution_node_id <> -1
)

select * from final
