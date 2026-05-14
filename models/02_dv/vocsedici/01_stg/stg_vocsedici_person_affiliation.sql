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
        _load_datetime
    from {{ ref('ldg_vocsedici_paragraphs_item_field_data') }}
    where paragraph_type = 'filiacion'
      and parent_type = 'node'
      and parent_field_name = 'field_filiacion'
      and paragraph_id <> -1
),
paragraph_persona as (
    select distinct on (paragraph_id)
        paragraph_id,
        persona_node_id
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
raw_source as (
    select
        p.paragraph_id,
        p.revision_id,
        p.langcode,
        p.paragraph_type,
        p.is_published,
        p.person_parent_node_id,
        pp.persona_node_id as person_field_node_id,
        coalesce(pp.persona_node_id, p.person_parent_node_id) as person_node_id,
        pi.institution_node_id,
        fi.fecha_inicio,
        ff.fecha_fin,
        p.created_at,
        p._load_datetime as load_datetime,
        coalesce(pp.persona_node_id, p.person_parent_node_id)::text as person_bk,
        pi.institution_node_id::text as institution_bk,
        p.paragraph_id::text as paragraph_bk,
        case
            when pp.persona_node_id is null then 'parent_id'
            when pp.persona_node_id = p.person_parent_node_id then 'parent_id_and_field_persona_id'
            else 'field_persona_id'
        end as person_resolution_rule
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
        on coalesce(pp.persona_node_id, p.person_parent_node_id) = np.node_id
       and np.node_type = 'persona'
    left join {{ ref('ldg_vocsedici_node') }} ni
        on pi.institution_node_id = ni.node_id
       and ni.node_type = 'instituci_n'
    where coalesce(pp.persona_node_id, p.person_parent_node_id) <> -1
      and pi.institution_node_id is not null
      and pi.institution_node_id <> -1
),
final as (
    select
        person_hk,
        institution_hk,
        person_institution_hk,
        person_affiliation_hashdiff,
        person_bk,
        institution_bk,
        paragraph_bk,
        paragraph_id,
        person_node_id,
        person_parent_node_id,
        person_field_node_id,
        institution_node_id,
        revision_id,
        langcode,
        paragraph_type,
        is_published,
        person_resolution_rule,
        fecha_inicio,
        fecha_fin,
        created_at,
        load_datetime,
        '!VOCSEDICI'::text as source,
        coalesce(fecha_inicio::timestamp, created_at, load_datetime) as effective_from
    from raw_source s0
    cross join lateral (
        select
            {{ automate_dv.hash(columns='person_bk', alias='person_hk') }},
            {{ automate_dv.hash(columns='institution_bk', alias='institution_hk') }},
            {{ automate_dv.hash(columns=['person_bk', 'institution_bk', 'paragraph_bk'], alias='person_institution_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'person_bk',
                    'institution_bk',
                    'paragraph_bk',
                    'revision_id',
                    'langcode',
                    'paragraph_type',
                    'is_published',
                    'person_resolution_rule',
                    'fecha_inicio',
                    'fecha_fin'
                ],
                alias='person_affiliation_hashdiff',
                is_hashdiff=true
            ) }}
    ) s1
)

select * from final
