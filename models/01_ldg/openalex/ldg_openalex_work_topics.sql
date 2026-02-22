{% set map_work_topics_relation = source('openalex', 'map_work_topics') %}
{% if execute %}
  {% set map_work_topics_col_names = adapter.get_columns_in_relation(map_work_topics_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set map_work_topics_col_names = none %}
{% endif %}

with source as (
	    select * from {{ map_work_topics_relation }}
	),
	base as (
	    select
	        id::text as work_id,
	        display_name::text,
	        topic_id::text,
	        score::double precision,
	        {{ safe_cast(map_work_topics_relation, 'domain.display_name', 'text', alias='domain_display_name', col_names=map_work_topics_col_names) }},
	        {{ safe_cast(map_work_topics_relation, 'domain.id', 'text', alias='domain_id', col_names=map_work_topics_col_names) }},
	        {{ safe_cast(map_work_topics_relation, 'field.display_name', 'text', alias='field_display_name', col_names=map_work_topics_col_names) }},
	        {{ safe_cast(map_work_topics_relation, 'field.id', 'text', alias='field_id', col_names=map_work_topics_col_names) }},
	        {{ safe_cast(map_work_topics_relation, 'subfield.display_name', 'text', alias='subfield_display_name', col_names=map_work_topics_col_names) }},
	        {{ safe_cast(map_work_topics_relation, 'subfield.id', 'text', alias='subfield_id', col_names=map_work_topics_col_names) }},
	        dv_load_datetime::timestamp
	    from source
	),
	ghost_record as (
    select
        '!UNKNOWN'::text as work_id,
        '!UNKNOWN'::text as display_name,
        '!UNKNOWN'::text as topic_id,
        -1.0::double precision as score,
        '!UNKNOWN'::text as domain_display_name,
        '!UNKNOWN'::text as domain_id,
        '!UNKNOWN'::text as field_display_name,
        '!UNKNOWN'::text as field_id,
        '!UNKNOWN'::text as subfield_display_name,
        '!UNKNOWN'::text as subfield_id,
        {{ dbt_date.today() }} as dv_load_datetime
)
select * from base
union all
select * from ghost_record
