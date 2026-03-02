{% set researchproduct_instances_relation = source('openaire', 'researchproduct_instances') %}
{% if execute %}
  {% set researchproduct_instances_col_names = adapter.get_columns_in_relation(researchproduct_instances_relation) | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set researchproduct_instances_col_names = none %}
{% endif %}

with source as (
  select * from {{ researchproduct_instances_relation }}
),
renamed as (
  select
    id::text as researchproduct_id,
    license::text,
    {{ safe_cast(researchproduct_instances_relation, 'publicationDate', 'text', alias='publication_date', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    refereed::text,
    type::text,
    urls::text as url,
    {{ safe_cast(researchproduct_instances_relation, 'accessRight.code', 'text', alias='accessright_code', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'accessRight.label', 'text', alias='accessright_label', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'accessRight.openAccessRoute', 'text', alias='accessright_openaccessroute', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'accessRight.scheme', 'text', alias='accessright_scheme', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'collectedFrom.key', 'text', alias='collectedfrom_key', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'collectedFrom.value', 'text', alias='collectedfrom_value', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'hostedBy.key', 'text', alias='hostedby_key', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'hostedBy.value', 'text', alias='hostedby_value', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'articleProcessingCharge.amount', 'text', alias='apc_amount', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    {{ safe_cast(researchproduct_instances_relation, 'articleProcessingCharge.currency', 'text', alias='apc_currency', col_names=researchproduct_instances_col_names, default_mode='ghost') }},
    scheme::text,
    value::text,
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as license,
    '!UNKNOWN'::text as publication_date,
    '!UNKNOWN'::text as refereed,
    '!UNKNOWN'::text as type,
    '!UNKNOWN'::text as url,
    '!UNKNOWN'::text as accessright_code,
    '!UNKNOWN'::text as accessright_label,
    '!UNKNOWN'::text as accessright_openaccessroute,
    '!UNKNOWN'::text as accessright_scheme,
    '!UNKNOWN'::text as collectedfrom_key,
    '!UNKNOWN'::text as collectedfrom_value,
    '!UNKNOWN'::text as hostedby_key,
    '!UNKNOWN'::text as hostedby_value,
    '!UNKNOWN'::text as apc_amount,
    '!UNKNOWN'::text as apc_currency,
    '!UNKNOWN'::text as scheme,
    '!UNKNOWN'::text as value,
    {{ dbt_date.today() }} as _load_datetime
)
select * from renamed
union all
select * from ghost_record
