with source as (
  select * from {{ source('openaire', 'researchproduct_instances') }}
),
renamed as (
  select
    id::text as researchproduct_id,
    license::text,
    {{ adapter.quote("publicationDate") }}::text as publication_date,
    refereed::text,
    type::text,
    urls::text as url,
    {{ adapter.quote("accessRight.code") }}::text as accessright_code,
    {{ adapter.quote("accessRight.label") }}::text as accessright_label,
    {{ adapter.quote("accessRight.openAccessRoute") }}::text as accessright_openaccessroute,
    {{ adapter.quote("accessRight.scheme") }}::text as accessright_scheme,
    {{ adapter.quote("collectedFrom.key") }}::text as collectedfrom_key,
    {{ adapter.quote("collectedFrom.value") }}::text as collectedfrom_value,
    {{ adapter.quote("hostedBy.key") }}::text as hostedby_key,
    {{ adapter.quote("hostedBy.value") }}::text as hostedby_value,
    {{ adapter.quote("articleProcessingCharge.amount") }}::text as apc_amount,
    {{ adapter.quote("articleProcessingCharge.currency") }}::text as apc_currency,
    scheme::text,
    value::text,
    dv_load_datetime::timestamp
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
    {{ dbt_date.today() }} as dv_load_datetime
)
select * from renamed
union all
select * from ghost_record
