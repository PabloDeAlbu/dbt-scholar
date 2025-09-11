with source as (
  select * from {{ source('openaire', 'researchproduct_instances') }}
),
renamed as (
  select
    id::text as researchproduct_id,
--    {{ adapter.quote("articleProcessingCharge") }}::text as apc,
    license::text,
    {{ adapter.quote("publicationDate") }}::text as publication_date,
    refereed::text,
    type::text,
    urls::text,
--    {{ adapter.quote("accessRight") }}::text as accessright,
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
    load_datetime::timestamp
  from source
)
select * from renamed
