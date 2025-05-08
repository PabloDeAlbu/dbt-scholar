with source as (
  select * from {{ source('openaire', 'rel_researchproduct_instances') }}
),
renamed as (
  select
    id,
    {{ adapter.quote("articleProcessingCharge") }} as apc,
    license,
    {{ adapter.quote("publicationDate") }} as publication_date,
    refereed,
    type,
    urls,
    {{ adapter.quote("articleProcessingCharge.amount") }} as apc_amount,
    {{ adapter.quote("articleProcessingCharge.currency") }} as apc_currency,
    scheme,
    value,
    load_datetime::timestamp as load_datetime
  from source
)
select * from renamed
