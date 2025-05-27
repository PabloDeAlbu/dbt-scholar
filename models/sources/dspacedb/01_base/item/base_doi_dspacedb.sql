with source as (
      select * from {{ source('dspacedb', 'doi') }}
),
renamed as (
    select
        {{ adapter.quote("doi_id") }},
        {{ adapter.quote("doi") }},
        {{ adapter.quote("resource_type_id") }},
        {{ adapter.quote("resource_id") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("dspace_object") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  