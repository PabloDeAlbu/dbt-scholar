with source as (
      select * from {{ source('dspacedb', 'handle') }}
),
renamed as (
    select
        {{ adapter.quote("handle_id") }},
        {{ adapter.quote("handle") }},
        {{ adapter.quote("resource_type_id") }},
        {{ adapter.quote("resource_legacy_id") }},
        {{ adapter.quote("resource_id") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  