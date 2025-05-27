with source as (
      select * from {{ source('dspacedb', 'collection') }}
),
renamed as (
    select
        {{ adapter.quote("collection_id") }},
        {{ adapter.quote("uuid") }},
        {{ adapter.quote("submitter") }},
        {{ adapter.quote("admin") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  