with source as (
        select * from {{ source('oai', 'item_by_set') }}
  ),
  renamed as (
      select
        {{ adapter.quote("item_id") }},
        {{ adapter.quote("handle") }},
        {{ adapter.quote("col_id") }},
        {{ adapter.quote("title") }},
        {{ adapter.quote("date_issued") }},
        {{ adapter.quote("type_openaire") }},
        {{ adapter.quote("type_snrd") }},
        {{ adapter.quote("version") }},
        {{ adapter.quote("access_right") }},
        {{ adapter.quote("license_condition") }},
        {{ adapter.quote("load_datetime") }}

      from source
  )
  select * from renamed
    