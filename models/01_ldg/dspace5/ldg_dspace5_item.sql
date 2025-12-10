with source as (
        select * from {{ source('dspace5', 'item') }}
  ),
  renamed as (
      select
        item_id,
        submitter_id,
        in_archive,
        withdrawn,
        last_modified,
        owning_collection,
        discoverable,
        {{ dbt_date.today() }} as load_datetime
      from source
  )
  select * from renamed
    