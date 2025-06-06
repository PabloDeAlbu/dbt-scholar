with source as (
        select * from {{ source('dspace5', 'item2bundle') }}
  ),
  renamed as (
      select
        id,
        item_id,
        bundle_id,
        {{ dbt_date.today() }} as load_datetime
      from source
  )
  select * from renamed
    