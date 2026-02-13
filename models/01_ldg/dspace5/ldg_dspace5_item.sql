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
),
ghost_record as (
  select
    -1 as item_id,
    -1 as submitter_id,
    false as in_archive,
    false as withdrawn,
    '1900-01-01'::timestamp as last_modified,
    -1 as owning_collection,
    false as discoverable,
    {{ dbt_date.today() }} as load_datetime
)
select * from renamed
union all
select * from ghost_record
    
