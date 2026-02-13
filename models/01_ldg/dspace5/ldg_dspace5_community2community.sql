with source as (
  select * from {{ source('dspace5', 'community2community') }}
),
renamed as (
  select
    id as community_community_id,
    parent_comm_id,
    child_comm_id,
    {{ dbt_date.today() }} as load_datetime
  from source
),
ghost_record as (
  select
    -1 as community_community_id,
    -1 as parent_comm_id,
    -1 as child_comm_id,
    {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
