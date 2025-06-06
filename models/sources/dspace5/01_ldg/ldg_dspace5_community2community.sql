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
)

select * from renamed
