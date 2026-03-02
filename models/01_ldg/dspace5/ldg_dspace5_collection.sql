with source as (
  select * from {{ source('dspace5', 'collection') }}
),
renamed as (
  select
    collection_id,
    logo_bitstream_id,
    template_item_id,
    workflow_step_1,
    workflow_step_2,
    workflow_step_3,
    submitter,
    admin,
    {{ dbt_date.today() }} as _load_datetime
  from source
),
ghost_record as (
  select
    -1 as collection_id,
    -1 as logo_bitstream_id,
    -1 as template_item_id,
    false as workflow_step_1,
    false as workflow_step_2,
    false as workflow_step_3,
    false as submitter,
    false as admin,
    {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
