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
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
