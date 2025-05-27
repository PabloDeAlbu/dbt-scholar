with source as (
      select * from {{ source('dspacedb', 'communities2item') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  