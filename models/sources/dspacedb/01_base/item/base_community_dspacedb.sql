with source as (
      select * from {{ source('dspacedb', 'community') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  