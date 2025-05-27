with source as (
      select * from {{ source('dspacedb', 'community2community') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  