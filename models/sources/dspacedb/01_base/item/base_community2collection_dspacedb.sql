with source as (
      select * from {{ source('dspacedb', 'community2collection') }}
),
renamed as (
    select
        

    from source
)
select * from renamed
  