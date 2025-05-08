with source as (
        select * from {{ source('openaire', 'rel_researchproduct_subjects') }}
  ),
  renamed as (
      select
        id::varchar,
        COALESCE(provenance::varchar, 'NO DATA' ) as provenance,
        COALESCE({{ adapter.quote("subject.scheme") }}::varchar, 'NO DATA') as subject_scheme,
        COALESCE({{ adapter.quote("subject.value") }}::varchar, 'NO DATA') as subject_value,
        load_datetime::timestamp as load_datetime
      from source
  )
  select * from renamed
    