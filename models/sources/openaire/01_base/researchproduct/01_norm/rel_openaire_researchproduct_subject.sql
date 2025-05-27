with source as (
        select * from {{ source('openaire', 'rel_researchproduct_subjects') }}
  ),
  renamed as (
      select
        id::varchar,
        provenance::varchar as provenance,
        {{ adapter.quote("subject.scheme") }}::varchar as subject_scheme,
        {{ adapter.quote("subject.value") }}::varchar as subject_value,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
      from source
  )
  select * from renamed
    