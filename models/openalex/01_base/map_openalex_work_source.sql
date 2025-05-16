with source as (
      select * from {{ source('openalex', 'map_work_location') }}
),
renamed as (
    select
        {{ adapter.quote("id") }} as work_id,
        {{ adapter.quote("source_id") }},
        {{ adapter.quote("source_display_name") }},
        {{ adapter.quote("source_is_core") }},
        {{ adapter.quote("source_type") }},
        {{ adapter.quote("source_host_organization") }},
        {{ adapter.quote("source_host_organization_name") }},
        {{ adapter.quote("is_accepted") }},
        {{ adapter.quote("is_oa") }},
        {{ adapter.quote("is_published") }},
        {{ adapter.quote("landing_page_url") }},
        {{ adapter.quote("license") }},
        {{ adapter.quote("license_id") }},
        {{ adapter.quote("pdf_url") }},
        {{ adapter.quote("version") }},
        {{ adapter.quote("source_is_in_doaj") }},
        {{ adapter.quote("source_is_oa") }},
        {{ adapter.quote("source_issn_l") }},
        {{ adapter.quote("load_datetime") }}
    from source
),

casted as (
    select
        work_id::varchar,
        source_id::varchar,
        source_display_name::varchar,
        source_type::varchar,
        source_host_organization::varchar,
        source_host_organization_name::varchar,
        source_is_core::varchar,
        source_issn_l::varchar,
        landing_page_url::varchar,
        license::varchar,
        license_id::varchar,
        pdf_url::varchar,
        version::varchar,
        is_accepted::boolean,
        is_oa::boolean,
        is_published::boolean,
        source_is_in_doaj::boolean,
        source_is_oa::boolean,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
    from renamed
)

select * from casted
  