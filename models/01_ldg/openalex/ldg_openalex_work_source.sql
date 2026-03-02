with source as (
    select
        id::text as work_id,
        source_id::text,
        source_display_name::text,
        source_type::text,
        source_host_organization::text,
        source_host_organization_name::text,
        source_is_core::text,
        source_issn_l::text,
        landing_page_url::text,
        license::text,
        license_id::text,
        pdf_url::text,
        version::text,
        is_accepted::boolean,
        is_oa::boolean,
        is_published::boolean,
        source_is_in_doaj::boolean,
        source_is_oa::boolean,
        _load_datetime::timestamp
    from {{ source('openalex', 'map_work_location') }}
),
ghost_record as (
    select
        '!UNKNOWN'::text as work_id,
        '!UNKNOWN'::text as source_id,
        '!UNKNOWN'::text as source_display_name,
        '!UNKNOWN'::text as source_type,
        '!UNKNOWN'::text as source_host_organization,
        '!UNKNOWN'::text as source_host_organization_name,
        '!UNKNOWN'::text as source_is_core,
        '!UNKNOWN'::text as source_issn_l,
        '!UNKNOWN'::text as landing_page_url,
        '!UNKNOWN'::text as license,
        '!UNKNOWN'::text as license_id,
        '!UNKNOWN'::text as pdf_url,
        '!UNKNOWN'::text as version,
        false as is_accepted,
        false as is_oa,
        false as is_published,
        false as source_is_in_doaj,
        false as source_is_oa,
        {{ dbt_date.today() }} as _load_datetime
)

select * from source
union all
select * from ghost_record
