with source as (
    select * from {{ source('openalex', 'institution') }}
),
renamed as (
    select distinct
        id as institution_id,
        ror::text,
        display_name::text,
        country_code::text,
        type::text,
        type_id::text,
        {# lineage::text, #}
        homepage_url::text,
        image_url::text,
        image_thumbnail_url::text,
        {# display_name_acronyms::text, #}
        {# display_name_alternatives::text, #}
        {# repositories::text, #}
        works_count::int,
        {# cited_by_count::text, #}
        {# summary_stats::text, #}
        {# ids::text, #}
        {# geo::text, #}
        {# international::text, #}
        {# associated_institutions::text, #}
        {# counts_by_year::text, #}
        {# roles::text, #}
        {# topics::text, #}
        {# topic_share::text, #}
        {# x_concepts::text, #}
        is_super_system::text,
        works_api_url::text,
        updated_date::text,
        created_date::text,
        _extract_datetime::timestamp as extract_datetime,
        _load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN' as institution_id,
        '!UNKNOWN' as ror,
        '!UNKNOWN' as display_name,
        '!UNKNOWN' as country_code,
        '!UNKNOWN' as type,
        '!UNKNOWN' as type_id,
        '!UNKNOWN' as homepage_url,
        '!UNKNOWN' as image_url,
        '!UNKNOWN' as image_thumbnail_url,
        -1 as works_count,
        '!UNKNOWN' as is_super_system,
        '!UNKNOWN' as works_api_url,
        '!UNKNOWN' as updated_date,
        '!UNKNOWN' as created_date,
        '1900-01-01'::timestamp as extract_datetime,
        {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
