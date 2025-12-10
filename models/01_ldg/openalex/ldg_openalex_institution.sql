with base as (
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
        extract_datetime::timestamp,
        load_datetime::timestamp
    from {{ source('openalex', 'institution') }}
)

select * from base
