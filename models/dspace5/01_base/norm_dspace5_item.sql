with source as (
  select 
    i.item_id,
    item_title.title,
    item_title.title_lang,
    item_type.type,
    item_dateavailable.dateavailable,
    item_dateissued.dateissued,
    item_handle.handle,
    COALESCE(i2doi.doi, 'NO DATA') as doi,
    i.submitter_id,
    i.withdrawn,
    i.in_archive,
    i.discoverable,
    i.last_modified,
    i.load_datetime
  from {{ ref('base_dspace5_item') }} i
  inner join {{ ref('map_dspace5_item_title') }} item_title ON item_title.item_id = i.item_id
  inner join {{ ref('map_dspace5_item_type') }} item_type ON item_type.item_id = i.item_id
  inner join {{ ref('map_dspace5_item_dateavailable') }} item_dateavailable ON item_dateavailable.item_id = i.item_id
  inner join {{ ref('map_dspace5_item_dateissued') }} item_dateissued ON item_dateissued.item_id = i.item_id
  inner join {{ ref('map_dspace5_item_handle') }} item_handle ON item_handle.item_id = i.item_id
  left join {{ ref('map_dspace5_item_doi') }} i2doi ON i2doi.item_id = i.item_id
)

select * from source
