with source as (
  select 
    i.item_id, 
    i.submitter_id, 
    i.withdrawn, 
    i.owning_collection, 
    i.in_archive, 
    i.discoverable, 
    i.last_modified, 
    mv.text_value, 
    mv.text_lang, 
    mv.authority, 
--    mv.place, 
    mv.confidence, 
    msr.short_id, 
    mfr.element, 
    mfr.qualifier, 
    i.load_datetime
  from {{ ref('base_dspace5_item') }} i
  INNER JOIN {{ ref('base_dspace5_metadatavalue') }} mv ON mv.resource_type_id = '2' AND mv.resource_id = i.item_id
  INNER JOIN {{ ref('base_dspace5_metadatafield') }} mfr ON mfr.metadata_field_id = mv.metadata_field_id
  INNER JOIN {{ ref('base_dspace5_metadataschema') }} msr ON msr.metadata_schema_id = mfr.metadata_schema_id
)

select * from source
