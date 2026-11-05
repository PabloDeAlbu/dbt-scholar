{{ config(materialized='table') }}

-- ¿Qué publicaciones conforman el extracto de metadatos CENDIE?
WITH publication AS (
    SELECT *
    FROM {{ ref('fct_cendie_oai_record_publication') }}
),

-- ¿Qué valores dc.creator corresponden a cada publicación?
creator AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_creator, '|' ORDER BY dc_creator) AS dc_creator
    FROM {{ ref('brg_oai_record_creator') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.description corresponden a cada publicación?
description AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_description, '|' ORDER BY dc_description) AS dc_description
    FROM {{ ref('brg_oai_record_description') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.subject corresponden a cada publicación?
subject AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_subject, '|' ORDER BY dc_subject) AS dc_subject
    FROM {{ ref('brg_oai_record_subject') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.publisher corresponden a cada publicación?
publisher AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_publisher, '|' ORDER BY dc_publisher) AS dc_publisher
    FROM {{ ref('brg_oai_record_publisher') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.language corresponden a cada publicación?
language AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_language, '|' ORDER BY dc_language) AS dc_language
    FROM {{ ref('brg_oai_record_language') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.type corresponden a cada publicación?
type_metadata AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_type, '|' ORDER BY dc_type) AS dc_type
    FROM {{ ref('brg_oai_record_type') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.format corresponden a cada publicación?
format_metadata AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_format, '|' ORDER BY dc_format) AS dc_format
    FROM {{ ref('brg_oai_record_format') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.identifier corresponden a cada publicación?
identifier AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_identifier, '|' ORDER BY dc_identifier) AS dc_identifier
    FROM {{ ref('brg_oai_record_identifier') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.relation corresponden a cada publicación?
relation_metadata AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_relation, '|' ORDER BY dc_relation) AS dc_relation
    FROM {{ ref('brg_oai_record_relation') }}
    GROUP BY record_hk
),

-- ¿Qué valores dc.right corresponden a cada publicación?
right_metadata AS (
    SELECT
        record_hk,
        STRING_AGG(DISTINCT dc_right, '|' ORDER BY dc_right) AS dc_right
    FROM {{ ref('brg_oai_record_right') }}
    GROUP BY record_hk
),

-- ¿Qué columnas expone cendie_oai_record_metadata?
final AS (
    SELECT
        publication.record_id,
        publication.journal_id,
        publication.journal_name,
        publication.section_ids,
        publication.section_names,
        publication.title AS dc_title,
        publication.date_issued AS dc_date,
        creator.dc_creator,
        description.dc_description,
        subject.dc_subject,
        publisher.dc_publisher,
        language.dc_language,
        type_metadata.dc_type,
        format_metadata.dc_format,
        identifier.dc_identifier,
        relation_metadata.dc_relation,
        right_metadata.dc_right,
        publication.repository_identifier,
        publication.extract_datetime,
        publication.load_datetime,
        publication.record_hk
    FROM publication
    LEFT JOIN creator USING (record_hk)
    LEFT JOIN description USING (record_hk)
    LEFT JOIN subject USING (record_hk)
    LEFT JOIN publisher USING (record_hk)
    LEFT JOIN language USING (record_hk)
    LEFT JOIN type_metadata USING (record_hk)
    LEFT JOIN format_metadata USING (record_hk)
    LEFT JOIN identifier USING (record_hk)
    LEFT JOIN relation_metadata USING (record_hk)
    LEFT JOIN right_metadata USING (record_hk)
)

SELECT *
FROM final
