WITH dc_identifier_uri AS (
    SELECT 
        hub.identifier as dc_identifier_uri,
        hub.identifier_hk
    FROM {{ ref('hub_oai_identifier') }} hub
    WHERE identifier like 'http://hdl.handle.net/11336/%'
),

dc_relation_doi AS (
    SELECT 
        REPLACE(hub.relation,'info:eu-repo/semantics/altIdentifier/doi/', '') as dc_relation_doi,
        hub.relation_hk
    FROM {{ ref('hub_oai_relation') }} hub
    WHERE relation like 'info:eu-repo/semantics/altIdentifier/doi/%'
)

SELECT * FROM dc_identifier_uri
