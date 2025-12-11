WITH dc_identifier_uri AS (
    SELECT 
        record_hk,
        -- Si hubiera duplicados, tomamos el primero o el más corto/largo según regla
        MIN(dc_identifier) as dc_identifier_uri 
    FROM {{ ref('brg_oai_record_identifier') }} 
    WHERE dc_identifier LIKE 'http://hdl.handle.net/11336/%'
    GROUP BY record_hk
)

SELECT * FROM dc_identifier_uri
