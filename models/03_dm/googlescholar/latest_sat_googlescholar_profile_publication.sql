{{ config(materialized='table') }}

WITH latest AS {{ latest_satellite(
    ref('sat_googlescholar_profile_publication'),
    'profile_publication_hk',
    order_column='load_datetime'
) }}

SELECT * FROM latest
