{{ config(materialized='view') }}

select 
    locationID,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref("taxi_zone_lookup") }}