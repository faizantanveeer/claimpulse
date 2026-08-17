with source_carriers as (
    select * from {{ ref('stg_carriers') }}
)
select 
    carrier_id,
    carrier_name,
    tpa_flag
from source_carriers