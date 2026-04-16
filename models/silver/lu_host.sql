select distinct
  host_id,
  host_name,
  host_is_superhost
from {{ ref('stg_airbnb_listings') }}
where host_id is not null
