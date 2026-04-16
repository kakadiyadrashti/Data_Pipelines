select distinct property_type
from {{ ref('stg_airbnb_listings') }}
where property_type is not null
