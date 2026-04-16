select distinct room_type
from {{ ref('stg_airbnb_listings') }}
where room_type is not null
