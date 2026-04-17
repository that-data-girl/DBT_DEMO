with trip_fact as 
(

select ride_id,
rideable_type,
date(to_timestamp(started_at)) as trip_date,
start_statio_id as station_id,
member_csual as member_casual,
timestampdiff(second, to_timestamp(started_at),to_timestamp(ended_at)) as trip_duration
from {{ source('demo', 'bike') }}
where ride_id != 'ride_id'
)

select * from trip_fact