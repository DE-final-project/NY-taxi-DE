-- Gold must only contain trips that passed cleaning. FAILS if any row breaks a rule.
select trip_id, trip_distance, trip_duration_min, fare_amount, total_amount
from {{ ref('fact_trips') }}
where trip_distance <= 0
   or trip_duration_min < 0
   or fare_amount < 0
   or total_amount < 0