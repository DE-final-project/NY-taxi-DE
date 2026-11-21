-- Surrogate key for one trip: SHA2_256 of the columns that identify it.
-- Shared by silver_trips and quarantine_trips so both always compute the SAME key.
{% macro trip_id() %}
    convert(char(64), hashbytes('SHA2_256', concat_ws('|',
        vendor_id,
        convert(varchar(23), pickup_at, 121),
        convert(varchar(23), dropoff_at, 121),
        pickup_location_id,
        dropoff_location_id,
        cast(total_amount as decimal(10, 2))
    )), 2)
{% endmacro %}