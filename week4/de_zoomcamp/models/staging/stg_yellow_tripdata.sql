{{ config(materialized='view') }}

SELECT
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid','tpep_pickup_datetime']) }} AS trip_id,
    CAST(vendorid AS INTEGER) AS vendorid,
    CAST(ratecodeid AS INTEGER) AS ratecodeid,
    CAST(pulocationid AS INTEGER) AS pulocationid,
    CAST(dolocationid AS INTEGER) AS dolocationid,

    -- timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    1 AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    0 AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INTEGER) AS payment_type,
    {{ get_payment_type_description('payment_type') }} AS payment_type_description,
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge,

FROM {{ source('staging', 'yellow_tripdata') }}
WHERE vendorid IS NOT NULL
-- dbt build --n <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}