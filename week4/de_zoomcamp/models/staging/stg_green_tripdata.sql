{{ config(materialized="view") }}

SELECT *
FROM de_zoomcamp.external_green_tripdata;