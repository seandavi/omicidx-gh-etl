-- Bronze staging: Incremental table capturing daily changes for GEO platforms
-- Reads from raw.src_geo_platforms and materializes changes
MODEL (
    name bronze.stg_geo_platforms,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (last_update_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    *
FROM
    raw.src_geo_platforms
WHERE
    last_update_date BETWEEN @start_ds AND @end_ds
