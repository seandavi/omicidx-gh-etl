-- Bronze staging: Incremental table capturing daily changes for GEO series
-- Reads from raw.src_geo_series and materializes changes
MODEL (
    name bronze.stg_geo_series,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (last_update_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    *
FROM
    raw.src_geo_series
WHERE
    last_update_date BETWEEN @start_ds AND @end_ds
