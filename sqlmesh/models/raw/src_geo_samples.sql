-- Raw source view: Direct read from NDJSON files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_geo_samples,
    kind VIEW
    -- Note: Omitting columns definition to allow all columns from read_ndjson_auto
    -- Including complex types like STRUCT and JSON[] that are difficult to define
);

SELECT
    title,
    status,
    submission_date,
    last_update_date,
    type,
    anchor,
    contact,
    description,
    accession,
    biosample,
    tag_count,
    tag_length,
    platform_id,
    hyb_protocol,
    channel_count,
    scan_protocol,
    data_row_count,
    library_source,
    overall_design,
    sra_experiment,
    data_processing,
    supplemental_files,
    channels,
    contributor
FROM
    read_ndjson_auto(@data_root || '/geo/gsm*.ndjson.gz', union_by_name=true)
-- WHERE
--    last_update_date BETWEEN @start_ds AND @end_ds; -- Incremental by time range filtering