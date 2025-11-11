-- Raw source view: Direct read from GEO platforms NDJSON files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_geo_platforms,
    kind VIEW
);

SELECT
    title,
    status,
    submission_date,
    last_update_date,
    accession,
    contact,
    summary,
    organism,
    sample_id,
    series_id,
    technology,
    description,
    distribution,
    manufacturer,
    data_row_count,
    contributor,
    relation,
    manufacture_protocol
FROM
    read_ndjson_auto(@data_root || '/geo/gpl*.ndjson.gz', union_by_name=true)
