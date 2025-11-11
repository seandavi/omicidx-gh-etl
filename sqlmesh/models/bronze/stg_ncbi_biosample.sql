-- Bronze staging: Incremental table capturing NCBI BioSample changes
-- Reads from raw.src_ncbi_biosample and materializes changes based on last_update
MODEL (
    name bronze.stg_ncbi_biosample,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (last_update_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    is_reference,
    CAST(submission_date AS TIMESTAMP) AS submission_timestamp,
    CAST(last_update AS TIMESTAMP) AS last_update_timestamp,
    CAST(last_update AS DATE) AS last_update_date,
    CAST(publication_date AS TIMESTAMP) AS publication_timestamp,
    access,
    id,
    accession,
    id_recs,
    ids,
    sra_sample,
    dbgap,
    gsm,
    title,
    description,
    taxonomy_name,
    taxon_id,
    attribute_recs,
    attributes,
    model
FROM
    raw.src_ncbi_biosample
WHERE
    CAST(last_update AS DATE) BETWEEN @start_ds AND @end_ds
