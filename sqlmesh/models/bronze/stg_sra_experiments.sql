-- Bronze staging: Incremental SRA experiments with update dates from accessions
-- Joins raw.src_sra_experiments with raw.src_sra_accessions to get Updated timestamp
MODEL (
    name bronze.stg_sra_experiments,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (updated_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    e.*,
    CAST(a.Updated AS DATE) AS updated_date,
    a.Updated AS updated_timestamp,
    a.Status AS status,
    a.BioSample AS biosample,
    a.BioProject AS bioproject
FROM
    raw.src_sra_experiments e
    INNER JOIN raw.src_sra_accessions a ON e.accession = a.Accession
WHERE
    a.Type = 'EXPERIMENT'
    AND CAST(a.Updated AS DATE) BETWEEN @start_ds AND @end_ds
