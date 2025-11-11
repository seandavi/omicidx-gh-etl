-- Bronze staging: Incremental SRA runs with update dates from accessions
-- Joins raw.src_sra_runs with raw.src_sra_accessions to get Updated timestamp
MODEL (
    name bronze.stg_sra_runs,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (updated_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    r.*,
    CAST(a.Updated AS DATE) AS updated_date,
    a.Updated AS updated_timestamp,
    a.Status AS status,
    a.Experiment AS experiment_ref,
    a.Sample AS sample_ref,
    a.Study AS study_ref,
    a.BioSample AS biosample,
    a.BioProject AS bioproject
FROM
    raw.src_sra_runs r
    INNER JOIN raw.src_sra_accessions a ON r.accession = a.Accession
WHERE
    a.Type = 'RUN'
    AND CAST(a.Updated AS DATE) BETWEEN @start_ds AND @end_ds
