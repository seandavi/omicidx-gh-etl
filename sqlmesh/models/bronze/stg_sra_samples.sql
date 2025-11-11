-- Bronze staging: Incremental SRA samples with update dates from accessions
-- Joins raw.src_sra_samples with raw.src_sra_accessions to get Updated timestamp
MODEL (
    name bronze.stg_sra_samples,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (updated_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    s.*,
    CAST(a.Updated AS DATE) AS updated_date,
    a.Updated AS updated_timestamp,
    a.Status AS status,
    a.Experiment AS experiment_ref,
    a.Study AS study_ref,
    a.BioProject AS bioproject
FROM
    raw.src_sra_samples s
    INNER JOIN raw.src_sra_accessions a ON s.accession = a.Accession
WHERE
    a.Type = 'SAMPLE'
    AND CAST(a.Updated AS DATE) BETWEEN @start_ds AND @end_ds
