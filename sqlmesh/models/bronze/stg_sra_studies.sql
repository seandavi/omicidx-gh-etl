-- Bronze staging: Incremental SRA studies with update dates from accessions
-- Joins raw.src_sra_studies with raw.src_sra_accessions to get Updated timestamp
MODEL (
    name bronze.stg_sra_studies,
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
    a.Status AS status
FROM
    raw.src_sra_studies s
    INNER JOIN raw.src_sra_accessions a ON s.accession = a.Accession
WHERE
    a.Type = 'STUDY'
    AND CAST(a.Updated AS DATE) BETWEEN @start_ds AND @end_ds
