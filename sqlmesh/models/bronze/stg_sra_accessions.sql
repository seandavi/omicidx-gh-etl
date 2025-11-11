-- Bronze staging: Incremental table capturing SRA accession changes
-- Reads from raw.src_sra_accessions and materializes changes based on Updated timestamp
MODEL (
    name bronze.stg_sra_accessions,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (updated_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    Accession AS accession,
    Submission AS submission,
    Status AS status,
    CAST(Updated AS DATE) AS updated_date,
    Updated AS updated_timestamp,
    Published AS published,
    Received AS received,
    Type AS type,
    Center AS center,
    Visibility AS visibility,
    Alias AS alias,
    Experiment AS experiment,
    Sample AS sample,
    Study AS study,
    Loaded AS loaded,
    Spots AS spots,
    Bases AS bases,
    Md5sum AS md5sum,
    BioSample AS biosample,
    BioProject AS bioproject,
    ReplacedBy AS replaced_by
FROM
    raw.src_sra_accessions
WHERE
    CAST(Updated AS DATE) BETWEEN @start_ds AND @end_ds
