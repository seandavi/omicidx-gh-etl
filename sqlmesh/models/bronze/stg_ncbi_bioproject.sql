-- Bronze staging: Incremental table capturing NCBI BioProject changes
-- Reads from raw.src_ncbi_bioproject and materializes changes based on release_date
MODEL (
    name bronze.stg_ncbi_bioproject,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (release_date_only)
    ),
    cron '@daily',
    grain accession
);

SELECT
    title,
    description,
    name,
    accession,
    publications,
    locus_tags,
    CAST(release_date AS TIMESTAMP) AS release_timestamp,
    CAST(release_date AS DATE) AS release_date_only,
    data_types,
    external_links
FROM
    raw.src_ncbi_bioproject
WHERE
    CAST(release_date AS DATE) BETWEEN @start_ds AND @end_ds
