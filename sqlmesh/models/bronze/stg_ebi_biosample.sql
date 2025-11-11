-- Bronze staging: Incremental table capturing EBI BioSample changes
-- Reads from raw.src_ebi_biosample and materializes changes based on update timestamp
MODEL (
    name bronze.stg_ebi_biosample,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column (update_date)
    ),
    cron '@daily',
    grain accession
);

SELECT
    accession,
    name,
    CAST("update" AS TIMESTAMP) AS update_timestamp,
    CAST("update" AS DATE) AS update_date,
    CAST(release AS TIMESTAMP) AS release_timestamp,
    CAST("create" AS TIMESTAMP) AS create_timestamp,
    taxId,
    characteristics,
    organization,
    contact,
    publications,
    externalReferences,
    _links
FROM
    raw.src_ebi_biosample
WHERE
    CAST("update" AS DATE) BETWEEN @start_ds AND @end_ds
