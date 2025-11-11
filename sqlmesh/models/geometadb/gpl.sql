-- GEOmetadb compatibility view: GPL (GEO Platforms)
-- Provides backward-compatible schema matching the original GEOmetadb SQLite database
MODEL (
    name geometadb.gpl,
    kind VIEW
);

SELECT
    title,
    accession AS gpl,
    status,
    submission_date,
    last_update_date,
    technology,
    distribution,
    organism,
    manufacturer,
    manufacture_protocol,
    description,
    'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' || accession AS web_link,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    data_row_count,
    summary
FROM bronze.stg_geo_platforms
