-- GEOmetadb compatibility view: GSE (GEO Series)
-- Provides backward-compatible schema matching the original GEOmetadb SQLite database
MODEL (
    name geometadb.gse,
    kind VIEW
);

SELECT
    accession AS gse,
    title,
    status,
    submission_date,
    last_update_date,
    summary,
    pubmed_id,
    type,
    contributor,
    'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' || accession AS web_link,
    overall_design,
    contact.country AS contact_country,
    contact.email AS contact_email,
    contact."name"."first" AS contact_first_name,
    contact.institute AS contact_institute,
    contact."name"."last" AS contact_last_name,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    supplemental_files AS supplementary_file,
    data_processing
FROM bronze.stg_geo_series
