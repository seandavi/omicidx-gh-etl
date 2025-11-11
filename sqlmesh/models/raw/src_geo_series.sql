-- Raw source view: Direct read from GEO series NDJSON files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_geo_series,
    kind VIEW
);

SELECT
    title,
    status,
    submission_date,
    last_update_date,
    accession,
    subseries,
    bioprojects,
    sra_studies,
    contact,
    type,
    summary,
    relation,
    pubmed_id,
    sample_id,
    sample_taxid,
    sample_organism,
    platform_id,
    platform_taxid,
    platform_organism,
    data_processing,
    description,
    supplemental_files,
    overall_design,
    contributor
FROM
    read_ndjson_auto(@data_root || '/geo/gse*.ndjson.gz', union_by_name=true)
