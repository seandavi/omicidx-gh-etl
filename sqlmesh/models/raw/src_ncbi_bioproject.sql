-- Raw source view: Direct read from NCBI BioProject parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_ncbi_bioproject,
    kind VIEW
);

SELECT
    title,
    description,
    name,
    accession,
    publications,
    locus_tags,
    release_date,
    data_types,
    external_links
FROM
    read_parquet(@data_root || '/biosample/bioproject-*.parquet')
