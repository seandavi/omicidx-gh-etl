-- Raw source view: Direct read from SRA studies parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_sra_studies,
    kind VIEW
);

SELECT
    accession,
    abstract,
    alias,
    attributes,
    BioProject,
    broker_name,
    center_name,
    description,
    GEO,
    identifiers,
    pubmed_ids,
    study_accession,
    study_type,
    title,
    xrefs
FROM
    read_parquet(@data_root || '/sra/*Full-study-*.parquet')
