-- Raw source view: Direct read from NCBI BioSample parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_ncbi_biosample,
    kind VIEW
);

SELECT
    is_reference,
    submission_date,
    last_update,
    publication_date,
    access,
    id,
    accession,
    id_recs,
    ids,
    sra_sample,
    dbgap,
    gsm,
    title,
    description,
    taxonomy_name,
    taxon_id,
    attribute_recs,
    attributes,
    model
FROM
    read_parquet(@data_root || '/biosample/biosample-*.parquet')
