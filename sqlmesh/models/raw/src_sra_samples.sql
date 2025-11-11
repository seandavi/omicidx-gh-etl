-- Raw source view: Direct read from SRA samples parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_sra_samples,
    kind VIEW
);

SELECT
    accession,
    alias,
    attributes,
    BioSample,
    description,
    geo,
    identifiers,
    organism,
    taxon_id,
    title,
    xrefs
FROM
    read_parquet(@data_root || '/sra/*Full-sample-*.parquet')
