-- Raw source view: Direct read from EBI BioSample parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_ebi_biosample,
    kind VIEW
);

SELECT
    accession,
    name,
    "update",
    release,
    "create",
    taxId,
    characteristics,
    organization,
    contact,
    publications,
    externalReferences,
    _links
FROM
    read_parquet(@data_root || '/ebi_biosample/biosamples-*.parquet')
