-- Raw source view: Direct read from SRA accessions parquet file
-- This is an internal view for ETL only - not exposed to end users
-- Source: https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab
MODEL (
    name raw.src_sra_accessions,
    kind VIEW
);

SELECT
    Accession,
    Submission,
    Status,
    Updated,
    Published,
    Received,
    Type,
    Center,
    Visibility,
    Alias,
    Experiment,
    Sample,
    Study,
    Loaded,
    Spots,
    Bases,
    Md5sum,
    BioSample,
    BioProject,
    ReplacedBy
FROM
    read_parquet(@data_root || '/sra/sra_accessions.parquet')
