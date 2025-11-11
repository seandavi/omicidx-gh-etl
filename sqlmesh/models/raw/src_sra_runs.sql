-- Raw source view: Direct read from SRA runs parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_sra_runs,
    kind VIEW
);

SELECT
    accession,
    alias,
    attributes,
    avg_length,
    base_counts,
    experiment_accession,
    files,
    identifiers,
    qualities,
    reads,
    size,
    tax_analysis,
    title,
    total_bases,
    total_spots
FROM
    read_parquet(@data_root || '/sra/*Full-run-*.parquet')
