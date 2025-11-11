-- Raw source view: Direct read from SRA experiments parquet files
-- This is an internal view for ETL only - not exposed to end users
MODEL (
    name raw.src_sra_experiments,
    kind VIEW
);

SELECT
    accession,
    experiment_accession,
    alias,
    title,
    description,
    design,
    center_name,
    study_accession,
    sample_accession,
    platform,
    instrument_model,
    library_name,
    library_construction_protocol,
    library_layout,
    library_layout_orientation,
    library_layout_length,
    library_layout_sdev,
    library_strategy,
    library_source,
    library_selection,
    spot_length,
    nreads,
    identifiers,
    attributes,
    xrefs,
    reads
FROM
    read_parquet(@data_root || '/sra/*Full-experiment-*.parquet')
