-- Staging: Clean and enrich SRA runs
-- - Standardize run metadata
-- - Parse run dates
-- - Add data quality flags

SELECT
    accession,
    experiment_accession,
    title,

    -- Run execution info
    run_center,
    TRY_CAST(run_date AS TIMESTAMP) AS run_date,
    
    -- Submitter info
    center_name,
    broker_name,
    alias,

    -- Cross-references
    GEO,
    identifiers,
    attributes,

    -- Data quality metadata
    qualities,

    -- Data quality flags
    CASE
        WHEN experiment_accession IS NULL THEN FALSE
        WHEN run_date IS NULL THEN FALSE
        ELSE TRUE
    END AS has_complete_run_info,

    -- Audit
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.sra_runs
