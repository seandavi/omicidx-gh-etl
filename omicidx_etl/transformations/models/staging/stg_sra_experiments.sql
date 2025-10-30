-- Staging: Clean and enrich SRA experiments
-- - Standardize library metadata
-- - Add instrument and platform categories

SELECT
    accession,
    experiment_accession,
    title,
    description,

    -- Study and sample links
    study_accession,
    sample_accession,

    -- Sequencing platform
    platform,
    instrument_model,

    -- Library metadata
    library_name,
    library_strategy,
    library_source,
    library_selection,
    library_layout,
    library_layout_orientation,
    library_layout_length,
    library_layout_sdev,
    library_construction_protocol,

    -- Read information
    CAST(nreads AS BIGINT) AS nreads,
    CAST(spot_length AS BIGINT) AS spot_length,
    reads,

    -- Submitter info
    center_name,
    broker_name,
    alias,

    -- Cross-references
    GEO,
    identifiers,
    attributes,
    xrefs,

    -- Design information
    design,

    -- Data quality flags
    CASE
        WHEN library_strategy IS NULL THEN FALSE
        WHEN platform IS NULL THEN FALSE
        ELSE TRUE
    END AS has_complete_library_info,

    -- Audit
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.sra_experiments
