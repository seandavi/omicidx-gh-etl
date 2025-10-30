-- Staging: Clean and enrich SRA samples
-- - Standardize organism metadata
-- - Validate taxonomy information
-- - Add data quality flags

SELECT
    accession,
    sample_accession,
    title,
    description,

    -- Organism information
    organism,
    taxon_id,

    -- BioSample link
    BioSample,

    -- Submitter info
    center_name,
    broker_name,
    alias,

    -- Cross-references
    GEO,
    identifiers,
    attributes,
    xrefs,

    -- Data quality flags
    CASE
        WHEN organism IS NULL THEN FALSE
        WHEN taxon_id IS NULL THEN FALSE
        ELSE TRUE
    END AS has_complete_organism_info,

    CASE
        WHEN BioSample IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_biosample_link,

    -- Audit
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.sra_samples
