-- Staging: Clean and enrich SRA studies
-- - Add data quality flags
-- - Extract key attributes

SELECT
    accession,
    study_accession,
    title,
    description,
    abstract,
    study_type,
    center_name,
    broker_name,
    alias,

    -- Cross-references
    BioProject,
    GEO,
    pubmed_ids,

    -- Data quality flags
    CASE
        WHEN title IS NULL OR title = '' THEN FALSE
        WHEN description IS NULL OR description = '' THEN FALSE
        ELSE TRUE
    END AS has_complete_metadata,

    -- Array attributes
    attributes,
    identifiers,
    xrefs,

    -- Audit
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.sra_studies
