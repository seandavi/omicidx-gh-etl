-- Staging: Clean and enrich NCBI BioProjects
-- - Standardize release date
-- - Preserve nested structures for downstream processing
-- - Add data quality flags and audit timestamp

SELECT
    -- Core identifiers
    accession,
    name,
    title,
    description,

    -- Dates
    TRY_CAST(release_date AS DATE) AS release_date,

    -- Arrays and nested structures (kept for later normalization if needed)
    data_types,
    publications,
    external_links,
    locus_tags,

    -- Data quality flags
    CASE
        WHEN accession IS NOT NULL AND (title IS NOT NULL OR name IS NOT NULL) THEN TRUE
        ELSE FALSE
    END AS has_core_metadata,

    -- Audit
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.ncbi_bioprojects
WHERE accession IS NOT NULL
