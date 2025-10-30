-- Staging: Clean and standardize SRA accessions
-- - Standardize timestamps to consistent format
-- - Add data quality flags
-- - Clean NULL values represented as '-'
-- - Add helper columns for filtering

SELECT
    -- Primary identifiers
    Accession as accession,
    Submission as submission,
    Type as type,

    -- Status and visibility
    Status as status,
    Visibility as visibility,

    -- Timestamps (already in proper timestamp format from CSV parsing)
    Updated as updated_at,
    Published as published_at,
    Received as received_at,

    -- Submitter information
    Center as center,
    Alias as alias,

    -- Cross-references (convert '-' to NULL for cleaner data)
    NULLIF(Experiment, '-') AS experiment,
    NULLIF(Sample, '-') AS sample,
    NULLIF(Study, '-') AS study,
    NULLIF(BioSample, '-') AS biosample,
    NULLIF(BioProject, '-') AS bioproject,
    NULLIF(ReplacedBy, '-') AS replacedby,

    -- Run-specific metrics (convert '-' to NULL)
    CAST(Loaded AS BIGINT) AS loaded,
    CAST(Spots AS BIGINT) AS spots,
    CAST(Bases AS BIGINT) AS bases,

    -- Metadata
    Md5sum as md5sum,

    -- Data quality flags
    CASE
        WHEN status = 'live' AND visibility = 'public' THEN TRUE
        ELSE FALSE
    END AS is_public_live,

    CASE
        WHEN ReplacedBy IS NOT NULL AND ReplacedBy != '-' THEN TRUE
        ELSE FALSE
    END AS is_replaced,

    CASE
        WHEN Type = 'RUN' AND Spots IS NOT NULL THEN TRUE
        WHEN Type != 'RUN' THEN TRUE
        ELSE FALSE
    END AS has_complete_metrics,

    -- Helper: days since last update
    DATE_DIFF('day', CAST(Updated AS DATE), CURRENT_DATE) AS days_since_update,

    -- Helper: accession prefix (useful for grouping by archive)
    SUBSTRING(Accession, 1, 3) AS accession_prefix,

    -- Audit timestamp
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.sra_accessions
WHERE Accession IS NOT NULL
