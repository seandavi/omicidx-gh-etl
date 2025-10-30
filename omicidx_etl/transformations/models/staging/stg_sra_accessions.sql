-- Staging: Clean and standardize SRA accessions
-- - Standardize timestamps to consistent format
-- - Add data quality flags
-- - Clean NULL values represented as '-'
-- - Add helper columns for filtering

SELECT
    -- Primary identifiers
    Accession,
    Submission,
    Type,

    -- Status and visibility
    Status,
    Visibility,

    -- Timestamps (already in proper timestamp format from CSV parsing)
    Updated,
    Published,
    Received,

    -- Submitter information
    Center,
    Alias,

    -- Cross-references (convert '-' to NULL for cleaner data)
    NULLIF(Experiment, '-') AS Experiment,
    NULLIF(Sample, '-') AS Sample,
    NULLIF(Study, '-') AS Study,
    NULLIF(BioSample, '-') AS BioSample,
    NULLIF(BioProject, '-') AS BioProject,
    NULLIF(ReplacedBy, '-') AS ReplacedBy,

    -- Run-specific metrics (convert '-' to NULL)
    CASE WHEN Loaded = '-' THEN NULL ELSE CAST(Loaded AS BIGINT) END AS Loaded,
    CASE WHEN Spots = '-' THEN NULL ELSE CAST(Spots AS BIGINT) END AS Spots,
    CASE WHEN Bases = '-' THEN NULL ELSE CAST(Bases AS BIGINT) END AS Bases,

    -- Metadata
    Md5sum,

    -- Data quality flags
    CASE
        WHEN Status = 'live' AND Visibility = 'public' THEN TRUE
        ELSE FALSE
    END AS is_public_live,

    CASE
        WHEN ReplacedBy IS NOT NULL AND ReplacedBy != '-' THEN TRUE
        ELSE FALSE
    END AS is_replaced,

    CASE
        WHEN Type = 'RUN' AND Spots IS NOT NULL AND Spots != '-' THEN TRUE
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
