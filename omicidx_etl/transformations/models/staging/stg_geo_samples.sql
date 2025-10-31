-- Staging model for GEO samples (GSM)
-- Transforms raw GEO sample data into clean, structured format

SELECT
    -- Core identifiers
    accession,
    title,
    status,
    type,

    -- Dates
    submission_date,
    last_update_date,

    -- Cross-references
    biosample,
    platform_id,
    sra_experiment,

    -- Sample details
    description,
    anchor,
    channel_count,
    tag_count,
    tag_length,
    data_row_count,
    library_source,

    -- Protocols
    hyb_protocol,
    scan_protocol,
    data_processing,

    -- Contact information (flatten STRUCT)
    contact.name.first AS contact_first_name,
    contact.name.last AS contact_last_name,
    contact.email AS contact_email,
    contact.institute AS contact_institute,
    contact.country AS contact_country,

    -- Arrays (keep as-is for flexibility)
    supplemental_files,

    -- Complex nested structures (keep as JSON)
    channels,
    overall_design,
    contributor,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.geo_samples
WHERE accession IS NOT NULL
