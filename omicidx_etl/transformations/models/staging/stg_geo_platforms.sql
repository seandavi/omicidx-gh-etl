-- Staging model for GEO platforms (GPL)
-- Transforms raw GEO platform data into clean, typed format

SELECT
    -- Core identifiers
    accession,
    title,
    status,

    -- Dates
    submission_date,
    last_update_date,

    -- Platform details
    organism,
    technology,
    manufacturer,
    distribution,
    data_row_count,

    -- Description and protocols
    description,
    manufacture_protocol,

    -- Contact information (flatten STRUCT)
    contact.name.first AS contact_first_name,
    contact.name.last AS contact_last_name,
    contact.email AS contact_email,
    contact.institute AS contact_institute,
    contact.country AS contact_country,

    -- Arrays (keep as-is for flexibility)
    sample_id,
    series_id,
    relation,

    -- Summary (JSON field - keep as-is)
    summary,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.geo_platforms
WHERE accession IS NOT NULL
