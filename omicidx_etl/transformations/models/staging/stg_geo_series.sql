-- Staging model for GEO series (GSE)
-- Transforms raw GEO series data into clean, structured format

SELECT
    -- Core identifiers
    accession,
    title,
    status,

    -- Dates
    submission_date,
    last_update_date,

    -- Description and design
    summary,
    overall_design,

    -- Contact information (flatten STRUCT)
    contact.name.first AS contact_first_name,
    contact.name.last AS contact_last_name,
    contact.email AS contact_email,
    contact.institute AS contact_institute,
    contact.country AS contact_country,

    -- Cross-references (arrays)
    subseries,
    bioprojects,
    sra_studies,
    pubmed_id,
    sample_id,
    platform_id,
    relation,

    -- Organism information (arrays)
    sample_taxid,
    sample_organism,
    platform_taxid,
    platform_organism,

    -- Array columns
    type,
    supplemental_files,

    -- Complex nested structures (keep as JSON)
    data_processing,
    description,
    contributor,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.geo_series
WHERE accession IS NOT NULL
