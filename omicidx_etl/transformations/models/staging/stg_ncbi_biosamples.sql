-- Staging model for NCBI BioSamples
-- Transforms raw BioSample data into clean, structured format

SELECT
    -- Core identifiers
    accession,
    id,
    title,
    description,

    -- Cross-references
    sra_sample,
    dbgap,
    gsm,

    -- Taxonomy
    taxonomy_name,
    taxon_id,

    -- Access and status
    access,
    is_reference,

    -- Dates (convert VARCHAR to DATE where possible)
    TRY_CAST(submission_date AS DATE) AS submission_date,
    TRY_CAST(last_update AS DATE) AS last_update,
    TRY_CAST(publication_date AS DATE) AS publication_date,

    -- Model
    model,

    -- Complex structures (keep as-is for downstream processing)
    id_recs,
    ids,
    attribute_recs,
    attributes,

    -- Metadata
    CURRENT_TIMESTAMP AS _loaded_at

FROM raw.ncbi_biosamples
WHERE accession IS NOT NULL
