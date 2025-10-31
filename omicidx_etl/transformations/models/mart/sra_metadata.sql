-- Data mart: Complete SRA metadata combining studies, experiments, samples, and runs
-- This is a production-ready view for export and querying

WITH study_stats AS (
    SELECT
        study_accession,
        COUNT(DISTINCT accession) AS experiment_count
    FROM staging.stg_sra_experiments
    GROUP BY study_accession
),

experiment_sample_join AS (
    SELECT
        e.experiment_accession,
        e.study_accession,
        e.platform,
        e.instrument_model,
        e.library_strategy,
        e.library_source,
        e.library_selection,
        e.library_layout,
        s.accession AS sample_accession,
        s.organism,
        s.taxon_id,
        s.BioSample
    FROM staging.stg_sra_experiments e
    LEFT JOIN raw.sra_samples s
        ON e.sample_accession = s.sample_accession
)

SELECT
    -- Study information
    st.accession AS study_accession,
    st.title AS study_title,
    st.study_type,
    st.abstract,
    st.BioProject,
    st.GEO AS study_geo,

    -- Experiment information
    e.experiment_accession,
    e.platform,
    e.instrument_model,
    e.library_strategy,
    e.library_source,
    e.library_selection,
    e.library_layout,

    -- Sample information
    e.sample_accession,
    e.organism,
    e.taxon_id,
    e.BioSample,

    -- Study statistics
    ss.experiment_count,

    -- Metadata quality
    st.has_complete_metadata,

    -- Timestamps
    st._loaded_at AS study_loaded_at

FROM staging.stg_sra_studies st
LEFT JOIN experiment_sample_join e
    ON st.study_accession = e.study_accession
LEFT JOIN study_stats ss
    ON st.study_accession = ss.study_accession
