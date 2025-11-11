-- GEOmetadb compatibility view: Supplemental files from GEO Series and Samples
-- Unnests supplemental file arrays and extracts filenames
MODEL (
    name geometadb.geo_supplemental_files,
    kind VIEW
);

WITH supp_file AS (
    SELECT
        accession,
        'gse' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM bronze.stg_geo_series

    UNION ALL

    SELECT
        accession,
        'gsm' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM bronze.stg_geo_samples
)
SELECT
    accession,
    accession_type,
    supplemental_file,
    regexp_extract(supplemental_file, '[^/]+$') AS filename
FROM supp_file
WHERE supplemental_file != 'NONE'
