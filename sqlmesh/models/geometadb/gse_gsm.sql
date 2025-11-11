-- GEOmetadb compatibility view: GSE-GSM junction table
-- Links GEO Series to GEO Samples (many-to-many relationship)
MODEL (
    name geometadb.gse_gsm,
    kind VIEW
);

SELECT DISTINCT
    accession AS gse,
    UNNEST(sample_id) AS gsm
FROM bronze.stg_geo_series
