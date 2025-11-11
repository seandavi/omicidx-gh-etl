-- GEOmetadb compatibility view: GSE-GPL junction table
-- Links GEO Series to GEO Platforms (many-to-many relationship)
MODEL (
    name geometadb.gse_gpl,
    kind VIEW
);

SELECT DISTINCT
    accession AS gpl,
    UNNEST(series_id) AS gse
FROM bronze.stg_geo_platforms
