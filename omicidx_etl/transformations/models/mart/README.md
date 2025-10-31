# Data Mart Models

This directory contains production-ready analytical models following dimensional modeling principles (star schema). These models are optimized for querying, reporting, and downstream analytics.

## Architecture Overview

The mart layer implements a **star schema** with:
- **Fact tables**: Transactional/event data with measures and foreign keys
- **Dimension tables**: Descriptive attributes for filtering and grouping
- **Bridge tables**: Many-to-many relationships between facts and dimensions
- **Aggregate tables**: Pre-computed metrics for performance

```
staging/ (cleaned, normalized)
    ↓
mart/
    ├── dimensions/     (who, what, where, when)
    ├── facts/          (measures, metrics, events)
    ├── bridges/        (many-to-many relationships)
    └── aggregates/     (pre-computed summaries)
```

---

## Naming Conventions

### File Naming
- **Dimensions**: `dim_<entity>.sql` (e.g., `dim_studies.sql`, `dim_organisms.sql`)
- **Facts**: `fact_<process>_<grain>.sql` (e.g., `fact_sequencing_runs.sql`, `fact_publications.sql`)
- **Bridges**: `bridge_<entity1>_<entity2>.sql` (e.g., `bridge_study_bioproject.sql`)
- **Aggregates**: `agg_<entity>_<metrics>.sql` (e.g., `agg_study_metrics.sql`)
- **Legacy/wide tables**: `<domain>_metadata.sql` (e.g., `sra_metadata.sql`) - for backward compatibility

### Column Naming
- **Primary keys**: `<entity>_key` (surrogate integer key, e.g., `study_key`, `sample_key`)
- **Natural keys**: `<entity>_accession` (e.g., `study_accession`, `sample_accession`)
- **Foreign keys**: Same as dimension primary key (e.g., `study_key` in fact table)
- **Measures**: Descriptive names with units (e.g., `spot_count`, `base_count`, `experiment_count`)
- **Flags**: `is_<condition>` or `has_<attribute>` (e.g., `is_public`, `has_biosample_link`)
- **Dates**: `<event>_date` (e.g., `publish_date`, `submission_date`)
- **Timestamps**: `<event>_at` (e.g., `_loaded_at`, `updated_at`)
- **Categories**: `<attribute>_type` or `<attribute>_category` (e.g., `study_type`, `platform_category`)

### Schema Prefixes
- No prefix: Default schema (or explicit `mart.`)
- `staging.`: Cleaned staging models
- `raw.`: Raw source data

---

## Design Principles

### 1. Grain Definition
Every fact table MUST have a clearly defined grain (one row = what?):
```sql
-- ✓ GOOD: Clear grain
-- fact_sequencing_runs: One row per SRA run (SRR accession)

-- ✗ BAD: Ambiguous grain
-- sra_data: Unclear what one row represents
```

### 2. Surrogate Keys
- Use integer surrogate keys (`ROW_NUMBER()` or `SEQUENCE`) for:
  - Better join performance
  - Handling natural key changes (SCD Type 2)
  - Smaller index sizes
- Always include the natural key (accession) for reference

```sql
-- Standard surrogate key pattern
SELECT 
    ROW_NUMBER() OVER (ORDER BY accession) AS study_key,  -- Surrogate key
    accession AS study_accession,                          -- Natural key
    ...
FROM staging.stg_sra_studies
```

### 3. Conformed Dimensions
Dimensions shared across multiple fact tables must be conformed (identical structure):
- `dim_organisms`: Used by SRA samples, GEO samples, BioSamples
- `dim_studies`: Unified view of SRA studies, GEO series, BioProjects
- `dim_date`: Standard date dimension for all temporal analysis

### 4. Slowly Changing Dimensions (SCD)
For dimensions that change over time:
- **Type 1**: Overwrite (for corrections, use for most dimensions)
- **Type 2**: Track history with `effective_from`, `effective_to`, `is_current`

```sql
-- SCD Type 2 pattern
SELECT 
    sample_key,
    sample_accession,
    organism,
    _loaded_at AS effective_from,
    NULL AS effective_to,      -- NULL means current
    TRUE AS is_current         -- Flag for active record
FROM staging.stg_sra_samples
```

### 5. Denormalization for Performance
Dimensions should be denormalized to avoid joins during queries:
```sql
-- ✓ GOOD: Denormalized dimension
SELECT 
    platform,
    instrument_model,
    platform_category,      -- Derived attribute
    instrument_family,      -- Derived attribute
    manufacturer            -- Would be in separate table in 3NF
FROM dim_platforms

-- ✗ BAD: Normalized (requires joins)
SELECT p.platform, m.manufacturer
FROM dim_platforms p
JOIN dim_manufacturers m ON p.manufacturer_id = m.id
```

### 6. Bridge Tables for Many-to-Many
Use bridge tables to resolve many-to-many relationships:
```sql
-- One study can have multiple BioProjects
-- One BioProject can be linked to multiple studies
SELECT 
    study_key,
    bioproject_accession
FROM bridge_study_bioproject
```

### 7. Fact Table Measures
- Include only numeric measures and foreign keys in fact tables
- Move descriptive attributes to dimensions
- Include degenerate dimensions (e.g., run_accession) if low cardinality

```sql
-- ✓ GOOD: Fact table structure
SELECT 
    run_key,               -- Degenerate dimension or PK
    experiment_key,        -- FK to dim_experiments
    sample_key,            -- FK to dim_samples
    date_key,              -- FK to dim_date
    spot_count,            -- Measure
    base_count,            -- Measure
    run_date               -- Timestamp measure
FROM fact_sequencing_runs

-- ✗ BAD: Don't include descriptive text
SELECT 
    ...,
    organism_name,         -- Should be in dim_organisms
    study_title,           -- Should be in dim_studies
    experiment_description -- Should be in dim_experiments
FROM fact_sequencing_runs
```

---

## Data Domains

### SRA (Sequence Read Archive)
**Grain hierarchy**: Study → Experiment → Sample → Run

**Entities**:
- Studies (SRP/ERP/DRP): Research project level
- Experiments (SRX/ERX/DRX): Library preparation and sequencing design
- Samples (SRS/ERS/DRS): Biological material
- Runs (SRR/ERR/DRR): Sequencing output files

**Recommended models**:
- `dim_studies` (includes SRA studies)
- `dim_experiments`
- `dim_samples` (unified with BioSamples)
- `dim_platforms`
- `dim_organisms`
- `fact_sequencing_runs` (grain: one row per run)
- `bridge_experiment_sample` (if needed for many-to-many)

### GEO (Gene Expression Omnibus)
**Grain hierarchy**: Series (GSE) → Sample (GSM) → Platform (GPL)

**Entities**:
- Series (GSE): Study/experiment
- Samples (GSM): Individual samples
- Platforms (GPL): Array or sequencing platform

**Recommended models**:
- `dim_studies` (includes GEO series)
- `dim_geo_platforms`
- `dim_samples` (unified)
- `fact_geo_series_samples` (grain: one row per GSE-GSM pair)
- `bridge_series_platform`

### BioSample & BioProject
**Cross-cutting dimensions** that link SRA, GEO, and other NCBI databases

**Recommended models**:
- `dim_samples` (unified with SRA and GEO samples)
- `dim_bioprojects`
- `bridge_study_bioproject`
- `bridge_sample_biosample`

### PubMed & Publications
**For citation analysis and linking studies to literature**

**Recommended models**:
- `dim_publications`
- `bridge_study_publication`
- `fact_citations` (grain: one row per citation relationship)

---

## Materialization Strategy

### Dimensions
```yaml
materialized: export_table
export:
  enabled: true
  path: "marts/dim_<entity>.parquet"
  compression: zstd
  row_group_size: 100000
```
- Materialize as tables for stability
- Refresh periodically (daily or weekly)
- Use zstd compression for balance of size/speed

### Facts
```yaml
# Option 1: Always fresh (smaller datasets)
materialized: export_view

# Option 2: Incremental (large datasets)
materialized: export_table
incremental: true
partition_by: ["year", "month"]
```

### Aggregates
```yaml
materialized: export_table
export:
  enabled: true
  path: "marts/agg_<entity>_<metric>.parquet"
```
- Always materialize for performance
- Refresh when underlying facts change

---

## Query Patterns

### Standard Join Pattern
```sql
-- Always join through foreign keys
SELECT 
    s.study_title,
    o.organism_name,
    p.platform_category,
    SUM(f.spot_count) AS total_spots
FROM fact_sequencing_runs f
JOIN dim_studies s ON f.study_key = s.study_key
JOIN dim_samples sam ON f.sample_key = sam.sample_key
JOIN dim_organisms o ON sam.organism_key = o.organism_key
JOIN dim_experiments e ON f.experiment_key = e.experiment_key
JOIN dim_platforms p ON e.platform_key = p.platform_key
WHERE s.study_type = 'Transcriptome Analysis'
  AND o.organism_name = 'Homo sapiens'
GROUP BY s.study_title, o.organism_name, p.platform_category
```

### Time-Series Analysis
```sql
-- Use dim_date for time-based queries
SELECT 
    d.year,
    d.quarter,
    COUNT(DISTINCT f.study_key) AS study_count,
    SUM(f.spot_count) AS total_spots
FROM fact_sequencing_runs f
JOIN dim_date d ON CAST(f.run_date AS DATE) = d.date_value
WHERE d.year >= 2020
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter
```

### Bridge Table Navigation
```sql
-- Find all studies linked to a BioProject
SELECT DISTINCT
    s.study_accession,
    s.study_title
FROM dim_studies s
JOIN bridge_study_bioproject b ON s.study_key = b.study_key
WHERE b.bioproject_accession = 'PRJNA12345'
```

---

## Common Pitfalls to Avoid

### ❌ Don't create fact tables without measures
```sql
-- BAD: This is just a bridge table, not a fact
SELECT study_key, sample_key
FROM fact_study_samples  -- Should be bridge_study_samples
```

### ❌ Don't denormalize facts into dimensions
```sql
-- BAD: Don't put aggregated metrics in dimensions
SELECT 
    study_key,
    study_title,
    total_experiments,  -- This belongs in agg_study_metrics
    total_samples       -- Not in dim_studies
FROM dim_studies
```

### ❌ Don't create dimensions for every column
```sql
-- BAD: Over-dimensioning
dim_library_strategies  -- Just put this in dim_experiments
dim_instrument_models   -- Just put this in dim_platforms
```

### ❌ Don't mix grains in a single fact table
```sql
-- BAD: Mixed grain
SELECT 
    study_accession,    -- Study grain
    run_accession,      -- Run grain (causes fan-out)
    total_spots         -- Ambiguous: study-level or run-level?
FROM fact_mixed_grain
```

---

## Version History & Schema Evolution

### Adding New Columns
- Add to end of SELECT for backward compatibility
- Update schema.yml documentation
- Consider if column belongs in dimension vs fact

### Changing Keys
- Never change surrogate key generation logic (breaks downstream)
- If natural key changes, consider SCD Type 2

### Deprecating Models
- Move to `deprecated/` subdirectory
- Add deprecation notice in schema.yml
- Keep for 2-3 release cycles before removing

---

## Testing & Quality

### Required Tests
All dimension and fact models should include:

```yaml
# In schema.yml
models:
  - name: dim_studies
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - study_key
      
    columns:
      - name: study_key
        tests:
          - unique
          - not_null
      
      - name: study_accession
        tests:
          - not_null
```

### Data Quality Checks
- Primary keys are unique and not null
- Foreign keys exist in parent dimensions
- Measures are within expected ranges
- Dates are valid and in expected range
- No unintended NULLs in required fields

---

## Examples

See existing models:
- `sra_metadata.sql` - Legacy wide table (pre-dimensional)
- Future: `dim_studies.sql`, `fact_sequencing_runs.sql`, etc.

---

## References

- **Kimball Dimensional Modeling**: "The Data Warehouse Toolkit" by Ralph Kimball
- **Star Schema**: https://en.wikipedia.org/wiki/Star_schema
- **Slowly Changing Dimensions**: https://en.wikipedia.org/wiki/Slowly_changing_dimension
- **dbt Best Practices**: https://docs.getdbt.com/guides/best-practices

---

## Contributing

When adding new models to this directory:

1. Follow naming conventions strictly
2. Define grain explicitly in model comment
3. Document in schema.yml with column descriptions
4. Add appropriate tests
5. Update this README if introducing new patterns
6. Consider impact on existing models and downstream consumers

---

**Last Updated**: October 30, 2025  
**Maintainer**: OmicIDX Team
