copy (
SELECT
  *
FROM read_ndjson_auto('/data/davsean/omicidx_root/geo/gsm*.ndjson.gz', union_by_name=True)
) to '/data/davsean/omicidx_root/exports/src_geo_samples.parquet' (format 'parquet', compression zstd);
