copy (
SELECT
  *
FROM read_ndjson_auto('/data/davsean/omicidx_root/geo/gse*.ndjson.gz', union_by_name=True)
) to '/data/davsean/omicidx_root/exports/src_geo_series.parquet' (format 'parquet', compression zstd);
