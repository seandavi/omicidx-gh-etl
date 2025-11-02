copy (
  select * from read_parquet('/data/davsean/omicidx_root/sra/*run*.parquet', union_by_name := true)
) to '/data/davsean/omicidx_root/exports/src_sra_runs.parquet' (format 'parquet', compression zstd);
