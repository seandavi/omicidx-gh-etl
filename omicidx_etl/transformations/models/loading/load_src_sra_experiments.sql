copy (
  select * from read_parquet('/data/davsean/omicidx_root/sra/*experiment*.parquet', union_by_name := true)
) to '/data/davsean/omicidx_root/exports/src_sra_experiments.parquet' (format 'parquet', compression zstd);
