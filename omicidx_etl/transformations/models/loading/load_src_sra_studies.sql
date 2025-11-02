copy (
  select * from read_parquet('/data/davsean/omicidx_root/sra/*study*.parquet', union_by_name := true)
) to '/data/davsean/omicidx_root/exports/src_sra_studies.parquet' (format 'parquet', compression zstd);
