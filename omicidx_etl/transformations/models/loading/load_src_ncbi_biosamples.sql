copy (
  select * from read_parquet('/data/davsean/omicidx_root/biosample/biosample*.parquet', union_by_name := true)
) to '/data/davsean/omicidx_root/exports/src_ncbi_biosamples.parquet' (format 'parquet', compression zstd);
