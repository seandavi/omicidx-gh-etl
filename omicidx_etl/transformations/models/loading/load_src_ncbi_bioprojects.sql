copy (
  select * from read_parquet('/data/davsean/omicidx_root/biosample/bioproject*.parquet', union_by_name := true)
) to '/data/davsean/omicidx_root/exports/src_ncbi_bioprojects.parquet' (format 'parquet', compression zstd);
