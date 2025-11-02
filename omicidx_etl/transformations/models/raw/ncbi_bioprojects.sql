create or replace view src_ncbi_bioprojects as
select * from read_parquet('/data/davsean/omicidx_root/exports/src_ncbi_bioprojects.parquet');