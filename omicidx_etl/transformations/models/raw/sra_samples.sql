create or replace view src_sra_samples as
select * from read_parquet('/data/davsean/omicidx_root/exports/src_sra_samples.parquet');
