create or replace view src_geo_samples as
select * from read_parquet('/data/davsean/omicidx_root/exports/src_geo_samples.parquet');
