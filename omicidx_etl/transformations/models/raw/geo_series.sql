create or replace view src_geo_series as
select * from read_parquet('/data/davsean/omicidx_root/exports/src_geo_series.parquet');