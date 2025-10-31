
select * from 
read_ndjson_auto('/data/davsean/omicidx_root/geo/gse*.ndjson.gz', union_by_name=True)