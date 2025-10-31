-- Raw SRA samples from parquet source

SELECT * FROM read_parquet('/data/davsean/omicidx_root/sra/*sample*.parquet', union_by_name := true)
