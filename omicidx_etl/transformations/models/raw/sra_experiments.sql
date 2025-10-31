-- Raw SRA experiments from parquet source

SELECT * FROM read_parquet('/data/davsean/omicidx_root/sra/*experiment*.parquet', union_by_name := true)
