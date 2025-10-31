-- Raw SRA runs from parquet source

SELECT * FROM read_parquet('/data/davsean/omicidx_root/sra/*run*.parquet', union_by_name := true)
