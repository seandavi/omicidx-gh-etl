-- Raw SRA studies from parquet source
-- This is the entry point from your ETL pipeline to the warehouse
-- Minimal transformation: just consolidate parquet files

SELECT * FROM read_parquet('/data/davsean/omicidx_root/sra/*study*.parquet', union_by_name := true)
