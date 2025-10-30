-- Raw NCBI BioSamples from parquet source

SELECT * FROM read_parquet('/data/davsean/omicidx_root/biosample/biosample*.parquet', union_by_name := true)
