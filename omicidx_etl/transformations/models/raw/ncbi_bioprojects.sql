-- Raw NCBI BioProjects from parquet source

SELECT * FROM read_parquet('/data/davsean/omicidx_root/biosample/bioproject*.parquet', union_by_name := true)
