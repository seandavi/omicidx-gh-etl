
copy (
    select * from read_ndjson_auto('{ROOT_PATH}/geo/gsm*.ndjson.gz', union_by_name := true)
)
to '{ROOT_PATH}/transformed/gsm.parquet' (format parquet , compression zstd);

copy (
    select * from read_ndjson_auto('{ROOT_PATH}/geo/gse*.ndjson.gz', union_by_name := true)
)
to '{ROOT_PATH}/transformed/gse.parquet' (format parquet , compression zstd);


copy (
    select * from read_ndjson_auto('{ROOT_PATH}/geo/gpl*.ndjson.gz', union_by_name := true)
)
to '{ROOT_PATH}/transformed/gpl.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/sra/*study*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/sra_studies.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/sra/*experiment*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/sra_experiments.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/sra/*sample*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/sra_samples.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/sra/*run*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/sra_runs.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/sra/*run*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/sra_runs.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/biosample/biosample*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/ncbi_biosamples.parquet' (format parquet , compression zstd);

copy (
    select * from read_parquet('{ROOT_PATH}/biosample/bioproject*.parquet', union_by_name := true)
)
to '{ROOT_PATH}/transformed/ncbi_bioprojects.parquet' (format parquet , compression zstd);