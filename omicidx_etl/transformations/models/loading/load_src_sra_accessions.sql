copy (
    select * from
    read_csv_auto('https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab', nullstr=['-'])
) to '/data/davsean/omicidx_root/src_sra_accessions.parquet' (format parquet, compression zstd);
