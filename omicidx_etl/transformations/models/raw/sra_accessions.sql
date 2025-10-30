-- Raw SRA accessions from parquet source
-- This is a comprehensive index of all SRA accessions across all entity types

SELECT * FROM read_csv_auto('https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab', nullstr=['-']) limit 500
