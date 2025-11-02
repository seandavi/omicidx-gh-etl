-- This is a comprehensive index of all SRA accessions across all entity types
create or replace view src_sra_accessions as
    select * from
    read_parquet('/data/davsean/omicidx_root/src_sra_accessions.parquet');