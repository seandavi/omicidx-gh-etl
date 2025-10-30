# omicidx-gh-etl

ETL pipelines and data warehouse for OmicIDX - a comprehensive metadata resource for genomics and transcriptomics data.

## Data Warehouse

This repository includes a DuckDB-based data warehouse that transforms raw ETL outputs into production-ready datasets:

- **3-layer architecture**: raw → staging → mart
- **Export system**: Automatic parquet export for models
- **Deployment tools**: Deploy to Cloudflare R2 with catalog and remote database
- **Zero dependencies**: Pure SQL transformations, no dbt required

### Quick Start

```bash
# Run warehouse transformations
uv run oidx warehouse run

# Deploy to R2
uv run oidx warehouse deploy all
```

### Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Complete deployment guide for Cloudflare R2
- [EXPORT_DEPLOYMENT.md](EXPORT_DEPLOYMENT.md) - Export materialization reference
- [warehouse.yml.example](warehouse.yml.example) - Configuration template

## ETL status badges

| Workflow | Description |
|-------|------| 
| [![Biosample](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_biosample.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_biosample.yaml) | All metadata from the NCBI Biosample and Bioproject databases |
| [![PubMed](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/pubmed_etl.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/pubmed_etl.yaml) | PubMed metadata and abstracts |
| [![icite](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/icite.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/icite.yaml) | NIH iCite resource for article impact and citations |
| [![GEO](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/geo.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/geo.yaml) | All metadata from NCBI Gene Expression Omnibus (GEO) |
| [![SRA](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_sra_etl.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_sra_etl.yaml) | All metadata from NCBI Sequence Read Archive (SRA) |
| [![Scimago](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/scimago.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/scimago.yaml) | the Scimago Journal Impact Factor database | 


## Components

Note that in the table descriptions below, details of last modified date, bytes, etc. are not routinely updated. 
The important information is the schema description for each table. 

```
Table omicidx-338300:biodatalake.src_ncbi__biosamples

   Last modified                   Schema                  Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- -------------------------------------- ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  08 Jul 23:11:59   |- model: string                       39678095     52876118213   28 Jul 07:51:12                                          52876118213           15165586514
                    |- attributes: string (repeated)
                    |- taxonomy_name: string
                    |- description: string
                    |- title: string
                    |- gsm: string
                    |- ids: string (repeated)
                    +- id_recs: record (repeated)
                    |  |- id: string
                    |  |- label: string
                    |  |- db: string
                    |- accession: string
                    +- attribute_recs: record (repeated)
                    |  |- unit: string
                    |  |- display_name: string
                    |  |- harmonized_name: string
                    |  |- value: string
                    |  |- attribute_name: string
                    |- sra_sample: string
                    |- access: string
                    |- publication_date: timestamp
                    |- dbgap: string
                    |- last_update: timestamp
                    |- id: integer
                    |- submission_date: timestamp
                    |- taxon_id: integer
                    |- is_reference: string


Table omicidx-338300:biodatalake.src_ncbi__bioprojects

   Last modified                   Schema                  Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- -------------------------------------- ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  09 Jul 11:47:11   |- data_types: string (repeated)       822511       589564453     27 Jul 16:37:06                                          589564453             2550969738
                    |- accession: string
                    |- description: string
                    |- name: string
                    +- publications: record (repeated)
                    |  |- db: string
                    |  |- id: string
                    |  |- pubdate: timestamp
                    +- external_links: record (repeated)
                    |  |- url: string
                    |  |- label: string
                    |  |- category: string
                    +- locus_tags: record (repeated)
                    |  |- biosample_id: string
                    |  |- assembly_id: string
                    |  |- value: string
                    |- release_date: timestamp
                    |- title: string


Table omicidx-338300:biodatalake.src_geo__gpl

   Last modified                  Schema                 Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ------------------------------------ ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  08 Jul 21:35:59   |- manufacture_protocol: string      27090        101764425     24 Jul 16:59:51                                          101764425             89430852
                    |- relation: string (repeated)
                    |- data_row_count: integer
                    |- manufacturer: string (repeated)
                    |- distribution: string
                    |- description: string
                    |- series_id: string (repeated)
                    |- title: string
                    |- status: string
                    |- sample_id: string (repeated)
                    |- summary: string
                    +- contact: record
                    |  |- phone: string
                    |  |- institute: string
                    |  |- web_link: string
                    |  |- country: string
                    |  |- department: string
                    |  |- state: string
                    |  |- email: string
                    |  +- name: record
                    |  |  |- last: string
                    |  |  |- middle: string
                    |  |  |- first: string
                    |  |- address: string
                    |  |- zip_postal_code: string
                    |  |- city: string
                    |- technology: string
                    |- accession: string
                    |- contributor: json
                    |- last_update_date: date
                    |- submission_date: date
                    |- organism: string


Table omicidx-338300:biodatalake.src_geo__gsm

   Last modified                     Schema                    Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ------------------------------------------ ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  08 Jul 21:35:47   |- submission_date: date                   7711926      18843204340   24 Jul 16:57:47                                          18843204340           29750284579
                    +- channels: record (repeated)
                    |  +- characteristics: record (repeated)
                    |  |  |- value: string
                    |  |  |- tag: string
                    |  |- treatment_protocol: string
                    |  |- extract_protocol: string
                    |  |- label_protocol: string
                    |  |- source_name: string
                    |  |- organism: string
                    |  |- molecule: string
                    |  |- taxid: integer (repeated)
                    |  |- growth_protocol: string
                    |  |- label: string
                    |- status: string
                    |- overall_design: string
                    |- library_source: string
                    |- data_row_count: integer
                    |- title: string
                    |- data_processing: string
                    |- channel_count: integer
                    |- platform_id: string
                    |- tag_length: string
                    |- anchor: string
                    |- contributor: string (repeated)
                    |- biosample: string
                    |- sra_experiment: string
                    |- description: string
                    +- contact: record
                    |  |- phone: string
                    |  |- institute: string
                    |  |- web_link: string
                    |  |- country: string
                    |  |- department: string
                    |  |- state: string
                    |  |- email: string
                    |  +- name: record
                    |  |  |- last: string
                    |  |  |- middle: string
                    |  |  |- first: string
                    |  |- address: string
                    |  |- zip_postal_code: string
                    |  |- city: string
                    |- supplemental_files: string (repeated)
                    |- scan_protocol: string
                    |- tag_count: string
                    |- type: string
                    |- hyb_protocol: string
                    |- accession: string
                    |- last_update_date: date


Table omicidx-338300:biodatalake.src_geo__gse

   Last modified                  Schema                 Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ------------------------------------ ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  08 Jul 21:34:35   |- manufacture_protocol: string      261283       466364992     24 Jul 16:49:12                                          466364992             606765995
                    |- relation: string (repeated)
                    |- data_row_count: integer
                    |- manufacturer: string (repeated)
                    |- distribution: string
                    |- description: string
                    |- series_id: string (repeated)
                    |- title: string
                    |- status: string
                    |- sample_id: string (repeated)
                    |- summary: string
                    +- contact: record
                    |  |- phone: string
                    |  |- institute: string
                    |  |- web_link: string
                    |  |- country: string
                    |  |- department: string
                    |  |- state: string
                    |  |- email: string
                    |  +- name: record
                    |  |  |- last: string
                    |  |  |- middle: string
                    |  |  |- first: string
                    |  |- address: string
                    |  |- zip_postal_code: string
                    |  |- city: string
                    |- technology: string
                    |- accession: string
                    |- contributor: json
                    |- last_update_date: date
                    |- submission_date: date
                    |- organism: string


Table omicidx-338300:biodatalake.src_sra__runs

   Last modified                 Schema                 Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ----------------------------------- ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  09 Jul 01:47:29   |- accession: string                32190413     5540707350    27 Jul 13:31:38                                          5540707350            10592070106
                    |- alias: string
                    +- attributes: record (repeated)
                    |  |- tag: string
                    |  |- value: string
                    |- broker_name: string
                    |- center_name: string
                    |- experiment_accession: string
                    +- identifiers: record (repeated)
                    |  |- id: string
                    |  |- namespace: string
                    |  |- uuid: string
                    |- GEO: string
                    |- run_center: string
                    |- run_date: timestamp
                    |- title: string


Table omicidx-338300:biodatalake.src_sra__experiments

   Last modified                     Schema                    Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ------------------------------------------ ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  09 Jul 01:41:12   |- accession: string                       30079885     17238814649   27 Jul 13:25:36                                          17238814649           34933276114
                    |- alias: string
                    +- attributes: record (repeated)
                    |  |- tag: string
                    |  |- value: string
                    |- broker_name: string
                    |- center_name: string
                    |- description: string
                    |- library_layout_orientation: string
                    |- GEO: string
                    |- design: string
                    |- experiment_accession: string
                    +- identifiers: record (repeated)
                    |  |- id: string
                    |  |- namespace: string
                    |  |- uuid: string
                    |- instrument_model: string
                    |- library_construction_protocol: string
                    |- library_layout: string
                    |- library_layout_length: integer
                    |- library_layout_sdev: float
                    |- library_name: string
                    |- library_selection: string
                    |- library_source: string
                    |- library_strategy: string
                    |- nreads: integer
                    |- platform: string
                    +- reads: record (repeated)
                    |  |- base_coord: integer
                    |  |- read_class: string
                    |  |- read_index: integer
                    |  |- read_type: string
                    |- sample_accession: string
                    |- spot_length: integer
                    |- study_accession: string
                    |- title: string
                    +- xrefs: record (repeated)
                    |  |- id: string
                    |  |- db: string


Table omicidx-338300:biodatalake.src_sra__samples

   Last modified                 Schema                 Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ----------------------------------- ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  09 Jul 01:26:50   |- accession: string                30777187     23584148305   27 Jul 13:12:26                                          23584148305           37063254550
                    |- broker_name: string
                    |- alias: string
                    +- attributes: record (repeated)
                    |  |- tag: string
                    |  |- value: string
                    |- BioSample: string
                    |- center_name: string
                    |- description: string
                    |- GEO: string
                    +- identifiers: record (repeated)
                    |  |- id: string
                    |  |- uuid: string
                    |  |- namespace: string
                    |- organism: string
                    |- sample_accession: string
                    |- taxon_id: integer
                    |- title: string
                    +- xrefs: record (repeated)
                    |  |- db: string
                    |  |- id: string


Table omicidx-338300:biodatalake.src_sra__studies

   Last modified                 Schema                 Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ----------------------------------- ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  09 Jul 01:06:05   |- pubmed_ids: string (repeated)    531398       402484273     27 Jul 12:49:56                                          402484273             1075548776
                    |- status: string
                    |- update_date: timestamp
                    |- meta_update_date: timestamp
                    |- publish_date: timestamp
                    |- received_date: timestamp
                    |- visibility: string
                    |- insdc: boolean
                    |- accession: string
                    |- alias: string
                    |- title: string
                    |- center_name: string
                    |- broker_name: string
                    |- description: string
                    +- attributes: record (repeated)
                    |  |- value: string
                    |  |- tag: string
                    +- identifiers: record (repeated)
                    |  |- id: string
                    |  |- namespace: string
                    +- xrefs: record (repeated)
                    |  |- id: string
                    |  |- db: string
                    |- study_type: string
                    |- study_accession: string
                    |- abstract: string
                    |- BioProject: string
                    |- GEO: string


Table omicidx-338300:biodatalake.src_pubmed__metadata

   Last modified                 Schema                Total Rows   Total Bytes      Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- ---------------------------------- ------------ -------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  09 Jul 01:04:47   |- _inserted_at: timestamp         40779946     123857132228   24 Jul 16:00:59                                          123857132228          595461806081
                    |- _read_from: string
                    |- abstract: string
                    +- authors: record (repeated)
                    |  |- affiliation: string
                    |  |- forename: string
                    |  |- identifier: string
                    |  |- initials: string
                    |  |- lastname: string
                    |- chemical_list: string
                    |- country: string
                    |- delete: boolean
                    |- doi: string
                    +- grant_ids: record (repeated)
                    |  |- agency: string
                    |  |- country: string
                    |  |- grant_acronym: string
                    |  |- grant_id: string
                    |- issn_linking: string
                    |- issue: string
                    |- journal: string
                    |- keywords: string
                    |- languages: string
                    |- medline_ta: string
                    |- mesh_terms: string
                    |- nlm_unique_id: string
                    |- other_id: string
                    |- pages: string
                    |- pmc: string
                    |- pmid: integer
                    |- pubdate: string
                    |- publication_types: string
                    +- references: record (repeated)
                    |  |- citation: string
                    |  |- pmid: string
                    |- title: string
                    |- vernacular_title: string


Table omicidx-338300:biodatalake.src_icite

   Last modified                   Schema                   Total Rows   Total Bytes     Expiration      Time Partitioning   Clustered Fields   Total Logical Bytes   Total Physical Bytes   Labels
 ----------------- --------------------------------------- ------------ ------------- ----------------- ------------------- ------------------ --------------------- ---------------------- --------
  28 Jun 14:35:18   |- last_modified: string                37355990     26489744258   28 Jul 14:35:18                                          26489744258           10653309617
                    |- references: string
                    |- y_coord: float
                    |- cited_by: string
                    |- is_clinical: boolean
                    |- cited_by_clin: string
                    |- x_coord: float
                    |- authors: string
                    |- human: float
                    |- nih_percentile: float
                    |- relative_citation_ratio: float
                    |- citations_per_year: float
                    |- apt: float
                    |- animal: float
                    |- field_citation_rate: float
                    |- is_research_article: boolean
                    |- pmid: integer
                    |- journal: string
                    |- provisional: boolean
                    |- title: string
                    |- year: integer
                    |- citation_count: integer
                    |- molecular_cellular: float
                    |- expected_citations_per_year: float
                    |- doi: string

```
