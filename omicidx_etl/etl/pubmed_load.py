from . import db

sql = """
CREATE OR REPLACE TABLE
src_pubmed__metadata
ENGINE=MergeTree()
ORDER BY tuple(pmid)
SETTINGS 
  storage_policy='s3_main'
AS
SELECT 
  * EXCEPT pmid,
  pmid::UInt32 AS pmid
FROM 
s3('https://storage.googleapis.com/omicidx-json/pubmed/*jsonl.gz', JSONEachRow)
SETTINGS 
max_table_size_to_drop='100G';
"""


def load_pubmed_to_clickhouse():
    client = db.get_client()
    client.command(sql)


if __name__ == "__main__":
    load_pubmed_to_clickhouse()
    print("Done")
