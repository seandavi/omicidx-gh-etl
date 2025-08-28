from upath import UPath
import polars as pl
import tqdm
import click
from loguru import logger

OUTPUT_DIR = UPath("/Users/davsean/data/omicidx/europepmc/")

@click.group()
def europepmc():
    """Europe PMC ETL commands."""
    pass

@europepmc.command("extract")
@click.argument(
    'output_dir', 
    type=click.Path(path_type=UPath), 
)
def csv_to_parquet(output_dir: UPath):
    textmined_dir = UPath("https://europepmc.org/pub/databases/pmc/TextMinedTerms/")
    
    for i, csv_file in enumerate(list(textmined_dir.glob("*.csv"))):
        db = csv_file.stem
        logger.info(f"Processing {csv_file}")
        logger.info(f"File {i+1} of {len(list(textmined_dir.glob('*.csv')))}...")
        outfile = OUTPUT_DIR / f'{db}_link.parquet'
        outfile.parent.mkdir(parents=True, exist_ok=True)

        df = pl.read_csv(str(csv_file), infer_schema_length=1000000).with_columns(
            db = pl.lit(db)
        )
        replace_column = df.columns[0]
        df = df.rename({replace_column: "db_id"})
        df.write_parquet(outfile, compression="zstd")
        logger.info(f"Wrote {outfile} with {df.height} rows.")
    
if __name__ == "__main__":
    csv_to_parquet()
    logger.info("Europe PMC TextMined terms processed successfully.")
    logger.info(f"Output files are stored in {OUTPUT_DIR}")