from upath import UPath
import polars as pl
import click
from loguru import logger
import tenacity

@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(2))
def read_csv_from_remote(csv_file: UPath) -> pl.DataFrame:
    db = csv_file.stem
    df = pl.read_csv(str(csv_file), infer_schema_length=1000000).with_columns(
        db = pl.lit(db)
    )
    return df


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

    csv_file_list = list(textmined_dir.glob("*.csv"))

    for i, csv_file in enumerate(csv_file_list):
        db = csv_file.stem
        logger.info(f"Processing {csv_file}")
        logger.info(f"File {i+1} of {len(csv_file_list)}...")
        outfile = output_dir / f'{db}_link.parquet'
        outfile.parent.mkdir(parents=True, exist_ok=True)

        df = read_csv_from_remote(csv_file)
        replace_column = df.columns[0]
        df = df.rename({replace_column: "db_id"})
        df.write_parquet(outfile, compression="zstd")
        logger.info(f"Wrote {outfile} with {df.height} rows.")

if __name__ == "__main__":
    csv_to_parquet()
