import argparse
import logging
import sqlite3 as sqlite
import sys
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from omegaconf import OmegaConf
from tqdm import tqdm

DEVRUN_MAX_CHUNKS = 6

logger = logging.getLogger(__name__)


def setup_logging(log_level, caller_name):
    """Helper function for logging."""
    logger.setLevel(log_level)
    log_filename = caller_name.with_suffix(".log")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Add a stream handler for console output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


class TqdmFile:
    """Class to handle tqdm output to console when using logger."""

    def __init__(self, file):
        self.file = file

    def write(self, x):  # noqa: D102
        if len(x.rstrip()) > 0:
            tqdm.write(x, file=self.file)

    def flush(self):  # noqa: D102
        self.file.flush()


def db_to_file(  # noqa: PLR0913
    con: sqlite.Connection,
    sql_source: str,
    destination: Path,
    chunksize: int = 10_000,
    schema: None | list[tuple] = None,
    devrun: bool = False,
):
    """Read from database in chunks and write to parquet.

    Args:
        con (sqlite3.connection): Connection to the database.
        sql_source (str): name of the table or view in the database.
        destination: path to the parquet file to write the data to.
        chunksize (int): size of each chunk to be processed.
        schema (list): list of tuples for parquet data schema. Is converted to
        `pa.schema`. If `None`, use default conversion from pandas to arrow,
        which can lead to errors.
        devrun (bool): If True, runs only a few chunks for each query.
    """
    view_prefix = "v_"

    pqwriter = None
    if schema is not None:
        schema = pa.schema(schema)

    source_name = f"{view_prefix}{sql_source}"
    for i, chunk in tqdm(
        enumerate(
            pd.read_sql(
                f"select * from {source_name}",  # noqa: S608
                con,
                chunksize=chunksize,
            )
        ),
        desc=f"Processing {sql_source}",
        file=TqdmFile(sys.stdout),
        ncols=70,
    ):
        table = pa.Table.from_pandas(chunk, schema=schema)

        if i == 0:
            pqwriter = pq.ParquetWriter(destination, table.schema)

        if not pqwriter:
            raise RuntimeError

        pqwriter.write_table(table)
        if i > DEVRUN_MAX_CHUNKS and devrun:
            break

    if pqwriter:
        pqwriter.close()


def main(args):
    """Read tables specified in config and write to parquet."""
    config = OmegaConf.load("config.yaml")
    con = sqlite.connect(config.db.path)
    for query in config.db.views.values():
        con.execute(query)

    schema_dict = config.output.schema
    for sql_source in config.db.views:
        logger.info("Processing %s", sql_source)
        outfile = Path(config.output.path) / sql_source
        outfile = outfile.with_suffix(config.output.extension)
        schema = None
        if sql_source in schema_dict:
            schema = list(schema_dict[sql_source].items())
        db_to_file(con, sql_source, outfile, schema=schema, devrun=args.devrun)

    con.close()
    logger.info("Finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--devrun", action=argparse.BooleanOptionalAction, help="Fast dev run")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    args = parser.parse_args()

    caller_name = Path(__file__).resolve()
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level, caller_name)

    args = parser.parse_args()
    main(args)
