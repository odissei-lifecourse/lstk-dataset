import argparse
import os
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from omegaconf import OmegaConf
from tqdm import tqdm

DEVRUN_MAX_CHUNKS = 6


def aggregate(query: str, tbl_src: str, result_dir: Path, result_name_suffix: str, chunk_size: int = 100_000) -> None:
    """Read from a parquet file, aggregate and save to other parquet file.

    Args:
        query (str): The aggregation query, excluding the 'FROM SRC' statement.
        tbl_src (str): The name of the table to query.
        result_dir (pathlib.Path): directory to store the result.
        result_name_suffix (str): The suffix to append to the name of the result
        chunk_size (int): The size of chunks processed in an iteration.

    This uses duckdb's fetch_record_batch functionality and streams data in batches.
    If the source file is `/path/file.parquet`, the destination is `/path/file_result_name_suffix.parquet`.

    """
    devrun = os.getenv("DEVRUN", "False") == "True"

    query = f"{query} FROM '{tbl_src!s}' ORDER BY Year, AuthorId"  # ordering seems to speed up queries 5x
    destination = f"{tbl_src.stem}{result_name_suffix}"
    destination = result_dir / Path(destination).with_suffix(tbl_src.suffix)

    with duckdb.connect() as con:
        arrow_stream = con.execute(query).fetch_record_batch(rows_per_batch=chunk_size)

        first_batch = next(arrow_stream)
        with pq.ParquetWriter(destination, first_batch.schema) as writer:
            writer.write_batch(first_batch)
            for i, batch in tqdm(enumerate(arrow_stream), desc="Processing batches"):
                writer.write_batch(batch)
                if devrun and i > DEVRUN_MAX_CHUNKS:
                    break


def main():
    """Run aggregation queries for files specified in config."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--devrun", action=argparse.BooleanOptionalAction, help="Fast dev run")

    args = parser.parse_args()

    if args.devrun:
        os.environ["DEVRUN"] = "True"

    config = OmegaConf.load("config.yaml")
    result_dir = Path(config.output.path)

    for tbl, query in config.aggregate.tables.items():
        tbl_src = (Path(config.output.path) / Path(tbl)).with_suffix(config.output.extension)
        aggregate(query, tbl_src, result_dir, config.aggregate.suffix)


if __name__ == "__main__":
    main()
