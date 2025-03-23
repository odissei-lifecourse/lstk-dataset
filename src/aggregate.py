import argparse
import os
from pathlib import Path
import duckdb
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from omegaconf import OmegaConf
from omegaconf.dictconfig import DictConfig
from pyarrow.lib import RecordBatchReader
from tqdm import tqdm

DEVRUN_MAX_CHUNKS = 6


def write_batches(stream: RecordBatchReader, destination: str):
    """Write a RecordBatchReader to parquet."""
    devrun = os.getenv("DEVRUN", "False") == "True"

    first_batch = next(stream)
    with pq.ParquetWriter(destination, first_batch.schema) as writer:
        writer.write_batch(first_batch)
        for i, batch in tqdm(enumerate(stream), desc="Processing batches"):
            writer.write_batch(batch)
            if devrun and i > DEVRUN_MAX_CHUNKS:
                break


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
    query = f"{query} FROM '{tbl_src!s}' ORDER BY Year, AuthorId"  # ordering seems to speed up queries 5x
    destination = f"{tbl_src.stem}{result_name_suffix}"
    destination = result_dir / Path(destination).with_suffix(tbl_src.suffix)

    with duckdb.connect() as con:
        arrow_stream = con.execute(query).fetch_record_batch(rows_per_batch=chunk_size)
        write_batches(arrow_stream, destination)


def postprocess(task_config: DictConfig, output_config: DictConfig, chunk_size: int = 50_000):
    """Postprocess tables.

    Args:
        task_config (DictConfig): specifies input tables, query and name of the output.
    (through output.suffix.preprocess and reference_table.
        output_config (DictConfig): defines extension, path, and suffix.postprocess.
        chunk_size (int): Number of rows to process per batch.
    """
    data_dict = {}
    for tbl_name, file_name in task_config.inputs.items():
        file_path = (output_config.path / Path(file_name)).with_suffix(output_config.extension)
        data_dict[tbl_name] = ds.dataset(file_path).to_table()

    output_name = Path(f"{task_config.reference_table}{output_config.suffix.postprocess}")
    destination = (output_config.path / output_name).with_suffix(output_config.extension)
    with duckdb.connect() as con:
        for tbl_name, tbl in data_dict.items():
            con.register(tbl_name, tbl)
        arrow_stream = con.execute(task_config.query).fetch_record_batch(rows_per_batch=chunk_size)
        write_batches(arrow_stream, destination)


def main():
    """Run aggregation and postprocessing queries for files specified in config."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--devrun", action=argparse.BooleanOptionalAction, help="Fast dev run")

    args = parser.parse_args()

    if args.devrun:
        os.environ["DEVRUN"] = "True"

    config = OmegaConf.load("config.yaml")
    result_dir = Path(config.output.path)
    for task_config in config.postprocess.values():
        postprocess(task_config, config.output)

    for tbl, query in config.aggregate.tables.items():
        tbl_src = (Path(config.output.path) / Path(tbl)).with_suffix(config.output.extension)
        aggregate(query, tbl_src, result_dir, config.output.suffix.aggregate)


if __name__ == "__main__":
    main()
