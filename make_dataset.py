import logging
import sqlite3 as sqlite
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from omegaconf import OmegaConf
from tqdm import tqdm

logger = logging.getLogger("database")


def db_to_file(con: sqlite.Connection,
               sql_source: str,
               destination: Path,
               chunksize: int=10_000,
               schema: None | list[tuple]=None):
    """Read from database in chunks and write to parquet.

    Args:
        con (sqlite3.connection): Connection to the database.
        sql_source (str): name of the table or view in the database.
        destination: path to the parquet file to write the data to.
        chunksize (int): size of each chunk to be processed.
        schema (list): list of tuples for parquet data schema. Is converted to
        `pa.schema`. If `None`, use default conversion from pandas to arrow,
        which can lead to errors.
    """
    view_prefix = "v_"
    devrun_maxiter = 5

    pqwriter = None
    if schema is not None:
        schema = pa.schema(schema)

    source_name = f"{view_prefix}{sql_source}"
    for i, chunk in tqdm(enumerate(pd.read_sql("select * from ?", con, chunksize=chunksize,
                                          params=(source_name,))),
                         desc=f"Processing {sql_source}"
                         ):
        table = pa.Table.from_pandas(chunk, schema=schema)

        if i == 0:
            pqwriter = pq.ParquetWriter(destination, table.schema)

        if not pqwriter:
            raise RuntimeError

        pqwriter.write_table(table)
        if i > devrun_maxiter:
            break

    # close the parquet writer
    if pqwriter:
        pqwriter.close()



def main():
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
        db_to_file(con, sql_source, outfile, schema=schema)


    con.close()


if __name__ == "__main__":
    main()








