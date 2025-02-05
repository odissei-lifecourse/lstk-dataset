import sqlite3 as sqlite
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from pathlib import Path
from omegaconf import OmegaConf



def db_to_file(con, sql_source, destination, chunksize=10_000, schema=None, view_prefix="v_"):
    pqwriter = None
    if schema is not None:
        schema = pa.schema(schema)
    for i, chunk in tqdm(enumerate(pd.read_sql(f"select * from {view_prefix}{sql_source}", con, chunksize=chunksize)), 
                      desc=f"Processing {sql_source}"
                     ):
        table = pa.Table.from_pandas(chunk, schema=schema)
        
        if i == 0:
            pqwriter = pq.ParquetWriter(destination, table.schema)  
            
        pqwriter.write_table(table)
        if i > 5:
            break
    
    # close the parquet writer
    if pqwriter:
        pqwriter.close()



def main():
    config = OmegaConf.load('config.yaml')
    con = sqlite.connect(config.db.path)
    for query in config.db.views.values():
        con.execute(query)

    schema_dict = config.output.schema
    for sql_source in config.db.views.keys():
        print(sql_source)
        outfile = Path(config.output.path) / sql_source
        outfile = outfile.with_suffix(config.output.extension)
        schema = None
        if sql_source in schema_dict:
            schema = [(k,v) for k,v in schema_dict[sql_source].items()]
        db_to_file(con, sql_source, outfile, schema=schema)


    con.close()


if __name__ == "__main__":
    main()








