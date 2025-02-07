import pathlib
import re
from pathlib import Path
import pyarrow.parquet as pq
import yaml
from omegaconf import OmegaConf
from omegaconf.dictconfig import DictConfig


def get_metadata(src_path: pathlib.Path, meta_path: pathlib.Path, config: DictConfig) -> None:
    """Iterate over files in `src_path` and store metadata in `meta_path`."""
    aggregate_suffix = config.aggregate.suffix
    names_replace = config.export.replace_suffix
    extension = config.output.extension
    for file in src_path.glob("*.parquet"):
        skip = [f"{x}{extension}" in str(file) for x in names_replace]
        if any(skip):
            continue

        table_schema = pq.read_schema(file)

        schema_dict = {name: str(field.type) for name, field in zip(table_schema.names, table_schema, strict=False)}

        file_stem = re.sub(aggregate_suffix, "", file.stem)
        outfile = meta_path / Path(file_stem).with_suffix(".yaml")

        outfile.write_text(yaml.dump(schema_dict, default_flow_style=False))


def main():
    """Store parquet metadata in yaml files."""
    config = OmegaConf.load("config.yaml")
    src_path = Path(config.output.path)
    meta_path = Path(config.export.meta_path)
    meta_path.mkdir(parents=True, exist_ok=True)
    get_metadata(src_path, meta_path, config)


if __name__ == "__main__":
    main()
