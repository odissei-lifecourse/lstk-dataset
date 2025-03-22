import pathlib
import re
from pathlib import Path
import pyarrow.parquet as pq
import yaml
from omegaconf import OmegaConf
from omegaconf.dictconfig import DictConfig


def get_metadata(src_path: pathlib.Path, docs_path: pathlib.Path, config: DictConfig) -> None:
    """Iterate over files in `src_path` and store metadata in `meta_path`."""
    aggregate_suffix = config.aggregate.suffix
    names_replace = config.export.replace_suffix
    extension = config.output.extension
    schema_dict = {}
    for file in src_path.glob("*.parquet"):
        skip = [f"{x}{extension}" in str(file) for x in names_replace]
        if any(skip):
            continue

        table_schema = pq.read_schema(file)

        file_schema = {name: str(field.type) for name, field in zip(table_schema.names, table_schema, strict=False)}

        file_stem = re.sub(aggregate_suffix, "", file.stem)
        schema_dict[file_stem] = file_schema

    outfile = docs_path / "schema.yaml"
    outfile.write_text(yaml.dump(schema_dict, default_flow_style=False))


def main():
    """Store parquet metadata in yaml files."""
    config = OmegaConf.load("config.yaml")
    src_path = Path(config.output.path)
    docs_path = Path(config.export.docs_path)
    docs_path.mkdir(parents=True, exist_ok=True)
    get_metadata(src_path, docs_path, config)


if __name__ == "__main__":
    main()
