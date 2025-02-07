import pathlib
from pathlib import Path
import pyarrow.parquet as pq
import yaml
from omegaconf import OmegaConf


def get_metadata(src_path: pathlib.Path, meta_path: pathlib.Path, exclude: list | None = None) -> None:
    """Iterate over files in `src_path` and store metadata in `meta_path`."""
    for file in src_path.glob("*.parquet"):
        skip = [x in str(file) for x in exclude]
        if any(skip):
            continue

        table_schema = pq.read_schema(file)

        schema_dict = {name: str(field.type) for name, field in zip(table_schema.names, table_schema, strict=False)}

        outfile = meta_path / Path(file.stem).with_suffix(".yaml")

        outfile.write_text(yaml.dump(schema_dict, default_flow_style=False))


def main():
    """Store parquet metadata in yaml files."""
    config = OmegaConf.load("config.yaml")
    src_path = Path(config.output.path)
    meta_path = Path(config.export.meta_path)
    meta_path.mkdir(parents=True, exist_ok=True)
    get_metadata(src_path, meta_path, exclude=config.export.exclude)


if __name__ == "__main__":
    main()
