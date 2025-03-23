import pathlib
import re
import shutil
from pathlib import Path
import pyarrow.parquet as pq
import yaml
from omegaconf import OmegaConf
from omegaconf.dictconfig import DictConfig


def get_metadata(src_path: pathlib.Path, docs_path: pathlib.Path, config: DictConfig) -> None:
    """Iterate over files in `src_path`, store metadata in `meta_path` and store final files."""
    names_replace = config.export.replace_suffix
    extension = config.output.extension
    schema_dict = {}
    final_file_mapping = {}
    for file in src_path.glob("*.parquet"):
        skip = [f"{x}{extension}" in str(file) for x in names_replace]
        if any(skip):
            continue

        table_schema = pq.read_schema(file)

        file_schema = {name: str(field.type) for name, field in zip(table_schema.names, table_schema, strict=False)}

        file_stem = re.sub("|".join(config.output.suffix.values()), "", file.stem)
        schema_dict[file_stem] = file_schema
        final_file_mapping[file_stem] = file.stem

    outfile = docs_path / "schema.yaml"
    outfile.write_text(yaml.dump(schema_dict, default_flow_style=False))

    final_path = Path(*[config.output.path, config.export.final_data])
    final_path.mkdir(parents=True, exist_ok=True)
    for target, src in final_file_mapping.items():
        suffix = config.output.extension
        src_file = Path(*[config.output.path, src]).with_suffix(suffix)
        target_file = Path(*[config.output.path, config.export.final_data, target]).with_suffix(suffix)
        shutil.copy(src_file.with_suffix(suffix), target_file.with_suffix(suffix))


def main():
    """Store parquet metadata in yaml files."""
    config = OmegaConf.load("config.yaml")
    src_path = Path(config.output.path)
    docs_path = Path(config.export.docs_path)
    docs_path.mkdir(parents=True, exist_ok=True)
    get_metadata(src_path, docs_path, config)


if __name__ == "__main__":
    main()
