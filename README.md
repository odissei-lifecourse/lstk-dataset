
# A public, real-life dataset for sequencing people's life trajectories

This dataset accompanies the `lstk` package, to be published soon.

The data contain networks and career trajectories of CS researchers, extracted from Microsoft Academic. The data are processed with code in [mag_sample](https://github.com/f-hafner/mag_sample).


## Setup

You need access to a sqlite database built with `mag_sample`.

The `config.yaml` file specifies all queries, paths and file names.


### Installation

```bash

curl -LsSf https://astral.sh/uv/install.sh | sh

uv venv
source .venv/bin/activate
uv pip sync pyproject.toml

# add dependencies with
uv add pandas
```

**Notes**
- To open notebooks, run `uv run jupyter-lab --no-browser`
- I don't fully understand how uv interacts with pyenv, and there may be conflicts.

To run the pipeline, run

```bash
bash pipeline.sh
```


## Citation
If using these data, please cite TBD.
