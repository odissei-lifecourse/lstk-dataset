


- Install uv from website
- how to recreate?
- to open notebook, run `uv run jupyter-lab --no-browser`


Once `uv` is installed: using through the [pip interface](https://docs.astral.sh/uv/pip/)

```bash
uv venv
source .venv/bin/activate
uv pip sync pyproject.toml

# add dependencies with
uv add pandas
```



I don't fully understand how uv interacts with pyenv, and there may be conflicts.
