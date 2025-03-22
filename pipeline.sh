#!/bin/bash

source .venv/bin/activate
rm -rf data/*
mkdir logs
uv run src/make_dataset.py &> logs/make_dataset.log
uv run src/aggregate.py
uv run src/get_metadata.py
