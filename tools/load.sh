#!/bin/bash

DIRECTORY="files"

for filepath in "$DIRECTORY"/*.csv; do
    python -m load_csv "$filepath"
done
