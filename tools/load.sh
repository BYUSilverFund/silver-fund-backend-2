#!/bin/bash


tables=("positions") # "dividends" "nav" "delta_nav" "trades")
for table in "${tables[@]}"; do
    for filepath in "$table"/*.csv; do
        python -m load_csv "$filepath" "$table"
    done
done
