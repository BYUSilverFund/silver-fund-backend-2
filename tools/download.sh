#!/bin/bash

funds=("undergrad" "grad" "quant" "brigham_capital")
tables=("positions" "dividends" "nav" "delta_nav" "trades")
for fund in "${funds[@]}"; do
    for table in "${tables[@]}"; do
        python -m download_csv "$fund" "$table"
    done
done
