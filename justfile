PORT := "8889"

run:
    uv run demography.py --port {{PORT}}

demography-to-parquet:
    rm -f ./demography.parquet
    uv run datawrangler.py merge-demography-csvs -m ./demography-male.csv -f ./demography-female.csv -b ./demography-both.csv -o ./demography.parquet
