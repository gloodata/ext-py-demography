# Gloodata Demography Python Extension

A sample python extension for [Gloodata](https://gloodata.com/) using:

- [uv](https://docs.astral.sh/uv/)
- [duckdb](https://duckdb.org/docs/api/python/overview.html)
- [fastapi](https://fastapi.tiangolo.com/)
- [pydantic](https://docs.pydantic.dev/latest/)
- [uvicorn](https://www.uvicorn.org/)

## Data

`countries.csv` is from [github.com/lukes/ISO-3166-Countries-with-Regional-Codes](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/blob/master/all/all.csv).

The following files are from [Our World in Data: Population & Demography Data Explorer](https://ourworldindata.org/explorers/population-and-demography):

- `demography-both.csv`
- `demography-male.csv`
- `demography-female.csv`
- `fertility-rate.csv`

`demography.parquet` is generated using `datawrangler.py` for the complete command check the `demography-to-parquet` target in `justfile` or run `uv run datawrangler.py merge-demography-csvs -h`.
