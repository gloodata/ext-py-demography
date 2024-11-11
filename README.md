# Gloodata Demography Python Extension

A sample python extension for [Gloodata](https://gloodata.com/) that display fertility and demographic data from [Our World in Data: Population & Demography Data Explorer](https://ourworldindata.org/explorers/population-and-demography).

## Run

```sh
uv run demography.py --port 8889
```

If you have [just](https://github.com/casey/just) installed the following does the same:

```sh
just run
```

## Files

The extension code is in `demography.py`, the query code is in `queries.py`.

`glootil.py` will become a library at some point, you can read it if you are curious.

`datawrangler.py` is a script to merge `demography-*.csv` files into `demography.parquet`, run `uv run datawrangler.py -h` to see the options or check `justfile` to see how it's used, you don't need to run it since the parquet file is already in the repository.

## Dependencies

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
