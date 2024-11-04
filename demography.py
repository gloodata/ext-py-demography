from pydantic import BaseModel, Field

from glootil import (
    ContextActionReq,
    TagValueLoadReq,
    TagValueMatchReq,
    TagValueType,
    make_base_cli_parser,
    make_run_info,
    run,
)
from queries import (
    COUNTRY_COLS,
    COUNTRY_TABLE_COLS_INFO,
    DEM_COLS,
    DEM_DATA_COLS_AND_LABELS,
    DEM_MAX_YEAR,
    DEM_MIN_YEAR,
    DEM_TYPE_FEMALE,
    DEM_TYPE_MALE,
    all_countries,
    country_key_and_label_pairs,
    dem_by_code,
    dem_by_code_and_year,
    fert_by_year,
    fert_col_selector,
    get_country_by_fuzzy_name,
    init_db,
)

NS = "demography"


class LoadCountriesHandler(TagValueLoadReq):
    async def handler(self, info):
        return {"entries": country_key_and_label_pairs(info.conn)}


class MatchCountryHandler(TagValueMatchReq):
    async def handler(self, info):
        entry = get_country_by_fuzzy_name(info.conn, self.value)

        if entry:
            return {"entry": [entry.get("alpha_3"), entry.get("name")]}
        else:
            return {"entry": None}


class CountryContextActionHandler(ContextActionReq):
    async def handler(self, info):
        country = self.value.id if self.value else "GER"
        return {"args": {"country": country}}


class Country(TagValueType):
    ns = NS
    name = "Country"
    title = "Country"
    description = "Country Name"
    icon = "flag"

    ContextActionHandler = CountryContextActionHandler
    MatchHandler = MatchCountryHandler
    LoadHandler = LoadCountriesHandler


class CountryTable(BaseModel):
    class Info:
        title = "Country Table"

    async def handler(self, info):
        return {
            "info": {"type": "table", "cols": COUNTRY_TABLE_COLS_INFO},
            "data": {"cols": COUNTRY_COLS, "rows": all_countries(info.conn)},
        }


class DemographyByCountryAndYear(BaseModel):
    country: str = Country.to_field()
    year: int = Field(
        DEM_MAX_YEAR,
        title="Year",
        description=f"Year of demography data, if not provided use the latest year. Latest year is {DEM_MAX_YEAR}, first/earliest is {DEM_MIN_YEAR}",
    )

    class Info:
        title = "Demographic Pyramid by Country and Year"
        examples = [
            "Demography for Spain",
            "Italy's population pyramid in {DEM_MIN_YEAR}",
        ]

        context_actions = [Country.to_context_action()]
        default_args = {"country": "FRA", "year": DEM_MAX_YEAR}

    async def handler(self, info):
        male = (
            dem_by_code_and_year(info.conn, self.country, self.year, DEM_TYPE_MALE)
            or {}
        )
        female = (
            dem_by_code_and_year(info.conn, self.country, self.year, DEM_TYPE_FEMALE)
            or {}
        )

        items = []
        for name, label in reversed(DEM_DATA_COLS_AND_LABELS):
            start = male.get(name, 0)
            end = female.get(name, 0)

            items.append(dict(label=label, start=start, end=end))

        return {
            "info": {"type": "pop-pyramid"},
            "data": {"items": items},
        }


class WorldFertilityByYear(BaseModel):
    year: int = Field(
        DEM_MAX_YEAR,
        title="Year",
        description=f"Year of fertility data, if not provided use the latest year. Latest year is {DEM_MAX_YEAR}, first/earliest is {DEM_MIN_YEAR}",
    )

    class Info:
        title = "World Fertility by Year"
        ui_prefix = "World Fertility for"
        examples = [
            "World Fertility",
            "World Fertility in 1984",
        ]

        default_args = {"year": DEM_MAX_YEAR}

    async def handler(self, info):
        rows = fert_by_year(info.conn, self.year)
        sel = fert_col_selector("country", "fertility")

        areas = [
            dict(name=name, value=value) for row in rows for (name, value) in [sel(row)]
        ]

        return {
            "info": {
                "type": "areamap",
                "mapId": "world",
                "colorMap": "jet",
                "onClick": [
                    {
                        "action": "DTypeClick",
                        "dtypeNs": NS,
                        "dtypeName": Country.name,
                        "idField": "selected$$area",
                        "labelField": "selected$$area$label",
                    },
                ],
            },
            "data": {"areas": areas},
        }


class DemographyByCountryOverTime(BaseModel):
    country: str = Country.to_field()

    class Info:
        title = "Demography by Country over Time"
        ui_prefix = "Demography over Time for"
        examples = [
            "Demography timeserie for Spain",
            "Italy's demography over the years",
        ]
        default_args = {"country": "MEX"}
        context_actions = [Country.to_context_action()]

    async def handler(self, info):
        val_cols = []
        cols = [("year", "Year")]

        for name, label in DEM_DATA_COLS_AND_LABELS:
            val_cols.append(name)
            cols.append((name, label))

        col_indexes = [DEM_COLS.index(name) for (name, _) in cols]

        rows = dem_by_code(info.conn, self.country)
        chart_rows = []

        for row in rows:
            chart_row = [row[i] for i in col_indexes]
            chart_rows.append(chart_row)

        return {
            "info": {
                "type": "series",
                "title": "Demography by Country and Year",
                "yColTitle": "Inhabitants",
                "xCol": "year",
                "xAxisType": "time",
                "valCols": val_cols,
                "smooth": False,
                "cols": cols,
            },
            "data": {"rows": chart_rows},
        }


class CountryInfoBox(BaseModel):
    country: str = Country.to_field()

    class Info:
        title = "Country Information"
        ui_prefix = "Information for"
        examples = [
            "Information about Spain",
            "Italy's info",
        ]
        context_actions = [Country.to_context_action()]
        default_args = {"country": "BRA"}

    async def handler(self, info):
        row = get_country_by_fuzzy_name(info.conn, self.country) or {}

        return {
            "info": {"type": "infobox", "cols": COUNTRY_TABLE_COLS_INFO},
            "data": {"cols": list(row.keys()), "row": list(row.values())},
        }


def init(args):
    conn = init_db(
        args.dem_parquet_path, args.country_csv_path, args.fertility_csv_path
    )

    return make_run_info(
        NS,
        "World Demography",
        state=dict(conn=conn),
        tools=[
            CountryTable,
            CountryInfoBox,
            DemographyByCountryAndYear,
            DemographyByCountryOverTime,
            WorldFertilityByYear,
        ],
        tag_values=[Country],
    )


def make_cli_parser():
    parser = make_base_cli_parser()

    parser.add_argument(
        "--dem-parquet-path",
        default="./demography.parquet",
        help="Path to demography parquet file",
    )

    parser.add_argument(
        "--country-csv-path",
        default="./countries.csv",
        help="Path to countries csv file",
    )

    parser.add_argument(
        "--fertility-csv-path",
        default="./fertility-rate.csv",
        help="Path to fertility rate csv file",
    )

    return parser


if __name__ == "__main__":
    args_parser = make_cli_parser()
    args = args_parser.parse_args()

    run(
        init(args),
        host=args.host,
        port=args.port,
    )
