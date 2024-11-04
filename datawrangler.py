#! /usr/bin/python3
import duckdb
import argparse


def build_cli_parser():
    parser = argparse.ArgumentParser(description="CLI tool for demographic CSV merging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 'merge-demography-csvs' subcommand
    merge_parser = subparsers.add_parser(
        "merge-demography-csvs",
        help="Merge male, female, and both demographic CSV files",
    )
    merge_parser.add_argument(
        "-m", "--male", required=True, type=str, help="Path to the male CSV file"
    )
    merge_parser.add_argument(
        "-f", "--female", required=True, type=str, help="Path to the female CSV file"
    )
    merge_parser.add_argument(
        "-b", "--both", required=True, type=str, help="Path to the combined CSV file"
    )
    merge_parser.add_argument(
        "-o",
        "--output",
        default="demography.parquet",
        type=str,
        help="Path where the output is going to be saved (default: demography.parquet)",
    )

    return parser


def merge_demography_csvs(male_path, female_path, both_path, output_path):
    print(
        f"Merging male {male_path}, female {female_path} and {both_path} into {output_path}"
    )

    conn = duckdb.connect()

    conn.execute(f"""
    COPY (
        SELECT Code AS code,
               Year AS year,
               'm' AS type,
               "65+ years" AS years_65_plus,
               "25-64 years" AS years_25_64,
               "15-24 years" AS years_15_24,
               "5-14 years" AS years_5_14,
               "0-4 years" AS years_0_4
        FROM read_csv_auto('{male_path}')

        UNION

        SELECT Code AS code,
               Year AS year,
               'f' AS type,
               "65+ years" AS years_65_plus,
               "25-64 years" AS years_25_64,
               "15-24 years" AS years_15_24,
               "5-14 years" AS years_5_14,
               "0-4 years" AS years_0_4
        FROM read_csv_auto('{female_path}')

        UNION

        SELECT Code AS code,
               Year AS year,
               't' AS type,
               "65+ years" AS years_65_plus,
               "25-64 years" AS years_25_64,
               "15-24 years" AS years_15_24,
               "5-14 years" AS years_5_14,
               "0-4 years" AS years_0_4
        FROM read_csv_auto('{both_path}')

        WHERE code != ''
    )
    TO '{output_path}'
    (FORMAT 'parquet')
    """)

if __name__ == "__main__":
    parser = build_cli_parser()
    args = parser.parse_args()

    if args.command == "merge-demography-csvs":
        merge_demography_csvs(
            male_path=args.male,
            female_path=args.female,
            both_path=args.both,
            output_path=args.output,
        )
