import duckdb
import pandas as pd
import polars as pl
import pyarrow.csv
import pytest

from common import OCCURRENCE_TSV_PATH, set_benchmark_meta


BENCHMARK_NAME = "Simple"

EXPECTED_NUMBER_OF_SPECIES = 186
EXPECTED_TOTAL_NUMBER_OF_BIRDS = 40920379


def test_duckdb(benchmark):
    set_benchmark_meta(benchmark, BENCHMARK_NAME, "DuckDB", duckdb)

    conn = duckdb.connect()
    def get_bird_counts():
        return conn.execute(f"""
            SELECT scientificName, SUM(individualCount) as birds_count
            FROM read_csv_auto($occurrence_tsv_path)
            GROUP BY scientificName
            ORDER BY birds_count DESC
        """, {"occurrence_tsv_path": str(OCCURRENCE_TSV_PATH)}).fetchall()
    
    bird_counts = benchmark(get_bird_counts)
    assert len(bird_counts) == EXPECTED_NUMBER_OF_SPECIES
    assert sum(count for _, count in bird_counts) == EXPECTED_TOTAL_NUMBER_OF_BIRDS


def test_polars(benchmark):
    set_benchmark_meta(benchmark, BENCHMARK_NAME, "Polars", pl)

    def get_bird_counts():
        df = pl.read_csv(OCCURRENCE_TSV_PATH, separator="\t", quote_char=None)
        return (
            df[["scientificName", "individualCount"]]
            .groupby("scientificName")
            .sum()
            .sort("individualCount", descending=True)
            .to_dict()
        )
    
    bird_counts = benchmark(get_bird_counts)
    assert len(bird_counts["individualCount"]) == EXPECTED_NUMBER_OF_SPECIES
    assert sum(bird_counts["individualCount"]) == EXPECTED_TOTAL_NUMBER_OF_BIRDS


def test_pandas(benchmark):
    set_benchmark_meta(benchmark, BENCHMARK_NAME, "Pandas", pd)

    def get_bird_counts():
        df = pd.read_csv(OCCURRENCE_TSV_PATH, sep="\t")
        return (
            df[["scientificName", "individualCount"]]
            .groupby("scientificName")
            .sum()
            .sort_values("individualCount", ascending=False)
            .to_dict()
        )
    
    bird_counts = benchmark(get_bird_counts)
    assert len(bird_counts["individualCount"]) == EXPECTED_NUMBER_OF_SPECIES
    assert sum(bird_counts["individualCount"].values()) == EXPECTED_TOTAL_NUMBER_OF_BIRDS


def test_pyarrow(benchmark):
    set_benchmark_meta(benchmark, BENCHMARK_NAME, "PyArrow", pyarrow)

    def get_bird_counts():
        table = pyarrow.csv.read_csv(
            OCCURRENCE_TSV_PATH,
            parse_options=pyarrow.csv.ParseOptions(delimiter="\t"),
        )
        return (
            table.select(["scientificName", "individualCount"])
            .group_by("scientificName")
            .aggregate([("individualCount", "sum")])
            .sort_by([("individualCount_sum", "descending")])
            .to_pydict()
        )
    
    bird_counts = benchmark(get_bird_counts)
    assert len(bird_counts["individualCount_sum"]) == EXPECTED_NUMBER_OF_SPECIES
    assert sum(bird_counts["individualCount_sum"]) == EXPECTED_TOTAL_NUMBER_OF_BIRDS


if __name__ == "__main__":
    def mock_benchmark(func):
        return func()

    mock_benchmark.extra_info = {}
    test_pandas(mock_benchmark)
