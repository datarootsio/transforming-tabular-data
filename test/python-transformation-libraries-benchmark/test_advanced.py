import csv
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import numpy as np
import pyarrow as pa
import pyarrow.csv
import pyarrow.compute as pc
import pytest

from common import OCCURRENCE_TSV_PATH, EVENT_TSV_PATH


GBIF_BACKBONE_PATH = Path("/Users/pieter/Downloads/backbone/")
TAXON_TSV_PATH = GBIF_BACKBONE_PATH / "Taxon.tsv"

POI_LATITUDE = 50.87
POI_LONGITUDE = 4.70
POI_MAX_DISTANCE_DEGREES = 0.1

EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI = 15
EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI = 26


@pytest.mark.library_name("DuckDB")
def test_duckdb(benchmark):
    conn = duckdb.connect()
    def get_genera():
        return conn.execute(
            f"""
                WITH event_with_distance AS (
                    SELECT id, locality, sqrt(pow(decimalLatitude - $poi_latitude, 2) + pow(decimalLongitude - $poi_longitude, 2)) AS distance
                    FROM read_csv($event_csv, AUTO_DETECT=True, header=True, delim='\t', quote='')
                ),
                aves_taxon AS (
                    SELECT lower(canonicalName) AS canonicalName, lower(family) AS family, lower(genus) AS genus
                    FROM read_csv($taxon_tsv_path, AUTO_DETECT=True, header=True, delim='\t', quote='')
                    WHERE class = 'Aves'
                    ORDER BY canonicalName ASC
                ),
                occurrence AS (
                    SELECT lower(scientificName) as scientificName, eventID
                    FROM read_csv($occurrence_csv, AUTO_DETECT=True, header=True, delim='\t', quote='')
                )
                SELECT aves_taxon.genus, MIN(event_with_distance.distance) AS distance, ANY_VALUE(aves_taxon.family) == 'anatidae' AS is_anatidae
                FROM event_with_distance
                INNER JOIN occurrence
                ON event_with_distance.id = occurrence.eventID
                LEFT JOIN aves_taxon
                ON aves_taxon.canonicalName = occurrence.scientificName
                WHERE distance < $poi_max_distance_degrees
                GROUP BY aves_taxon.genus
                ORDER BY distance ASC
            """,
            {
                "event_csv": str(EVENT_TSV_PATH),
                "occurrence_csv": str(OCCURRENCE_TSV_PATH),
                "taxon_tsv_path": str(TAXON_TSV_PATH),
                "poi_latitude": POI_LATITUDE,
                "poi_longitude": POI_LONGITUDE,
                "poi_max_distance_degrees": POI_MAX_DISTANCE_DEGREES,
            }
        ).fetchall()
    
    genera = benchmark(get_genera)
    # Some birds are not in the taxon table (1 of the 41 in this dataset to be exact)
    # treat these as non-anatidae with bool(is_anatidae)
    number_of_anatidae = sum(bool(is_anatidae) for _, _, is_anatidae in genera)
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


@pytest.mark.library_name("Polars")
def test_polars(benchmark):
    def get_genera():
        event_distance_df = (
            pl.read_csv(EVENT_TSV_PATH, separator="\t", quote_char=None)
            .with_columns([
                ((pl.col("decimalLatitude") - POI_LATITUDE).pow(2) + (pl.col("decimalLongitude") - POI_LONGITUDE).pow(2)).sqrt().alias("distance"),
            ])
            .filter(pl.col("distance") < POI_MAX_DISTANCE_DEGREES)
            .select(
                event_eventID=pl.col("id"),
                distance=pl.col("distance"),
            )
        )
        aves_taxon_df = (
            pl.read_csv(TAXON_TSV_PATH, separator="\t", quote_char=None)
            .filter(pl.col("class") == "Aves")
            .select(
                canonicalName=pl.col("canonicalName").str.to_lowercase(),
                family=pl.col("family").str.to_lowercase(),
                genus=pl.col("genus").str.to_lowercase(),
            )
            .sort(pl.col("canonicalName"))
        )
        occurrence_df = (
            pl.read_csv(OCCURRENCE_TSV_PATH, separator="\t", quote_char=None)
            .select(
                scientificName=pl.col("scientificName").str.to_lowercase(),
                occurrence_eventID=pl.col("eventID"),
            )
        )
        return (
            event_distance_df
            .join(occurrence_df, left_on="event_eventID", right_on="occurrence_eventID")
            .join(aves_taxon_df, left_on="scientificName", right_on="canonicalName", how="left")
            .groupby("genus")
            .agg([
                pl.col("distance").min().alias("distance"),
                pl.col("scientificName").first().alias("sample_scientific_name"),
                pl.col("family").first().eq("anatidae").alias("is_anatidae"),
            ])
            .sort(pl.col("distance"))
            .to_dict()
        )
    
    
    genera = benchmark(get_genera)
    number_of_anatidae = sum(bool(isAnatidae) for isAnatidae in genera["is_anatidae"])
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera["is_anatidae"]) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


@pytest.mark.library_name("Pandas")
def test_pandas(benchmark):
    def get_genera():
        event_df = pd.read_csv(EVENT_TSV_PATH, sep="\t", quoting=csv.QUOTE_NONE)
        event_df["distance"] = np.sqrt((event_df.decimalLatitude - POI_LATITUDE)**2 + (event_df.decimalLongitude - POI_LONGITUDE)**2)
        event_df = event_df[["id", "distance"]]
        event_df = event_df[event_df.distance < POI_MAX_DISTANCE_DEGREES]

        aves_taxon_df = pd.read_csv(TAXON_TSV_PATH, sep="\t", quoting=csv.QUOTE_NONE)
        aves_taxon_df = aves_taxon_df[aves_taxon_df["class"] == "Aves"]
        aves_taxon_df = aves_taxon_df[["canonicalName", "family", "genus"]]
        aves_taxon_df["canonicalName"] = aves_taxon_df.canonicalName.str.lower()
        aves_taxon_df["family"] = aves_taxon_df.family.str.lower()
        aves_taxon_df["genus"] = aves_taxon_df.genus.str.lower()
        aves_taxon_df = aves_taxon_df.sort_values("canonicalName")

        occurrence_df = pd.read_csv(OCCURRENCE_TSV_PATH, sep="\t", quoting=csv.QUOTE_NONE)
        occurrence_df = occurrence_df[["scientificName", "eventID"]]
        occurrence_df["scientificName"] = occurrence_df.scientificName.str.lower()

        genera = (
            event_df
            .merge(occurrence_df, left_on="id", right_on="eventID")
            .merge(aves_taxon_df, left_on="scientificName", right_on="canonicalName", how="left")
            .groupby("genus", dropna=False)
            .agg({"distance": "min", "family": lambda s: s.iloc[0], "scientificName": lambda s: s.iloc[0]})
        )
        genera["is_anatidae"] = genera.family == "anatidae"
        genera = genera.sort_values("distance")
        return genera.to_dict()
    
    genera = benchmark(get_genera)
    number_of_anatidae = sum(bool(isAnatidae) for isAnatidae in genera["is_anatidae"].values())
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera["is_anatidae"]) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


PYARROW_READ_CSV_KWARGS = dict(
    read_options=pyarrow.csv.ReadOptions(use_threads=False),
    parse_options=pyarrow.csv.ParseOptions(delimiter="\t", quote_char=False),
)


@pytest.mark.library_name("PyArrow")
def test_pyarrow(benchmark):
    def get_genera():
        event_distance_table = pa.csv.read_csv(EVENT_TSV_PATH, convert_options=pa.csv.ConvertOptions(column_types={"decimalLatitude": pa.float64(), "decimalLongitude": pa.float64()}), **PYARROW_READ_CSV_KWARGS)
        event_distance_table = (
            event_distance_table
            .append_column("distance", pc.sqrt(pc.add(pc.power(pc.subtract(event_distance_table["decimalLatitude"], POI_LATITUDE), 2), pc.power(pc.subtract(event_distance_table["decimalLongitude"], POI_LONGITUDE), 2))))
        )
        event_distance_table = (
            event_distance_table
            .filter(pc.less(event_distance_table["distance"], POI_MAX_DISTANCE_DEGREES))
            .select(["id", "distance"])
            .rename_columns(["id", "distance"])
        )
        aves_taxon_table = (
            pa.csv.read_csv(TAXON_TSV_PATH, **PYARROW_READ_CSV_KWARGS)
            .filter(pc.equal(pc.field("class"), "Aves"))
            .select(["canonicalName", "family", "genus"])
        )
        aves_taxon_table = (
            aves_taxon_table
            .set_column(0, "canonicalName", pc.utf8_lower(aves_taxon_table["canonicalName"]))
            .set_column(1, "family", pc.utf8_lower(aves_taxon_table["family"]))
            .set_column(2, "genus", pc.utf8_lower(aves_taxon_table["genus"]))
            .sort_by([("canonicalName", "ascending")])
        )
        occurrence_table = pa.csv.read_csv(OCCURRENCE_TSV_PATH, **PYARROW_READ_CSV_KWARGS)
        occurrence_table = (
            occurrence_table
            .select(["scientificName", "eventID"])
            .set_column(0, "scientificName", pc.utf8_lower(occurrence_table["scientificName"]))
        )
        genera = (
            event_distance_table
            .join(occurrence_table, "id", "eventID", join_type="inner")
            .join(aves_taxon_table, "scientificName", "canonicalName", join_type="left outer")
            .group_by("genus")
            .aggregate([
                ("distance", "min"),
                ("family", "one"),
            ])
            .rename_columns(["genus", "distance", "family"])
        )
        return (
            genera
            .append_column("is_anatidae", pc.equal(genera["family"], "anatidae"))
            .to_pydict()
        )
    
    genera = benchmark(get_genera)
    number_of_anatidae = sum(bool(isAnatidae) for isAnatidae in genera["is_anatidae"])
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera["is_anatidae"]) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


if __name__ == "__main__":
    def mock_benchmark(func):
        return func()

    mock_benchmark.extra_info = {}
    # test_polars(mock_benchmark)
    # test_pandas(mock_benchmark)
