import csv
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import numpy as np
import pyarrow as pa
import pyarrow.csv
import pyarrow.compute as pc

from common import OCCURRENCE_TSV_PATH, EVENT_TSV_PATH


GBIF_BACKBONE_PATH = Path("/Users/pieter/Downloads/backbone/")
TAXON_TSV_PATH = GBIF_BACKBONE_PATH / "Taxon.tsv"

POI_LATITUDE = 50.87
POI_LONGITUDE = 4.70
POI_MAX_DISTANCE_DEGREES = 0.1

EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI = 15
EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI = 26


def test_join_duckdb(benchmark):
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
    print(genera)
    # Some birds are not in the taxon table (1 of the 41 in this dataset to be exact)
    # treat these as non-anatidae with bool(is_anatidae)
    number_of_anatidae = sum(bool(is_anatidae) for _, _, is_anatidae in genera)
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


def test_join_polars(benchmark):
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
                aves_taxon_canonicalName=pl.col("canonicalName").str.to_lowercase(),
                aves_taxon_family=pl.col("family").str.to_lowercase(),
                aves_taxon_genus=pl.col("genus").str.to_lowercase(),
            )
            .sort(pl.col("aves_taxon_canonicalName"))
        )
        occurrence_df = (
            pl.read_csv(OCCURRENCE_TSV_PATH, separator="\t", quote_char=None)
            .select(
                occurrence_scientificName=pl.col("scientificName").str.to_lowercase(),
                occurrence_eventID=pl.col("eventID"),
            )
        )
        return (
            event_distance_df
            .join(occurrence_df, left_on="event_eventID", right_on="occurrence_eventID")
            .join(aves_taxon_df, left_on="occurrence_scientificName", right_on="aves_taxon_canonicalName", how="left")
            .groupby("aves_taxon_genus")
            .agg([
                pl.col("distance").min().alias("distance"),
                pl.col("aves_taxon_family").first().eq("anatidae").alias("is_anatidae"),
            ])
            .sort(pl.col("distance"))
            .to_dict()
        )
    
    
    genera = benchmark(get_genera)
    number_of_anatidae = sum(bool(isAnatidae) for isAnatidae in genera["is_anatidae"])
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera["is_anatidae"]) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


def test_join_pandas(benchmark):
    def get_genera():
        event_df = (
            pd.read_csv(EVENT_TSV_PATH, sep="\t", quoting=csv.QUOTE_NONE)
            .assign(distance=lambda df: np.sqrt((df.decimalLatitude - POI_LATITUDE)**2 + (df.decimalLongitude - POI_LONGITUDE)**2))
            .loc[lambda df: df.distance < POI_MAX_DISTANCE_DEGREES]
            .loc[:, ["id", "distance"]]
            .rename(columns={"id": "event_eventID"})
        )
        aves_taxon_df = (
            pd.read_csv(TAXON_TSV_PATH, sep="\t", quoting=csv.QUOTE_NONE)
            .loc[lambda df: df["class"] == "Aves"]
            .loc[:, ["canonicalName", "family", "genus"]]
            .rename(columns={"canonicalName": "aves_taxon_canonicalName", "family": "aves_taxon_family", "genus": "aves_taxon_genus"})
            .sort_values("aves_taxon_canonicalName")
        )
        occurrence_df = (
            pd.read_csv(OCCURRENCE_TSV_PATH, sep="\t", quoting=csv.QUOTE_NONE)
            .loc[:, ["scientificName", "eventID"]]
            .rename(columns={"scientificName": "occurrence_scientificName", "eventID": "occurrence_eventID"})
        )
        return (
            event_df
            .merge(occurrence_df, left_on="event_eventID", right_on="occurrence_eventID")
            .merge(aves_taxon_df, left_on="occurrence_scientificName", right_on="aves_taxon_canonicalName", how="left")
            .groupby("aves_taxon_genus")
            .agg({"distance": "min", "is_anatidae": lambda s: s.iloc[0] == "anatidae"})
            .sort_values("distance")
            .to_dict()
        )
    
    genera = benchmark(get_genera)
    number_of_anatidae = sum(bool(isAnatidae) for isAnatidae in genera["is_anatidae"])
    assert number_of_anatidae == EXPECTED_NUMBER_OF_ANATIDAE_AROUND_POI
    assert len(genera["aves_taxon_family"]) - number_of_anatidae == EXPECTED_NUMBER_OF_OTHER_BIRDS_AROUND_POI


PYARROW_READ_CSV_KWARGS = dict(
    read_options=pyarrow.csv.ReadOptions(use_threads=False),
    parse_options=pyarrow.csv.ParseOptions(delimiter="\t", quote_char=False),
)

def test_join_pyarrow(benchmark):
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
        aves_taxon_table = pa.csv.read_csv(TAXON_TSV_PATH, **PYARROW_READ_CSV_KWARGS)
        # sample = aves_taxon_table.to_pandas().iloc[90000:90010]
        # print(sample[["taxonID", "canonicalName", "family", "genus"]])
        aves_taxon_table = (
            aves_taxon_table
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
        # print(aves_taxon_table.to_pandas().iloc[10000:10010])
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
