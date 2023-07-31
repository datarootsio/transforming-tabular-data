# Transforming Tabular Data in Python 🛠️📈
_Comparing Pandas v. Polars v. PyArrow v. DuckDB 🐼🐻‍❄️🏹🦆_


<br>

This repository contains the benchmarking code backing the identically titled blog post: [Transforming Tabular Data in Python 🛠️📈](https://dataroots.io/research/contributions/transforming-tabular-data-in-python). This blog post compares four different frameworks based on performance and ease of use:
- [Pandas](https://pandas.pydata.org/) - the (as of recently) de-facto standard for dataframes in Python
- [Polars](https://www.pola.rs/) - a challenger to Pandas, backed by [Rust](https://www.rust-lang.org/) and [Apache Arrow](https://arrow.apache.org/)
- [PyArrow](https://arrow.apache.org/docs/python/) - the direct Python bindings for Apache Arrow
- [DuckDB](https://duckdb.org/) - an in-process Python analytical SQL database


Benchmarking is performed using [pytest-benchmark](https://github.com/ionelmc/pytest-benchmark/), which extends [pytest](https://pytest.org/) with a `benchmark` fixture that is used in each framework's respective test to measure the execution time of the transformation. The benchmarking code is located in the [`test/`](test/) directory, and the datasets used for benchmarking can be downloaded to the [`datasets/`](datasets/) directory using the [`download-datasets.sh`](download-datasets.sh) script (see [Setup](#setup) below).

For each of the four frameworks, two transformations are benchmarked. First a simpler one which loads, groups, and orders data from a single csv file. Second a more advanced one which joins three csv files, filters based on multiple conditions, and finally also groups and orders the data. 

## Setup

```bash
poetry install
sh download-datasets.sh
```


## Running the Benchmarks

```bash
# All benchmarks
pytest test/python-transformation-libraries-benchmark --benchmark-autosave --benchmark-min-rounds=8 --benchmark-min-time=0

# Only simple
pytest test/python-transformation-libraries-benchmark/test_simple.py --benchmark-autosave --benchmark-min-rounds=8 --benchmark-min-time=0

# Only advanced
pytest test/python-transformation-libraries-benchmark/test_advanced.py --benchmark-autosave --benchmark-min-rounds=8 --benchmark-min-time=0
```


## Datasets

The main datasets used for benchmarking is the ~260MB ["Watervogels" dataset](https://www.gbif.org/dataset/7f9eb622-c036-44c6-8be9-5793eaa1fa1e) from the [Flemish Institute for Nature and Forest (INBO)](https://inbo.be). This dataset...
> contains information on more than 94,000 sampling events (bird counts) with over 720,000 observations (and zero counts when there is no associated occurrence) for the period 1991-2016, covering 167 species in over 1,100 wetland sites.

<small>from the [dataset description](https://www.gbif.org/dataset/7f9eb622-c036-44c6-8be9-5793eaa1fa1e)</small>

Additionally, the ~5.73GB [Backbone Taxonomy dataset](https://www.gbif.org/dataset/50c9509d-22c7-4a22-a47d-8c48425ef4a7) by the [Global Biodiversity Information Facility (GBIF)](https://www.gbif.org/) is used to enrich the Watervogels dataset with taxonomic information. 


## Related work

[_Database-like ops benchmark_](https://h2oai.github.io/db-benchmark/)

## License

The source code in this repository is licensed under the [MIT License](License.txt).
