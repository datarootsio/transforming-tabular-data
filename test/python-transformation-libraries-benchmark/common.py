from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent.parent

WATERBIRDS_PATH = (PROJECT_ROOT / "datasets" / "inbo-watervogels").resolve()
OCCURRENCE_TSV_PATH = WATERBIRDS_PATH / "occurrence.txt"
EVENT_TSV_PATH = WATERBIRDS_PATH / "event.txt"

GBIF_BACKBONE_PATH = (PROJECT_ROOT / "datasets" / "gbif-backbone" / "backbone").resolve()
TAXON_TSV_PATH = GBIF_BACKBONE_PATH / "Taxon.tsv"


def set_benchmark_meta(benchmark, benchmark_name: str, library_name: str, library: str):
    benchmark.extra_info["library_name"] = library_name
    benchmark.extra_info["library_version"] = library.__version__
    benchmark.extra_info["benchmark_name"] = benchmark_name
