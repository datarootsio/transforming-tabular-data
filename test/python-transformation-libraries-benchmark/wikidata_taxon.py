"""Currently unused, the GBIF backbone taxonomy is used instead."""
from functools import cache
import SPARQLWrapper as sparql
import pyarrow as pa


USER_AGENT = "duckdb-benchmark-blog-post"
WIKIDATA_P_TAXON_NAME = "P225"
WIKIDATA_P_PARENT_TAXON = "P171"
WIKIDATA_Q_ANATIDAE = "Q7556"


@cache
def fetch_wikidata_taxon_children(taxon_item_id: str = WIKIDATA_Q_ANATIDAE) -> pa.Table:
    """Fetches all child taxa of the given taxon item id from Wikidata.
    
    Works for any rank of taxon, e.g. species, genus, family, etc.
    """
    call = sparql.SPARQLWrapper("https://query.wikidata.org/sparql", agent=USER_AGENT)
    call.setQuery(f"""
        SELECT ?item ?itemLabel ?taxonName 
        WHERE
        {{
            ?item  wdt:{WIKIDATA_P_PARENT_TAXON}+ wd:{taxon_item_id} .
            ?item wdt:{WIKIDATA_P_TAXON_NAME} ?taxonName .
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
    """)
    call.setReturnFormat(sparql.JSON)
    results = call.queryAndConvert()["results"]["bindings"]
    return [
        { "id": row["item"]["value"], "taxonName": row["taxonName"]["value"].lower(), "label": row["itemLabel"]["value"] }
        for row in results
    ]
