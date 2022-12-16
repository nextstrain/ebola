"""
This part of the workflow handles fetching sequences from various sources.
Uses `config.sources` to determine which sequences to include in final output.

Currently only fetches sequences from GenBank, but other sources can be
defined in the config. If adding other sources, add a new rule upstream
of rule `fetch_all_sequences` to create the file `data/{source}.ndjson` or the
file must exist as a static file in the repo.

Produces final output as

    sequences_ndjson = "data/sequences.ndjson"

"""

rule fetch_from_genbank:
    output:
        genbank_ndjson=temp("data/genbank.ndjson"),
    params:
        serotype_tax_id='186536', # Returns 3530 records, check if we need a more specific Taxon ID
        csv_to_ndjson_url="https://raw.githubusercontent.com/nextstrain/monkeypox/644d07ebe3fa5ded64d27d0964064fb722797c5d/ingest/bin/csv-to-ndjson",
        fetch_from_genbank_url="https://raw.githubusercontent.com/nextstrain/dengue/ca659008bfbe4b3f799e11ecd106a0b95977fe93/ingest/bin/fetch-from-genbank",
        genbank_url_url="https://raw.githubusercontent.com/nextstrain/dengue/ca659008bfbe4b3f799e11ecd106a0b95977fe93/ingest/bin/genbank-url", # Update if dengue merged
    shell:
        """
        if [[ ! -d bin ]]; then
          mkdir bin
        fi
        cd bin
        [[ -f csv-to-ndjson ]] || wget {params.csv_to_ndjson_url}
        [[ -f genbank-url ]] || wget {params.genbank_url_url}
        [[ -f fetch-from-genbank ]] || wget {params.fetch_from_genbank_url}
        chmod 755 *
        cd ..
        ./bin/fetch-from-genbank {params.serotype_tax_id} > {output.genbank_ndjson}
        """


def _get_all_sources(wildcards):
    return [f"data/{source}.ndjson" for source in config["sources"]]


rule fetch_all_sequences:
    input:
        all_sources=_get_all_sources,
    output:
        sequences_ndjson=temp("data/sequences.ndjson"),
    shell:
        """
        cat {input.all_sources} > {output.sequences_ndjson}
        """
