"""
This part of the workflow handles fetching sequences and metadata from Pathoplexus.
"""

###########################################################################
####################### 1. Fetch from Pathoplexus #########################
###########################################################################

rule download_ppx_seqs:
    output:
        sequences= "data/{species}/ppx_sequences.fasta",
    params:
        sequences_url=lambda w: config["ppx_fetch"][w.species]["seqs"],
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/{species}/download_ppx_seqs.txt"
    log:
        "logs/{species}/download_ppx_seqs.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        curl -fsSL {params.sequences_url:q} -o {output.sequences:q}
        """

rule download_ppx_meta:
    output:
        metadata= "data/{species}/ppx_metadata.csv"
    params:
        metadata_url=lambda w: config["ppx_fetch"][w.species]["meta"],
        fields = ",".join(config["ppx_metadata_fields"])
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/{species}/download_ppx_meta.txt"
    log:
        "logs/{species}/download_ppx_meta.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        curl -fsSL '{params.metadata_url}&fields={params.fields}' -o {output.metadata:q}
        """

rule format_ppx_ndjson:
    input:
        sequences="data/{species}/ppx_sequences.fasta",
        metadata="data/{species}/ppx_metadata.csv"
    output:
        ndjson="data/{species}/sequences.ndjson"
    benchmark:
        "benchmarks/{species}/format_ppx_ndjson.txt"
    log:
        "logs/{species}/format_ppx_ndjson.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate passthru \
            --metadata {input.metadata:q} \
            --fasta {input.sequences:q} \
            --seq-id-column accessionVersion \
            --seq-field sequence \
            --unmatched-reporting warn \
            --duplicate-reporting warn \
            > {output.ndjson:q}
        """

###########################################################################
########### 2. Fetch from Entrez (Zaire Ebolavirus only) ##################
###########################################################################


rule fetch_from_ncbi_entrez:
    params:
        term=config["entrez_search_term"],
    output:
        genbank="data/ebov/genbank.gb", # zaire ebolavirus only
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/ebov/fetch_from_ncbi_entrez.txt"
    log:
        "logs/ebov/fetch_from_ncbi_entrez.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        {workflow.basedir}/../shared/vendored/scripts/fetch-from-ncbi-entrez \
            --term {params.term:q} \
            --output {output.genbank:q}
        """


rule parse_genbank_to_ndjson:
    input:
        genbank="data/ebov/genbank.gb",
    output:
        ndjson="data/ebov/ncbi_entrez.ndjson",
    benchmark:
        "benchmarks/ebov/parse_genbank_to_ndjson.txt"
    log:
        "logs/ebov/parse_genbank_to_ndjson.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        bio json --lines {input.genbank:q} \
          | jq -c '
              {{
                accession: .record.accessions[0],
                strain:    .record.strain[0],
                isolate:   .record.isolate[0],
                host:      .record.host[0],
                title:     .record.references[0].title,
                note:      .record.note[0],
              }}
            ' > {output.ndjson:q}
        """

###########################################################################
############# 3. Fetch from INRB (Zaire Ebolavirus only) ##################
###########################################################################

rule fetch_inrb_nord_kivu_metadata:
    output: "data/ebov/inrb-drc-nord-kivu-metadata.tsv"
    shell:
        r"""
        curl -fsSL https://github.com/inrb-drc/ebola-nord-kivu/raw/ba9b9b48ba1e8db83486d653f3043d9671611594/data/metadata.tsv -o {output:q}
        """
