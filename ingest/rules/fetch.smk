"""
This part of the workflow handles fetching sequences and metadata from Pathoplexus.

REQUIRED INPUTS:

    None

OUTPUTS:

    ndjson = data/sequences.ndjson

"""

###########################################################################
####################### 1. Fetch from Pathoplexus #########################
###########################################################################

rule download_ppx_seqs:
    output:
        sequences= "data/ppx_sequences.fasta",
    params:
        sequences_url=config["ppx_fetch"]["seqs"],
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/download_ppx_seqs.txt"
    log:
        "logs/download_ppx_seqs.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        curl {params.sequences_url:q} -o {output.sequences:q}
        """

rule download_ppx_meta:
    output:
        metadata= "data/ppx_metadata.csv"
    params:
        metadata_url=config["ppx_fetch"]["meta"],
        fields = ",".join(config["ppx_metadata_fields"])
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/download_ppx_meta.txt"
    log:
        "logs/download_ppx_meta.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        curl '{params.metadata_url}&fields={params.fields}' -o {output.metadata:q}
        """

rule format_ppx_ndjson:
    input:
        sequences="data/ppx_sequences.fasta",
        metadata="data/ppx_metadata.csv"
    output:
        ndjson="data/sequences.ndjson"
    benchmark:
        "benchmarks/format_ppx_ndjson.txt"
    log:
        "logs/format_ppx_ndjson.txt"
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
########################## 2. Fetch from Entrez ###########################
###########################################################################


rule fetch_from_ncbi_entrez:
    params:
        term=config["entrez_search_term"],
    output:
        genbank="data/genbank.gb",
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/fetch_from_ncbi_entrez.txt"
    log:
        "logs/fetch_from_ncbi_entrez.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        {workflow.basedir}/../shared/vendored/scripts/fetch-from-ncbi-entrez \
            --term {params.term:q} \
            --output {output.genbank:q}
        """


# If you are using additional Entrez data, add additional rules here for parsing
# the Entrez results and merging with the ncbi_dataset_report.tsv
# Remember to edit the `ncbi_dataset_tsv` input below to use the new merged TSV.
