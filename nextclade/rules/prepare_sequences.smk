"""
This part of the workflow prepares sequences for constructing the reference tree
of the Nextclade dataset.

REQUIRED INPUTS:

    metadata    = data/metadata.tsv
    sequences   = data/sequences.fasta
    reference   = ../shared/reference.fasta

OUTPUTS:

    prepared_sequences = results/prepared_sequences.fasta

This part of the workflow usually includes the following steps:

    - augur index
    - augur filter
    - nextclade run
    - augur mask

See Nextclade's and Augur's usage docs for these commands for more details.
"""

# FIXME: Instead of directly copying from local ingest results, upload those to
# data.nextstrain.org and download in this workflow.
rule get_ingest_files:
    output:
        sequences = "data/sequences.fasta",
        metadata = "data/metadata.tsv"
    shell:
        r"""
        mkdir -p data
        cp ../ingest/results/* data
        """

rule filter:
    """
    FIXME: add comment
    """
    input:
        sequences = "data/sequences.fasta",
        metadata = "data/metadata.tsv",
        include = config["files"]["include"],
    output:
        sequences = "results/filtered.fasta"
    params:
        id_column = config["id_column"],
    log:
        "logs/filter.txt"
    shell:
        r"""
        augur filter \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --metadata-id-columns {params.id_column:q} \
            --include {input.include} \
            --output-sequences {output.sequences:q} \
        2>&1 | tee {log}
        """
