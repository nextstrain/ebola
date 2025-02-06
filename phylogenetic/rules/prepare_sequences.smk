"""
This part of the workflow prepares sequences for constructing the phylogenetic tree.

REQUIRED INPUTS:

    metadata    = data/metadata.tsv
    sequences   = data/sequences.fasta
    reference   = ../shared/reference.fasta

OUTPUTS:

    prepared_sequences = results/prepared_sequences.fasta

This part of the workflow usually includes the following steps:

    - augur index
    - augur filter
    - augur align
    - augur mask

See Augur's usage docs for these commands for more details.
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
    Filtering to
      - {params.sequences_per_group} sequence(s) per {params.group_by!s}
      - from {params.min_date} onwards
      - excluding strains in {input.exclude}
    """
    input:
        sequences = "data/sequences.fasta",
        metadata = "data/metadata.tsv",
        exclude = config["filter"]["exclude"],
        include = config["filter"]["include"],
    output:
        sequences = "results/filtered.fasta"
    params:
        id_column = config["id_column"],
        min_date = config["filter"]["min_date"],
        max_date = config["filter"]["max_date"],
        group_by = config["filter"]["group_by"],
        subsample_max_sequences = config["filter"]["subsample_max_sequences"],
    log:
        "logs/filter.txt"
    shell:
        r"""
        augur filter \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --metadata-id-columns {params.id_column:q} \
            --min-date {params.min_date:q} \
            --max-date {params.max_date:q} \
            --include {input.include} \
            --exclude {input.exclude:q} \
            --output-sequences {output.sequences:q} \
            --group-by {params.group_by:q} \
            --subsample-max-sequences {params.subsample_max_sequences:q} \
        2>&1 | tee {log}
        """

rule align:
    """
    Aligning sequences to {input.reference}
      - filling gaps with N
      - removing reference sequence
    """
    input:
        sequences = "results/filtered.fasta",
        reference = config["files"]["reference"],
    output:
        alignment = "results/aligned.fasta"
    log:
        "logs/align.txt"
    shell:
        r"""
        augur align \
            --sequences {input.sequences:q} \
            --reference-sequence {input.reference:q} \
            --output {output.alignment:q} \
            --fill-gaps \
            --remove-reference \
            --nthreads auto \
        2>&1 | tee {log}
        """
