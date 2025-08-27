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

# TODO: upload ingest results to data.nextstrain.org and download them here.
rule copy_ingest_files:
    input:
        sequences = "../ingest/results/sequences.fasta",
        metadata = "../ingest/results/metadata.tsv"
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
        exclude = lambda w: config["build_params"][w.build]["filter"]["exclude"],
        include = lambda w: config["build_params"][w.build]["filter"]["include"],
    output:
        sequences = "results/{build}/filtered.fasta"
    params:
        id_column = config["id_column"],
        min_length = lambda w: config["build_params"][w.build]["filter"]["min_length"],
        min_date = lambda w: config["build_params"][w.build]["filter"]["min_date"],
        max_date = lambda w: config["build_params"][w.build]["filter"]["max_date"],
        group_by = lambda w: config["build_params"][w.build]["filter"]["group_by"],
        subsample_max_sequences = lambda w: config["build_params"][w.build]["filter"]["subsample_max_sequences"],
    benchmark:
        "benchmarks/{build}/filter.txt"
    log:
        "logs/{build}/filter.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur filter \
            --sequences {input.sequences:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            --min-length {params.min_length:q} \
            --min-date {params.min_date:q} \
            --max-date {params.max_date:q} \
            --include {input.include:q} \
            --exclude {input.exclude:q} \
            --output-sequences {output.sequences:q} \
            --group-by {params.group_by:q} \
            --subsample-max-sequences {params.subsample_max_sequences:q}
        """

rule align:
    """
    Aligning sequences to {input.reference}
      - filling gaps with N
      - removing reference sequence
    """
    input:
        sequences = "results/{build}/filtered.fasta",
        reference = lambda w: config["build_params"][w.build]["files"]["reference"],
    output:
        alignment = "results/{build}/aligned.fasta"
    benchmark:
        "benchmarks/{build}/align.txt"
    log:
        "logs/{build}/align.txt"
    threads: 4
    shell:
        r"""
        exec &> >(tee {log:q})

        augur align \
            --sequences {input.sequences:q} \
            --reference-sequence {input.reference:q} \
            --output {output.alignment:q} \
            --fill-gaps \
            --remove-reference \
            --nthreads {threads:q}
        """
