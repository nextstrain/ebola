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
rule filter:
    """
    Filtering to
      - {params.sequences_per_group} sequence(s) per {params.group_by!s}
      - from {params.min_date} onwards
      - excluding strains in {input.exclude}
    """
    input:
        sequences = lambda w: path_or_url(config["inputs"][0]['sequences']),
        metadata = lambda w: path_or_url(config["inputs"][0]['metadata']),
    output:
        sequences = "results/{build}/filtered.fasta",
        metadata = "results/{build}/filtered.tsv",
        log = "results/{build}/filter-log.txt",
    params:
        args = get_config_args("filter"),
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
            {params.args:q} \
            --output-sequences {output.sequences:q} \
            --output-metadata {output.metadata:q} \
            --output-log {output.log:q}
        """

rule align:
    """
    Aligning sequences to {input.reference}
      - filling gaps with N
      - removing reference sequence
    """
    input:
        sequences = "results/{build}/filtered.fasta",
        reference = config_path("align", "reference"),
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
