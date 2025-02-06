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

rule filter:
    """
    Filtering to
      - {params.sequences_per_group} sequence(s) per {params.group_by!s}
      - from {params.min_date} onwards
      - excluding strains in {input.exclude}
    """
    input:
        sequences = "results/sequences.fasta",
        metadata = "results/metadata.tsv",
        include = files.forced_strains,
        exclude = files.dropped_strains
    output:
        sequences = "results/filtered.fasta"
    params:
        group_by = "division year month",
        sequences_per_group = 25,
        min_date = 2012
    benchmark:
        "benchmarks/filter.txt"
    log:
        "logs/filter.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur filter \
            --sequences {input.sequences:q} \
            --metadata {input.metadata:q} \
            --include {input.include:q} \
            --exclude {input.exclude:q} \
            --output {output.sequences:q} \
            --group-by {params.group_by:q} \
            --sequences-per-group {params.sequences_per_group:q} \
            --min-date {params.min_date:q}
        """

rule align:
    """
    Aligning sequences to {input.reference}
      - filling gaps with N
      - removing reference sequence
    """
    input:
        sequences = "results/filtered.fasta",
        reference = files.reference
    output:
        alignment = "results/aligned.fasta"
    benchmark:
        "benchmarks/align.txt"
    log:
        "logs/align.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur align \
            --sequences {input.sequences:q} \
            --reference-sequence {input.reference:q} \
            --output {output.alignment:q} \
            --fill-gaps \
            --remove-reference \
            --nthreads auto
        """
