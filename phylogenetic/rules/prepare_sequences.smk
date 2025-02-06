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
    shell:
        """
        augur filter \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --include {input.include} \
            --exclude {input.exclude} \
            --output {output.sequences} \
            --group-by {params.group_by} \
            --sequences-per-group {params.sequences_per_group} \
            --min-date {params.min_date}
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
    shell:
        """
        augur align \
            --sequences {input.sequences} \
            --reference-sequence {input.reference} \
            --output {output.alignment} \
            --fill-gaps \
            --remove-reference \
            --nthreads auto
        """