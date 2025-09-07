"""
This part of the workflow constructs the reference tree for the Nextclade dataset

REQUIRED INPUTS:

    metadata            = data/metadata.tsv
    prepared_sequences  = results/prepared_sequences.fasta

OUTPUTS:

    tree            = results/tree.nwk
    branch_lengths  = results/branch_lengths.json

This part of the workflow usually includes the following steps:

    - augur tree
    - augur refine

See Augur's usage docs for these commands for more details.
"""


rule tree:
    input:
        sequences = "results/aligned.fasta"
    output:
        tree = "results/tree_raw.nwk"
    shell:
        """
        augur tree --alignment {input.sequences} \
        --output {output.tree} \
        --method iqtree \
        --nthreads 8
        """

rule refine:
    input:
        tree = "results/tree_raw.nwk",
        metadata = "results/filtered_metadata.tsv",
        sequences = "results/aligned.fasta"
    output:
        refined_tree = "results/tree.nwk",
        branch_lengths = "results/branch_lengths.json"
    shell:
        """
        augur refine --tree {input.tree} \
        --metadata {input.metadata} \
        --alignment {input.sequences} \
        --metadata-id-columns accession \
        --output-tree {output.refined_tree} \
        --output-node-data {output.branch_lengths} \
        --root mid_point
        """
