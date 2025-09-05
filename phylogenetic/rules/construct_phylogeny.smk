"""
This part of the workflow constructs the phylogenetic tree.

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
    """Building tree"""
    input:
        alignment = "results/aligned.fasta"
    output:
        tree = "results/tree_raw.nwk"
    benchmark:
        "benchmarks/tree.txt"
    log:
        "logs/tree.txt"
    threads: 4
    shell:
        r"""
        exec &> >(tee {log:q})

        augur tree \
            --alignment {input.alignment:q} \
            --output {output.tree:q} \
            --nthreads {threads:q}
        """

rule refine:
    """
    Refining tree
      - estimate timetree
      - use {params.coalescent} coalescent timescale
      - estimate {params.date_inference} node dates
    """
    input:
        tree = "results/tree_raw.nwk",
        alignment = "results/aligned.fasta",
        metadata = "data/metadata.tsv"
    output:
        tree = "results/tree.nwk",
        node_data = "results/branch_lengths.json"
    params:
        coalescent = config["refine"]["coalescent"],
        date_inference = config["refine"]["date_inference"],
        id_column = config["id_column"],
    benchmark:
        "benchmarks/refine.txt"
    log:
        "logs/refine.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur refine \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            --output-tree {output.tree:q} \
            --output-node-data {output.node_data:q} \
            --timetree \
            --coalescent {params.coalescent:q} \
            --date-confidence \
            --date-inference {params.date_inference:q}
        """
