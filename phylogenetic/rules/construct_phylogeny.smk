"""
This part of the workflow constructs the phylogenetic tree.

REQUIRED INPUTS:

    metadata            = results/{build}/filtered.tsv
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
        alignment = "results/{build}/aligned.fasta"
    output:
        tree = "results/{build}/tree_raw.nwk"
    benchmark:
        "benchmarks/{build}/tree.txt"
    log:
        "logs/{build}/tree.txt"
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
        tree = "results/{build}/tree_raw.nwk",
        alignment = "results/{build}/aligned.fasta",
        metadata = "results/{build}/filtered.tsv"
    output:
        tree = "results/{build}/tree.nwk",
        node_data = "results/{build}/branch_lengths.json"
    params:
        coalescent = conditional_config("--coalescent", "refine", "coalescent"),
        date_inference = conditional_config("--date-inference", "refine", "date_inference"),
        confidence = conditional_config("--date-confidence", "refine", "confidence"),
        timetree = conditional_config("--timetree", "refine", "timetree"),
        root = conditional_config("--root", "refine", "root"),
        remove_outgroup = conditional_config("--remove-outgroup", "refine", "remove_outgroup"),
        id_column = config["id_column"],
    benchmark:
        "benchmarks/{build}/refine.txt"
    log:
        "logs/{build}/refine.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur refine \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            {params.timetree:q} \
            {params.date_inference:q} \
            {params.coalescent:q} \
            {params.confidence:q} \
            {params.root:q} \
            {params.remove_outgroup:q} \
            --clock-rate 0.00105 \
            --output-tree {output.tree:q} \
            --output-node-data {output.node_data:q}
        """
