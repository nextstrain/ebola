"""
This part of the workflow creates additonal annotations for the phylogenetic tree.

REQUIRED INPUTS:

    metadata            = data/metadata.tsv
    prepared_sequences  = results/prepared_sequences.fasta
    tree                = results/tree.nwk

OUTPUTS:

    node_data = results/*.json

    There are no required outputs for this part of the workflow as it depends
    on which annotations are created. All outputs are expected to be node data
    JSON files that can be fed into `augur export`.

    See Nextstrain's data format docs for more details on node data JSONs:
    https://docs.nextstrain.org/page/reference/data-formats.html

This part of the workflow usually includes the following steps:

    - augur traits
    - augur ancestral
    - augur translate
    - augur clades

See Augur's usage docs for these commands for more details.

Custom node data files can also be produced by build-specific scripts in addition
to the ones produced by Augur commands.
"""

rule ancestral:
    """Reconstructing ancestral sequences and mutations"""
    input:
        tree = "results/{dataset}/tree.nwk",
        alignment = "results/{dataset}/aligned.fasta"
    output:
        node_data = "results/{dataset}/nt_muts.json"
    params:
        inference = config["ancestral"]["inference"],
    log:
        "logs/{dataset}/ancestral.txt"
    shell:
        r"""
        augur ancestral \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --output-node-data {output.node_data:q} \
            --inference {params.inference:q} \
        2>&1 | tee {log}
        """

rule translate:
    """Translating amino acid sequences"""
    input:
        tree = "results/{dataset}/tree.nwk",
        node_data = "results/{dataset}/nt_muts.json",
        reference = config["files"]["reference"],
    output:
        node_data = "results/{dataset}/aa_muts.json"
    log:
        "logs/{dataset}/translate.txt"
    shell:
        r"""
        augur translate \
            --tree {input.tree:q} \
            --ancestral-sequences {input.node_data:q} \
            --reference-sequence {input.reference:q} \
            --output {output.node_data:q} \
        2>&1 | tee {log}
        """

rule traits:
    """Inferring ancestral traits for {params.columns!s}"""
    input:
        tree = "results/{dataset}/tree.nwk",
        metadata = "data/metadata.tsv"
    output:
        node_data = "results/{dataset}/traits.json",
    params:
        columns = config["traits"]["columns"],
        id_column = config["id_column"],
    log:
        "logs/{dataset}/traits.txt"
    shell:
        r"""
        augur traits \
            --tree {input.tree:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            --output {output.node_data:q} \
            --columns {params.columns:q} \
            --confidence \
        2>&1 | tee {log}
        """
