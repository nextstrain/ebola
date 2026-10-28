# Rules and functions related to detecting and masking ADAR editing


def adar_enabled(wildcards):
    build_pair = f"{wildcards.species}/{wildcards.build}"
    return bool(config.get('adar_mutations', {}).get(build_pair, False))


def adar_param(wildcards, key):
    """Fetch a required ADAR-detection parameter from the config.

    The config must define both 'max_gap' and 'min_count' for any build with
    ADAR detection enabled; a missing key raises a KeyError rather than falling
    back to a workflow-level default.
    """
    build_pair = f"{wildcards.species}/{wildcards.build}"
    return config['adar_mutations'][build_pair][key]


rule detect_adar_edits:
    input:
        tree = "results/{species}/{build}/tree.nwk", # initial refine tree
        alignment = alignment_for_tree,
        muts = "results/{species}/{build}/muts.json" # from initial refine tree
    output:
        node_data = "results/{species}/{build}/adar_edits.json",
        alignment = "results/{species}/{build}/alignment-adar-stripped.fasta",
    params:
        script = os.path.join(workflow.basedir, "scripts", "adar-mutations.py"),
        max_gap = lambda w: adar_param(w, 'max_gap'),
        min_count = lambda w: adar_param(w, 'min_count'),
    benchmark:
        "benchmarks/{species}/{build}/detect_adar_edits.txt"
    log:
        "logs/{species}/{build}/detect_adar_edits.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        python3 {params.script} \
            --muts {input.muts:q} \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --output {output.node_data:q} \
            --output-alignment {output.alignment:q} \
            --max-gap {params.max_gap} \
            --min-count {params.min_count}
        """


rule refine_adar_mutations_removed:
    # Re-refine the refined tree using an ADAR-removed alignment which
    # should have a stronger clock signal.
    # This refine step should keep the topology unchanged via the config-supplied
    # args specifying `--keep-polytomies --keep-root`
    input:
        tree = "results/{species}/{build}/tree.nwk", # initial refine tree
        alignment = "results/{species}/{build}/alignment-adar-stripped.fasta",
        metadata = "results/{species}/{build}/metadata.tsv",
        keep_ids = "results/{species}/{build}/keep_ids.txt",
    output:
        tree = "results/{species}/{build}/tree-adar-stripped.nwk",
        node_data = "results/{species}/{build}/branch_lengths_after_adar_stripped.json"
    params:
        args = lambda w: config['refine_adar_mutations_removed'][f"{w.species}/{w.build}"],
    benchmark:
        "benchmarks/{species}/{build}/refine_adar_mutations_removed.txt"
    log:
        "logs/{species}/{build}/refine_adar_mutations_removed.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur refine \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --metadata {input.metadata:q} \
            --output-tree {output.tree:q} \
            --output-node-data {output.node_data:q} \
            --keep-ids {input.keep_ids:q} \
            --seed 0 \
            {params.args}
        """