"""
This part of the workflow collects the phylogenetic tree and annotations to
export a Nextstrain dataset.

REQUIRED INPUTS:

    metadata        = results/{build}/filtered.tsv
    tree            = results/tree.nwk
    branch_lengths  = results/branch_lengths.json
    node_data       = results/*.json

OUTPUTS:

    auspice_json = auspice/${build_name}.json

    There are optional sidecar JSON files that can be exported as part of the dataset.
    See Nextstrain's data format docs for more details on sidecar files:
    https://docs.nextstrain.org/page/reference/data-formats.html

This part of the workflow usually includes the following steps:

    - augur export v2
    - augur frequencies

See Augur's usage docs for these commands for more details.
"""

rule export:
    """Exporting data files for for auspice"""
    input:
        tree = "results/{build}/tree.nwk",
        metadata = "results/{build}/filtered.tsv",
        branch_lengths = "results/{build}/branch_lengths.json",
        traits = "results/{build}/traits.json",
        nt_muts = "results/{build}/nt_muts.json",
        aa_muts = "results/{build}/aa_muts.json",
        colors = lambda w: config["build_params"][w.build]["files"]["colors"],
        lat_longs = lambda w: config["build_params"][w.build]["files"]["lat_longs"],
        auspice_config = lambda w: config["build_params"][w.build]["files"]["auspice_config"],
        description = lambda w: config["build_params"][w.build]["files"]["description"],
    output:
        auspice_json = "auspice/ebola_{build}.json"
    params:
        id_column = config["id_column"],
    benchmark:
        "benchmarks/{build}/export.txt"
    log:
        "logs/{build}/export.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur export v2 \
            --tree {input.tree:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            --node-data {input.branch_lengths:q} {input.traits:q} {input.nt_muts:q} {input.aa_muts:q} \
            --colors {input.colors:q} \
            --lat-longs {input.lat_longs:q} \
            --auspice-config {input.auspice_config:q} \
            --description {input.description:q} \
            --include-root-sequence \
            --output {output.auspice_json:q}
        """
