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

rule set_hidden_attribute:
    input:
        clades = "results/{build}/clades.json",
        clade_defs = lambda w: config["build_params"][w.build]["files"].get("clades"),
    output:
        hidden = "results/{build}/hidden.json",
    benchmark:
        "benchmarks/{build}/set_hidden_attribute.txt"
    log:
        "logs/{build}/set_hidden_attribute.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        ./scripts/set-hidden-attribute.py {input.clades:q} {output.hidden:q}
        """


def export_node_data_inputs(w):
    files = [
        f"results/{w.build}/branch_lengths.json",
        f"results/{w.build}/traits.json",
        f"results/{w.build}/nt_muts.json",
        f"results/{w.build}/aa_muts.json",
    ]
    if config["build_params"][w.build]["files"].get("clades"):
        files += [
            f"results/{w.build}/clades.json",
            f"results/{w.build}/hidden.json",
        ]
    return files


rule export:
    """Exporting data files for for auspice"""
    input:
        tree = "results/{build}/tree.nwk",
        metadata = "results/{build}/filtered.tsv",
        node_data = lambda w: export_node_data_inputs(w),
        lat_longs = lambda w: config["build_params"][w.build]["files"]["lat_longs"],
        auspice_config = lambda w: config["build_params"][w.build]["files"]["auspice_config"],
        description = lambda w: config["build_params"][w.build]["files"]["description"],
    output:
        auspice_json = "auspice/ebola_{build}.json"
    params:
        id_column = config["id_column"],
        warning = lambda w: conditional("--warning", config["build_params"][w.build].get("export", {}).get("warning")),
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
            --node-data {input.node_data:q} \
            --lat-longs {input.lat_longs:q} \
            --auspice-config {input.auspice_config:q} \
            --description {input.description:q} \
            {params.warning:q} \
            --include-root-sequence-inline \
            --output {output.auspice_json:q}
        """
