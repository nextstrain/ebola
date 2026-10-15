

# rule infer_mutations_on_raw_tree:
#     input:
#         tree = "results/{species}/{build}/tree.nwk" # initial refine
#         alignment = alignment_for_tree,
#     output:
#         tree = "results/{species}/{build}/tree_raw_internal_nodes_labelled.nwk",
#         node_data = "results/{species}/{build}/muts-on-raw-tree.json",
#     benchmark:
#         "benchmarks/{species}/{build}/infer_mutations_on_raw_tree.txt"
#     log:
#         "logs/{species}/{build}/infer_mutations_on_raw_tree.txt"
#     shell:
#         r"""
#         exec &> >(tee {log:q})

#         augur refine \
#             --tree {input.tree:q} \
#             --output-tree {output.tree:q}

#         augur ancestral \
#             --tree {output.tree:q} \
#             --alignment {input.alignment:q} \
#             --keep-ambiguous \
#             --output-node-data {output.node_data:q}
#         """

rule detect_adar_edits:
    input:
        tree = "results/{species}/{build}/tree.nwk", # initial refine tree
        alignment = alignment_for_tree,
        muts = "results/{species}/{build}/muts.json" # from initial refine tree
    output:
        node_data = "results/{species}/{build}/adar_edits.json",
        alignment = "results/{species}/{build}/alignment-adar-stripped.fasta",
    benchmark:
        "benchmarks/{species}/{build}/detect_adar_edits.txt"
    log:
        "logs/{species}/{build}/detect_adar_edits.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        python3 scripts/adar-mutations.py \
            --muts {input.muts:q} \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --output {output.node_data:q} \
            --output-alignment {output.alignment:q}
        """
