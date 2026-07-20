
def sites_to_mask(wildcards):
    build_options = config['mask'][f"{wildcards.species}/{wildcards.build}"]
    sites = build_options.get('sites')
    if sites:
        return f'--mask-sites {sites}'
    return ""

rule mask:
    input:
        alignment="results/{species}/{build}/subsampled.fasta",
    output:
        alignment="results/{species}/{build}/masked.fasta",
    params:
        mask_beginning=lambda w: config['mask'][f"{w.species}/{w.build}"]['beginning'],
        mask_end=lambda w: config['mask'][f"{w.species}/{w.build}"]['end'],
        mask_sites =sites_to_mask
    shell:
        r"""
        augur mask --sequences {input.alignment} \
                    --mask-from-beginning {params.mask_beginning} \
                    --mask-from-end {params.mask_end} \
                    {params.mask_sites} \
                    --output {output.alignment}
        """


def alignment_for_tree(wildcards):
    """If masking is defined (for this build) this returns the masked alignment,
    else returns the unmasked (but subsampled) alignment
    """
    if config.get('mask', {}).get(f"{wildcards.species}/{wildcards.build}", False):
        return f"results/{wildcards.species}/{wildcards.build}/masked.fasta",
    return f"results/{wildcards.species}/{wildcards.build}/subsampled.fasta",


def args_for_tree(wildcards):
    build_options = config['tree'].get(f"{wildcards.species}/{wildcards.build}",False)
    if build_options:
        return build_options
    return ""

rule tree:
    """Building tree"""
    input:
        alignment = alignment_for_tree,
    output:
        tree = "results/{species}/{build}/tree_raw.nwk"
    params:
       args =args_for_tree,
    benchmark:
        "benchmarks/{species}/{build}/tree.txt"
    log:
        "logs/{species}/{build}/tree.txt"
    threads: 4
    shell:
        r"""
        exec &> >(tee {log:q})

        augur tree \
            --alignment {input.alignment:q} \
            --output {output.tree:q} \
            {params.args} \
            --nthreads {threads:q} 
        """

rule reroot_tree:
    input:
        tree = "results/{species}/{build}/tree_raw.nwk"
    output:
        tree = "results/{species}/{build}/tree_raw_rooted.nwk"
    params:
        strains = lambda w: config['reroot_tree'][f"{w.species}/{w.build}"]['strains'],
        remove_outgroup = lambda w: config['reroot_tree'][f"{w.species}/{w.build}"].get('remove_outgroup',False)
    run:
        from Bio import Phylo
        T = Phylo.read(input.tree, "newick")
        T.root_at_midpoint()
        strains = params.strains
        print("Rooting tree using the common ancestor of these strains as the outgroup:", strains)
        ca = T.common_ancestor(strains)
        T.root_with_outgroup(ca)
        if params.remove_outgroup:
            for strain in strains:
                T.prune(strain)
        Phylo.write(T, output.tree, "newick")


def tree_for_refine(wildcards):
    """Typically returns the newick tree from rule tree (augur tree)
    but some builds may us a different / additional rule to (e.g.)
    reroot the tree.
    """
    if config.get('reroot_tree', {}).get(f"{wildcards.species}/{wildcards.build}", False):
        return f"results/{wildcards.species}/{wildcards.build}/tree_raw_rooted.nwk",
    return f"results/{wildcards.species}/{wildcards.build}/tree_raw.nwk",


rule refine:
    """
    Refining tree
      - estimate timetree
      - use {params.coalescent} coalescent timescale
      - estimate {params.date_inference} node dates
    """
    input:
        tree = tree_for_refine,
        alignment = alignment_for_tree,
        metadata = "results/{species}/{build}/metadata.tsv"
    output:
        tree = "results/{species}/{build}/tree.nwk",
        node_data = "results/{species}/{build}/branch_lengths.json"
    params:
        args = lambda w: config['refine'][f"{w.species}/{w.build}"],
        id_field = config['strain_id_field'],
    benchmark:
        "benchmarks/{species}/{build}/refine.txt"
    log:
        "logs/{species}/{build}/refine.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur refine \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_field:q} \
            --output-tree {output.tree:q} \
            --output-node-data {output.node_data:q} \
            {params.args}
        """
