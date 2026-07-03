
def _root_seq(wildcards):
    """If the config specifies a root-sequence file we resolve it and return an array o
    arguments for `augur ancestral`. Since we resolve file paths the files must exist
    which avoids the need to use Snakemake's input functionaly.
    """
    if p:=config['ancestral'][f"{wildcards.species}/{wildcards.build}"].get('root-sequence', False):
        resolved = resolve_config_path(p)({})
        return ['--root-sequence', resolved]
    return []


rule ancestral:
    """Reconstructing ancestral sequences and mutations"""
    input:
        tree = "results/{species}/{build}/tree.nwk",
        alignment = "results/{species}/{build}/subsampled.fasta", # unmasked
        annotation = lambda w: resolve_config_path(config['ancestral'][f"{w.species}/{w.build}"]['annotation'])({}),
    output:
        node_data = "results/{species}/{build}/muts.json"
    params:
        genes = lambda w: config['ancestral'][f"{w.species}/{w.build}"]['genes'],
        inference = lambda w: conditional('--inference', config['ancestral'][f"{w.species}/{w.build}"].get('inference', False)),
        root_seq = _root_seq,
    benchmark:
        "benchmarks/{species}/{build}/ancestral.txt"
    log:
        "logs/{species}/{build}/ancestral.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur ancestral \
            --tree {input.tree:q} \
            --alignment {input.alignment:q} \
            --annotation {input.annotation} \
            --translations results/{wildcards.species}/translations/%GENE.fasta \
            --genes {params.genes} \
            {params.inference} \
            {params.root_seq} \
            --report-inconsistent-translation \
            --output-node-data {output.node_data:q}
        """

rule count_mutations:
    """Count the nucleotide and amino-acid mutations per node"""
    input:
        node_data = "results/{species}/{build}/muts.json"
    output:
        node_data = "results/{species}/{build}/muts-counts.json"
    params:
        script = os.path.join(workflow.basedir, "scripts", "collect-mutations.py"),
        cds = lambda w: config['count_mutations'][f"{w.species}/{w.build}"].get('cds', []),
        counts = lambda w: config['count_mutations'][f"{w.species}/{w.build}"].get('counts', ''),
    shell:
        r"""
        python {params.script} \
            --muts {input.node_data:q} \
            --cds {params.cds} \
            --counts {params.counts} \
            --output {output.node_data:q}
        """

rule traits:
    input:
        tree = "results/{species}/{build}/tree.nwk",
        metadata = "results/{species}/{build}/metadata.tsv",
    output:
        node_data = "results/{species}/{build}/traits.json",
    params:
        columns = lambda w: config['traits'][f"{w.species}/{w.build}"]['columns'],
        confidence = lambda w: conditional('--confidence', config['traits'][f"{w.species}/{w.build}"].get('confidence', False)),
        id_field = config['strain_id_field'],
    benchmark:
        "benchmarks/{species}/{build}/traits.txt"
    log:
        "logs/{species}/{build}/traits.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur traits \
            --tree {input.tree:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_field:q} \
            --columns {params.columns:q} \
            {params.confidence} \
            --output {output.node_data:q}
        """



rule sampling_year:
    input:
        metadata = "results/{species}/{build}/metadata.tsv",
    output:
        node_data = "results/{species}/{build}/sampling-year.json",
        config_block = "results/{species}/{build}/sampling-year.config.json",
    params:
        id_field = config['strain_id_field'],
        script = os.path.join(workflow.basedir, "scripts", "get_year.py"),
    shell:
        r"""
        exec &> >(tee {log:q})

        python {params.script} \
            --id-columns {params.id_field:q} \
            --metadata {input.metadata:q} \
            --output {output.node_data:q} \
            --output-config {output.config_block:q}
        """


rule label_outbreaks:
    input:
        metadata = "results/{species}/{build}/metadata.tsv",
        tree = "results/{species}/{build}/tree.nwk",
    output:
        node_data = "results/{species}/{build}/outbreaks.json"
    params:
        script = os.path.join(workflow.basedir, "scripts", "label_outbreaks.py"),
    benchmark:
        "benchmarks/{species}/{build}/label_outbreaks.txt"
    log:
        "logs/{species}/{build}/label_outbreaks.txt"
    wildcard_constraints:
        # these could be relaxed if we generalised the script
        species="ebov",
        build="all-outbreaks",
    shell:
        r"""
        exec &> >(tee {log:q})

        python {params.script} \
            --metadata {input.metadata:q} \
            --tree {input.tree:q} \
            --output {output.node_data:q}
        """
