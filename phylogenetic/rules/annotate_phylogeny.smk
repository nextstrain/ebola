
def _root_seq(wildcards):
    """If the config specifies a root-sequence file we resolve it and return an array o
    arguments for `augur ancestral`. Since we resolve file paths the files must exist
    which avoids the need to use Snakemake's input functionaly.
    """
    if p:=config['ancestral'][f"{wildcards.species}/{wildcards.build}"].get('root-sequence', False):
        resolved = resolve_config_path(p)({})
        return ['--root-sequence', resolved]
    return []


def _aa_reconstruction_via_ancestral(wildcards):
    genes = config['ancestral'][f"{wildcards.species}/{wildcards.build}"].get('genes', False)
    if genes is False:
        return ""
    if not isinstance(genes, str):
        raise Exception(f"ancestral's 'genes' config option must be a string ({wildcards.species}/{wildcards.build})")
    # Note: translations produced by run_nextclade
    translations_pattern = f"results/{wildcards.species}/translations/%GENE.fasta"
    annotation_file = resolve_config_path(config['ancestral'][f"{wildcards.species}/{wildcards.build}"]['annotation'])({})
    return f"--genes {genes} --translations {translations_pattern} --annotation {annotation_file} --report-inconsistent-translation"


rule ancestral:
    """Reconstructing mutations and (optionally) AA translations too"""
    input:
        tree = "results/{species}/{build}/tree.nwk",
        alignment = "results/{species}/{build}/subsampled.fasta", # unmasked
        # Note: input.annotation_file not directly used by the shell block, necessary for snakemake input checking. File is referenced by params.aa_reconstruction
        annotation_file = lambda w: resolve_config_path(config['ancestral'][f"{w.species}/{w.build}"]['annotation'])({}) if _aa_reconstruction_via_ancestral(w) else [],
    output:
        node_data = "results/{species}/{build}/muts.json"
    params:
        aa_reconstruction = _aa_reconstruction_via_ancestral,
        inference = lambda w: conditional('--inference', config['ancestral'][f"{w.species}/{w.build}"].get('inference', False)),
        extra_args = lambda w: config['ancestral'][f"{w.species}/{w.build}"].get('extra_args', ''), # will be replaced with config-in-YAML in the short/medium term
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
            {params.aa_reconstruction} \
            {params.inference} \
            {params.root_seq} \
            {params.extra_args} \
            --output-node-data {output.node_data:q}
        """

rule translate:
    input:
        tree = "results/{species}/{build}/tree.nwk",
        muts = "results/{species}/{build}/muts.json",
        reference = lambda w: resolve_config_path(config['translate'][f"{w.species}/{w.build}"]["reference"]),
    output:
        node_data = "results/{species}/{build}/aa_muts.json"
    params:
        genes = lambda w: conditional('--genes', config['translate'][f"{w.species}/{w.build}"].get('genes', False)),
    benchmark:
        "benchmarks/{species}/{build}/translate.txt"
    log:
        "logs/{species}/{build}/translate.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur translate \
            --tree {input.tree:q} \
            --ancestral-sequences {input.muts:q} \
            {params.genes} \
            --reference-sequence {input.reference:q} \
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
        bias = lambda w: conditional('--sampling-bias-correction', config['traits'][f"{w.species}/{w.build}"].get('bias', False)),
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
            --columns {params.columns:q} \
            {params.confidence} \
            {params.bias} \
            --output {output.node_data:q}
        """



rule sampling_year:
    input:
        metadata = "results/{species}/{build}/metadata.tsv",
    output:
        node_data = "results/{species}/{build}/sampling-year.json",
        config_block = "results/{species}/{build}/sampling-year.config.json",
    params:
        script = os.path.join(workflow.basedir, "scripts", "get_year.py"),
    shell:
        r"""
        exec &> >(tee {log:q})

        python {params.script} \
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
