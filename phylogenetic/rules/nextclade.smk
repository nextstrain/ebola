
rule get_nextclade_dataset:
    """Download Nextclade dataset"""
    output:
        dataset="results/{species}/nextclade-dataset.zip",
    params:
        name=lambda w: config['nextclade'][w.species]['dataset-name']
    benchmark:
        "benchmarks/{species}/get_nextclade_dataset.txt"
    log:
        "logs/{species}/get_nextclade_dataset.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        nextclade3 dataset get \
            --name {params.name} \
            --output-zip {output.dataset:q}
        """

rule run_nextclade:
    input:
        sequences="results/{species}/sequences.fasta",
        dataset="results/{species}/nextclade-dataset.zip",
    output:
        metadata="results/{species}/nextclade.tsv",
        alignment="results/{species}/alignment.fasta",
        # TODO - are translations an 'output'?
    params:
        translations=lambda w: f"results/{w.species}/translations/{{cds}}.fasta",
        # translations_dir = "results/{species}/translations/"
    benchmark:
        "benchmarks/{species}/run_nextclade.txt"
    log:
        "logs/{species}/run_nextclade.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        nextclade3 run \
            {input.sequences:q} \
            --input-dataset {input.dataset:q} \
            --output-tsv {output.metadata:q} \
            --output-fasta {output.alignment:q} \
            --output-translations {params.translations:q}
        """


rule add_nextclade_columns:
    """
    Merge config-defined columns from Nextclade's (verbose) TSV into our per-species metadata file
    """
    input:
        metadata="results/{species}/metadata.tsv",
        nextclade="results/{species}/nextclade.tsv",
    output:
        nextclade_subset=temp("results/{species}/nextclade_subset.tsv"),
        metadata="results/{species}/metadata_extended.tsv",
    params:
        source_cols=lambda w: ",".join(['seqName', *[el[0] for el in config['nextclade'][w.species]['columns']]]),
        dest_cols=lambda w: ",".join([config['strain_id_field'], *[el[1] for el in config['nextclade'][w.species]['columns']]]),
        id_field = config['strain_id_field'],
    benchmark:
        "benchmarks/{species}/add_nextclade_columns.txt"
    log:
        "logs/{species}/add_nextclade_columns.txt"
    shell:
        r"""
        exec &> >(tee {log:q})
    
        cat {input.nextclade} \
            | csvtk -t cut -f {params.source_cols} \
            | csvtk -t rename -f {params.source_cols} -n {params.dest_cols} \
            > {output.nextclade_subset:q}
    
        augur merge \
            --metadata nextclade={output.nextclade_subset:q}  metadata={input.metadata:q} \
            --metadata-id-columns {params.id_field} \
            --output-metadata {output.metadata:q} \
            --no-source-columns
        """

