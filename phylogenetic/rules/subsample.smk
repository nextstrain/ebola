from augur.subsample import get_referenced_files


rule subsample:
    input:
        config = "results/{species}/{build}/subsample_config.yaml",
        sequences = "results/{species}/alignment.fasta",
        metadata = "results/{species}/metadata_extended.tsv",
        # note: get_referenced_files will use the env variable AUGUR_SEARCH_PATHS
        referenced_files = lambda w: get_referenced_files(f"results/{w.species}/{w.build}/subsample_config.yaml")
    output:
        sequences = "results/{species}/{build}/subsampled.fasta",
        metadata = "results/{species}/{build}/metadata.tsv",
    params:
        id_field = config['strain_id_field'],
    log:
        "logs/{species}/{build}/subsample.txt",
    benchmark:
        "benchmarks/{species}/{build}/subsample.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        augur subsample \
            --config {input.config} \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --metadata-id-columns {params.id_field} \
            --output-sequences {output.sequences} \
            --output-metadata {output.metadata}
        """