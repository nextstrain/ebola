from augur.subsample import get_referenced_files


rule subsample:
    input:
        config = "results/{species}/{build}/subsample_config.yaml",
        sequences = "results/{species}/alignment.fasta",
        metadata = "results/{species}/metadata_extended.tsv",
        referenced_files = lambda w: get_referenced_files(f"results/{w.species}/{w.build}/subsample_config.yaml",
            None, # config section to navigate to
            SEARCH_PATHS),
    output:
        sequences = "results/{species}/{build}/subsampled.fasta",
        metadata = "results/{species}/{build}/metadata.tsv",
    params:
        id_field = config['strain_id_field'],
        search_paths = SEARCH_PATHS,
    log:
        "logs/{species}/{build}/subsample.txt",
    benchmark:
        "benchmarks/{species}/{build}/subsample.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        augur subsample \
            --config {input.config} \
            --search-paths {params.search_paths} \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --metadata-id-columns {params.id_field} \
            --output-sequences {output.sequences} \
            --output-metadata {output.metadata}
        """