rule update_example_data_wildcards:
    input:
        sequences="results/{species}/sequences.fasta",
        metadata="results/{species}/metadata.tsv",
    output:
        sequences="example_data/{species}/sequences.fasta",
        metadata="example_data/{species}/metadata.tsv",
    shell:
        r"""
        augur filter \
            --metadata {input.metadata} \
            --sequences {input.sequences} \
            --subsample-max-sequences 50 \
            --group-by month \
            --subsample-seed 0 \
            --output-metadata {output.metadata} \
            --output-sequences {output.sequences}
        """


rule update_example_data:
    input:
        expand(
            [
                "example_data/{species}/sequences.fasta",
                "example_data/{species}/metadata.tsv"
            ],
            species=[b.split('/')[0] for b in config['builds']]
        )
