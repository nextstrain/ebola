rule test_dataset:
    input:
        tree="datasets/{build_name}/tree.json",
        pathogen_json="datasets/{build_name}/pathogen.json",
        sequences="datasets/{build_name}/sequences.fasta",
        annotation="datasets/{build_name}/genome_annotation.gff3",
        readme="datasets/{build_name}/README.md",
        changelog="datasets/{build_name}/CHANGELOG.md",
    output:
        outdir=directory("test_output/{build_name}"),
    params:
        dataset_dir="datasets/{build_name}",
    shell:
        """
        nextclade3 run \
            {input.sequences} \
            --input-dataset {params.dataset_dir} \
            --output-all {output.outdir}
        """
