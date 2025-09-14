"""
This part of the workflow collects the phylogenetic tree and annotations to
export a reference tree and create the Nextclade dataset.

REQUIRED INPUTS:

    augur export:
        metadata            = data/metadata.tsv
        tree                = results/{build}/tree.nwk
        branch_lengths      = results/{build}/branch_lengths.json
        nt_muts             = results/{build}/nt_muts.json
        aa_muts             = results/{build}/aa_muts.json
        clades              = results/{build}/clades.json

    Nextclade dataset files:
        reference           = ../shared/{build}/reference.fasta
        pathogen            = config/pathogen.json
        genome_annotation   = config/genome_annotation.gff3
        readme              = config/README.md
        changelog           = config/CHANGELOG.md
        example_sequences   = config/sequence.fasta

OUTPUTS:

    nextclade_dataset = datasets/${build_name}/*

    See Nextclade docs on expected naming conventions of dataset files
    https://docs.nextstrain.org/projects/nextclade/page/user/datasets.html

This part of the workflow usually includes the following steps:

    - augur export v2
    - cp Nextclade datasets files to new datasets directory

See Augur's usage docs for these commands for more details.
"""


rule export:
    input:
        metadata = "results/{build}/filtered_metadata.tsv",
        tree = "results/{build}/tree.nwk",
        branch_lengths = "results/{build}/branch_lengths.json",
        muts = "results/{build}/muts.json",
        clades = "results/{build}/clades.json",
        years = "results/{build}/years.json",
        auspice_config = "defaults/auspice_config.json"
    output:
        auspice_json = "results/{build}/auspice.json"
    shell:
        """
        augur export v2 --tree {input.tree} \
        --metadata-id-columns accession \
        --metadata {input.metadata} \
        --auspice-config {input.auspice_config} \
        --node-data {input.branch_lengths} {input.muts} {input.years} {input.clades} \
        --include-root-sequence-inline \
        --output {output.auspice_json}
        """

rule prepare_dataset:
    input:
        auspice_json = "results/{build}/auspice.json",
        reference = "../shared/{build}/reference.fasta",
        pathogen = "dataset_files/{build}/pathogen.json",
        genome_annotation = "../shared/{build}/annotation.gff",
        readme = "dataset_files/{build}/README.md",
        changelog = "dataset_files/{build}/CHANGELOG.md",
        example_sequences = "results/{build}/example_sequences.fasta"
    output:
        dataset_dir = directory("dataset/{build}")
    shell:
        """
        mkdir -p {output.dataset_dir}
        cp {input.auspice_json} {output.dataset_dir}/tree.json
        cp {input.reference} {output.dataset_dir}/reference.fasta
        cp {input.pathogen} {output.dataset_dir}/pathogen.json
        cp {input.genome_annotation} {output.dataset_dir}/genome_annotation.gff3
        cp {input.readme} {output.dataset_dir}/README.md
        cp {input.changelog} {output.dataset_dir}/CHANGELOG.md
        cp {input.example_sequences} {output.dataset_dir}/example_sequences.fasta
        """
