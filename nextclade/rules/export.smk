"""
This part of the workflow collects the phylogenetic tree and annotations to
export a reference tree and create the Nextclade dataset.

REQUIRED INPUTS:

    augur export:
        metadata            = data/metadata.tsv
        tree                = results/tree.nwk
        branch_lengths      = results/branch_lengths.json
        nt_muts             = results/nt_muts.json
        aa_muts             = results/aa_muts.json
        clades              = results/clades.json

    Nextclade dataset files:
        reference           = ../shared/reference.fasta
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
        tree = "results/tree.nwk",
        metadata = "data/metadata.tsv",
        branch_lengths = "results/branch_lengths.json",
        clades = "results/clades.json",
        nt_muts = "results/nt_muts.json",
        aa_muts = "results/aa_muts.json",
        colors = config["files"]["colors"],
        auspice_config = config["files"]["auspice_config"],
    output:
        auspice_json = config["files"]["auspice_json"],
    params:
        id_column = config["id_column"],
    log:
        "logs/export.txt",
    benchmark:
        "benchmarks/export.txt",
    shell:
        r"""
        augur export v2 \
            --tree {input.tree:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            --node-data {input.branch_lengths:q} {input.nt_muts:q} {input.aa_muts:q} {input.clades:q} \
            --colors {input.colors:q} \
            --auspice-config {input.auspice_config:q} \
            --include-root-sequence-inline \
            --output {output.auspice_json:q} \
        2>&1 | tee {log}
        """

# rule assemble_dataset:

# rule test_dataset:
