"""
This part of the workflow creates additonal annotations for the reference tree
of the Nextclade dataset.

REQUIRED INPUTS:

    metadata            = data/metadata.tsv
    prepared_sequences  = results/{build}/prepared_sequences.fasta
    tree                = results/{build}/tree.nwk

OUTPUTS:

    nt_muts     = results/{build}/nt_muts.json
    aa_muts     = results/{build}/aa_muts.json
    clades      = results/{build}/clades.json

This part of the workflow usually includes the following steps:

    - augur ancestral
    - augur translate
    - augur clades

See Augur's usage docs for these commands for more details.
"""



rule ancestral:
    input:
        tree = "results/{build}/tree.nwk",
        sequences = "results/{build}/aligned.fasta",
        reference = "../shared/{build}/reference.gb"
    output:
        muts = "results/{build}/muts.json"
    params:
        translations = "results/{build}/%GENE_translations.fasta",
        genes = ['GP', 'sGP', 'ssGP', 'NP', 'L', 'VP24', 'VP30', 'VP35', 'VP40']
    shell:
        """
        augur ancestral --tree {input.tree} \
        --alignment {input.sequences} \
        --annotation {input.reference} \
        --output-node-data {output.muts} \
        --root-sequence {input.reference} \
        --translations {params.translations} \
        --genes {params.genes}
        """

rule extract_year:
    input:
        metadata = "results/{build}/filtered_metadata.tsv"
    output:
        years = "results/{build}/years.json"
    run:
        import pandas as pd
        import json
        df = pd.read_csv(input.metadata, sep="\t", dtype=str)
        df['year'] = df['date'].str.slice(0,4).astype(float)
        years_dict = df.set_index('accession')['year'].to_dict()
        years_dict_expanded = {k: {'year': v} for k, v in years_dict.items()}
        with open(output.years, 'w') as f:
            json.dump({"nodes": years_dict_expanded}, f)
rule clades:
    input:
        tree = "results/{build}/tree.nwk",
        muts = "results/{build}/muts.json",
        clades = "../shared/{build}/clades.tsv",
    output:
        clades = "results/{build}/clades.json"
    shell:
        """
        augur clades --tree {input.tree} \
        --mutations {input.muts} \
        --clade {input.clades} \
        --output {output.clades}
        """