"""
This part of the workflow creates additonal annotations for the reference tree
of the Nextclade dataset.

REQUIRED INPUTS:

    metadata            = data/metadata.tsv
    prepared_sequences  = results/prepared_sequences.fasta
    tree                = results/tree.nwk

OUTPUTS:

    nt_muts     = results/nt_muts.json
    aa_muts     = results/aa_muts.json
    clades      = results/clades.json

This part of the workflow usually includes the following steps:

    - augur ancestral
    - augur translate
    - augur clades

See Augur's usage docs for these commands for more details.
"""



rule ancestral:
    input:
        tree = "results/tree.nwk",
        sequences = "results/aligned.fasta",
        reference = "../shared/reference.gb"
    output:
        muts = "results/muts.json"
    params:
        translations = "results/%GENE_translations.fasta",
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
        metadata = "results/filtered_metadata.tsv"
    output:
        years = "results/years.json"
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
        tree = "results/tree.nwk",
        muts = "results/muts.json",
        clades = "../shared/clades.tsv",
    output:
        clades = "results/clades.json"
    shell:
        """
        augur clades --tree {input.tree} \
        --mutations {input.muts} \
        --clade {input.clades} \
        --output {output.clades}
        """