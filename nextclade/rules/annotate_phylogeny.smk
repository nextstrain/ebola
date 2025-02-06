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
