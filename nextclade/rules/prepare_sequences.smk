"""
This part of the workflow prepares sequences for constructing the reference tree
of the Nextclade dataset.

REQUIRED INPUTS:

    metadata    = data/metadata.tsv
    sequences   = data/sequences.fasta
    reference   = ../shared/reference.fasta

OUTPUTS:

    prepared_sequences = results/prepared_sequences.fasta

This part of the workflow usually includes the following steps:

    - augur index
    - augur filter
    - nextclade run
    - augur mask

See Nextclade's and Augur's usage docs for these commands for more details.
"""

rule include_file:
    input:
        include = "defaults/include_genbank.txt",
        metadata = "data/metadata.tsv"
    output:
        include = "results/include.txt"
    run:
        import pandas as pd
        df = pd.read_csv(input.metadata, sep="\t", dtype=str)
        # genbank to ppx accession conversion
        gb_to_ppx = { k.split('.')[0]: v for k,v in zip(df['INSDC_accession'], df['accession']) if pd.notna(k) and pd.notna(v)}

        with open(input.include) as f:
            include = []
            for line in f:
                entry = line.strip()
                if entry:
                    include.append(gb_to_ppx.get(entry, entry))

        with open(output.include, 'w') as f:
            f.writelines("\n".join(include))


rule filter:
    input:
        metadata = "data/metadata.tsv",
        sequences = "data/sequences.fasta",
        include = "results/include.txt"
    output:
        filtered_sequences = "results/filtered_sequences.fasta",
        filtered_metadata = "results/filtered_metadata.tsv"
    shell:
        """
        augur filter --metadata {input.metadata} \
        --sequences {input.sequences} \
        --group-by country year \
        --include {input.include} \
        --min-length 16000 \
        --exclude-where 'region!=Africa' \
        --metadata-id accession \
        --sequences-per-group 10 \
        --output-sequences {output.filtered_sequences} \
        --output-metadata {output.filtered_metadata}
        """

rule example_sequences:
    input:
        metadata = "data/metadata.tsv",
        sequences = "data/sequences.fasta"
    output:
        filtered_sequences = "results/example_sequences.fasta"
    shell:
        """
        augur filter --metadata {input.metadata} \
        --sequences {input.sequences} \
        --group-by country year \
        --exclude-where 'dataUseTerms=RESTRICTED' \
        --metadata-id accession \
        --sequences-per-group 1 \
        --output-sequences {output.filtered_sequences}
        """


rule align:
    input:
        reference = "../shared/reference.fasta",
        filtered_sequences = "results/filtered_sequences.fasta",
        pathogen_json = "dataset_files/pathogen.json",
        annotation = "../shared/annotation.gff"
    output:
        aligned_sequences = "results/aligned.fasta",
    params:
        translations = lambda w: f"results/{{cds}}_translations.fasta",
    shell:
        """
        nextclade run --input-ref {input.reference} \
        --input-pathogen-json {input.pathogen_json} \
        --input-annotation {input.annotation} \
        --output-fasta {output.aligned_sequences} \
        --output-translations {params.translations} \
        --jobs 4 \
        {input.filtered_sequences}
        """
