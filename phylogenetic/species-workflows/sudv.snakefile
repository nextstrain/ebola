# The bulk of the rules are generic and are located in the (included) generic.snakefile
# It needs inputs of "results/{species}/sequences.fasta" and "results/{species}/metadata.tsv"
# which we provision in this file

from pathlib import Path
REPO = Path(workflow.current_basedir).parent.parent

config['sequences'] = REPO / "ingest/results/{species}/sequences.fasta"
config['metadata']  = REPO / "ingest/results/{species}/metadata.tsv"
config['exclude']  = REPO / "phylogenetic" / "species-workflows" / "exclude_sudv.txt"
config['id_column'] = "accession"
config['species'] = ['sudv']
config['qc_min_length'] = 5_000
# config['treetime_args'] = "--timetree --clock-filter-iqd 0 --root best --precision 3 --max-iter 5"
config['treetime_args'] = "--root mid_point"
config['cds'] = ["NP", "VP35", "VP40", "GP", "sGP", "ssGP", "VP30", "VP24", "L"]
config['id_column'] = "accession"
config['genbank_reference'] = REPO / "shared" / "sudan" / "reference.gb"
config['fasta_reference'] = REPO / "shared" / "sudan" / "reference.fasta"
config['gff_annotation'] = REPO / "shared" / "sudan" / "genome_annotation.gff3"
config['nextclade_pathogen_json'] = REPO / "nextclade" / "dataset_files" / "bdbv" / "pathogen.json"

# Define an input function so that species can vary which node-data files are generated/used
def node_data_files(wildcards):
    return [
        "results/{species}/branch_lengths.json",
        "results/{species}/muts.json",
        "results/{species}/sampling-year.json",
    ]

include: "generic.snakefile"

rule all:
    input:
        tree=expand("auspice/ebola_{species}.json", species=config['species']),

