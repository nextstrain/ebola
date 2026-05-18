# Phylogenetic workflows for BDBV and SUDV

_Work in progress!_

Firstly run a local ingest build which fetches data from Pathoplexus:

```sh
# working directory: ingest
snakemake --cores 4 -pf results/{bdbv,sudv}/{sequences.fasta,metadata.tsv}
```

The run the phylo workflows:

```sh
# working directory: phylogenetic
snakemake --snakefile --cores 4 -pf species-workflows/bdbv.snakefile

snakemake --snakefile --cores 4 -pf species-workflows/sudv.snakefile
```



