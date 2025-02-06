# Shared

> **Warning**
> Please be aware of the multiple workflows that will be affected when editing files in this directory!

This is expected to be the directory that holds files that are shared across multiple workflows.

Instead of cross referencing between workflows when needing to share files,
just have all workflows use this top level directory. This allows us to be
abundantly clear that updating files in this `shared` directory will affect multiple workflows.

Potential files that could be in this directory:
- `clades.tsv` - clade definitions
- `exclude.txt` - list of sequences to exclude from builds
- `lat_longs.tsv` - location coordinates
- `mask.bed` - specific coordinates to mask in sequences
- `reference.fasta` - reference sequence
