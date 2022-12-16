# nextstrain.org/ebola

[![Build Status](https://github.com/nextstrain/ebola/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/nextstrain/ebola/actions/workflows/ci.yaml)

This is the [Nextstrain](https://nextstrain.org/) build for Ebola, visible at
[nextstrain.org/ebola](https://nextstrain.org/ebola).

The build encompasses fetching data, preparing it for analysis, doing quality
control, performing analyses, and saving the results in a format suitable for
visualization (with [auspice][]).  This involves running components of
Nextstrain such as [fauna][] and [augur][].

All Ebola-specific steps and functionality for the Nextstrain pipeline should be
housed in this repository.


## Usage

### Provision input data

Input sequences and metadata can be retrieved from data.nextstrain.org

* [sequences.fasta.xz](https://data.nextstrain.org/files/workflows/ebola/test/sequences.fasta.zst)
* [metadata.tsv.zst](https://data.nextstrain.org/files/workflows/ebola/test/metadata.tsv.zst)

Note that these data are generously shared by many labs around the world.
If you analyze and plan to publish using these data, please contact these labs first.

Within the analysis pipeline, these data are fetched from data.nextstrain.org and written to `data/` with:

```bash
nextstrain build . data/sequences.fasta data/metadata.tsv
```

### Run analysis pipeline

Run pipeline to produce "overview" tree for `/ebola` with:

```bash
nextstrain build . 
```

### Visualize results

View results with:

```bash
nextstrain view auspice/
```


<!--

If you're unfamiliar with Nextstrain builds, you may want to follow our
[quickstart guide][] first and then come back here.

The easiest way to run this pathogen build is using the [Nextstrain
command-line tool][nextstrain-cli]:

    nextstrain build .

See the [nextstrain-cli README][] for how to install the `nextstrain` command.

Alternatively, you should be able to run the build using `snakemake` within an
suitably-configured local environment.  Details of setting that up are not yet
well-documented, but will be in the future.

Build output goes into the directories `data/`, `results/` and `auspice/`.

Once you've run the build, you can view the results in auspice:

    nextstrain view auspice/

-->

## Configuration

Configuration takes place entirely with the `Snakefile`. This can be read top-to-bottom, each rule
specifies its file inputs and output and also its parameters. There is little redirection and each
rule should be able to be reasoned with on its own.


### fauna / RethinkDB credentials

This build starts by pulling sequences from our live [fauna][] database (a RethinkDB instance). This
requires environment variables `RETHINK_HOST` and `RETHINK_AUTH_KEY` to be set.

If you don't have access to our database, you can run the build using the example data provided in
this repository.  Before running the build, copy the example sequences into the `data/` directory
like so:

    mkdir -p data/ cp example_data/ebola.fasta data/

[fauna]: https://github.com/nextstrain/fauna
[augur]: https://github.com/nextstrain/augur
[auspice]: https://github.com/nextstrain/auspice
[snakemake cli]: https://snakemake.readthedocs.io/en/stable/executable.html#all-options
[nextstrain-cli]: https://github.com/nextstrain/cli
[nextstrain-cli README]: https://github.com/nextstrain/cli/blob/master/README.md
[quickstart guide]: https://nextstrain.org/docs/getting-started/quickstart
