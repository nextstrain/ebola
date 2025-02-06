# Nextstrain repository for ebola virus

This repository contains three workflows for the analysis of ebola virus data:

- [`ingest/`](./ingest) - Download data from GenBank, clean and curate it
- [`phylogenetic/`](./phylogenetic) - Filter sequences, align, construct phylogeny and export for visualization
- [`nextclade/`](./nextclade) - Create nextclade datasets

Each folder contains a README.md with more information.

## Installation

Follow the [standard installation instructions](https://docs.nextstrain.org/en/latest/install.html) for Nextstrain's suite of software tools.

After you've installed the Nextstrain CLI, you can set up ebola with

   nextstrain setup ebola

## Quickstart

Run the default phylogenetic workflow via:

   nextstrain run ebola phylogenetic ebola-analysis
   nextstrain view ebola-analysis

## Documentation

- [Running a pathogen workflow](https://docs.nextstrain.org/en/latest/tutorials/running-a-workflow.html)

## Working on this repo

This repo is configured to use [pre-commit](https://pre-commit.com),
to help automatically catch common coding errors and syntax issues
with changes before they are committed to the repo.

If you will be writing new code or otherwise working within this repo,
please do the following to get started:

1. [install `pre-commit`](https://pre-commit.com/#install) by running
   either `python -m pip install pre-commit` or `brew install
   pre-commit`, depending on your preferred package management
   solution
2. install the local git hooks by running `pre-commit install` from
   the root of the repo
3. when problems are detected, correct them in your local working tree
   before committing them.

Note that these pre-commit checks are also run in a GitHub Action when
changes are pushed to GitHub, so correcting issues locally will
prevent extra cycles of correction.
