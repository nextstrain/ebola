# Pathogen Repo Guide

> [!TIP]
> Follow the Nextstrain tutorial series for [Creating a pathogen repository][]
> to use the pathogen repo guide as the starting point for a new pathogen repository.


This is a Nextstrain pathogen repository guide for setting up a pathogen
repo to hold the files necessary to run and maintain a Nextstrain pathogen build.

Using this guide will allow you to start with the general repository
and workflow organization that is expected of a Nextstrain maintained pathogen.
However, the workflows will require customizations to support your specific pathogen
and should not be expected to "just work".

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

[Creating a pathogen repository]: https://docs.nextstrain.org/en/latest/tutorials/creating-a-pathogen-repo/index.html
