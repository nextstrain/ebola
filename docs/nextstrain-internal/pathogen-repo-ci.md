# Pathogen Repo CI

Setting up a CI GitHub Action workflow that automatically runs is extremely
helpful for catching any changes that break pathogen workflows.

The CI GH Action workflow is _not_ intended to check the validity of the results,
it just ensures that the pathogen workflow still runs to completion.

## Using `pathogen-repo-ci`

Nextstrain maintains a reusable [pathogen-repo-ci][] GitHub Action workflow that
can be used to create the CI workflow with minimum set up:

```yaml
name: CI

on:
  push:
    branches:
      - main
  pull_request:
  workflow_dispatch:
  # Routinely check that we continue to work in the face of external changes.
  schedule:
    # Every day at 18:37 UTC / 10:37 Seattle (winter) / 11:37 Seattle (summer)
    - cron: "37 18 * * *"

jobs:
  ci:
    uses: nextstrain/.github/.github/workflows/pathogen-repo-ci.yaml@master
```

### `pathogen-repo-ci` Requirements

> [!IMPORTANT]
> If your pathogen repo does not meet these requirements, please update the
> repo to conform to them to maintain uniformity of Nextstrain pathogen repos.

1. The pathogen repo must have a top level `nextstrain-pathogen.yaml` file.
2. To run the CI step for a workflow (ingest, nextclade, or phylogenetic), the
directory must contain a `build-configs/ci/config.yaml` config file.
3. At least one of the CI steps must be run or else the CI workflow will exit with an error.


## Pre-pathogen-repo-guide repos

> [!WARNING]
> This is mainly used by historical Nextstrain repos that have not been updated.
> Do not follow these instructions for new pathogen repos!

For pathogen repos that do _not_ follow the file structure of the pathogen-repo-guide
and use a top-level Snakefile, it is possible to set up a CI workflow with
the `pathogen-repo-ci@v0` GitHub Action workflow. See the [avian-flu CI workflow][]
as an example.


[avian-flu CI workflow]: https://github.com/nextstrain/avian-flu/blob/HEAD/.github/workflows/ci.yaml
[pathogen-repo-ci]: https://github.com/nextstrain/.github/blob/HEAD/.github/workflows/pathogen-repo-ci.yaml
