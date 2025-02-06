# Automated pathogen workflows

GitHub Action workflows can be scheduled to run at a specific cadence which is
useful for automating pathogen workflows.

## Using `pathogen-repo-build`

Nextstrain maintains a reusable [pathogen-repo-build][] GH Action workflow that
can be used to set up automated pathogen workflows.

The [pathogen-repo-build][] is very flexible to allow running of a variety of
pathogen workflows. The main input option `run` allows the GH Action workflow
to run any shell command, usually `nextstrain build ...`. See the description
for other input options in the [pathogen-repo-build][] workflow.

### Set up AWS credentials

The [pathogen-repo-build][] is able to automatically generate short-lived AWS
credentials for a [list of supported pathogens][]. Please follow
[instructions in the infra repo][] to add a new pathogen to the list.

Once this is done, the GH Action workflow will have credentials to run `aws s3`
commands within the build runtime for a [specific list of S3 paths for the pathogen][]
and have the option to run jobs using the Nextstrain `aws-batch` runtime.


[instructions in the infra repo]: https://github.com/nextstrain/infra?tab=readme-ov-file#how-to-add-a-new-pathogen-repository-for-use-with-pathogen-repo-build
[list of supported pathogens]: https://github.com/nextstrain/infra/blob/main/env/production/locals.tf
[pathogen-repo-build]: https://github.com/nextstrain/.github/blob/HEAD/.github/workflows/pathogen-repo-build.yaml
[specific list of S3 paths for the pathogen]: https://github.com/nextstrain/infra/blob/main/env/production/aws-iam-policy-NextstrainPathogen%40.tf
