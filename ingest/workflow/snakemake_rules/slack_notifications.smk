"""
This part of the workflow handles various Slack notifications.
Designed to be used internally by the Nextstrain team with hard-coded paths
to files on AWS S3.

All rules here require two environment variables:
    * SLACK_TOKEN
    * SLACK_CHANNELS
"""
import os
import sys

slack_envvars_defined = "SLACK_CHANNELS" in os.environ and "SLACK_TOKEN" in os.environ
if not slack_envvars_defined:
    print(
        "ERROR: Slack notifications require two environment variables: 'SLACK_CHANNELS' and 'SLACK_TOKEN'.",
        file=sys.stderr,
    )
    sys.exit(1)

S3_SRC = "s3://nextstrain-data/files/workflows/dengue"


rule notify_on_genbank_record_change:
    input:
        genbank_ndjson="data/genbank.ndjson",
    output:
        touch("data/notify/genbank-record-change.done"),
    params:
        s3_src=S3_SRC,
        notify_on_record_change_url="https://raw.githubusercontent.com/nextstrain/monkeypox/644d07ebe3fa5ded64d27d0964064fb722797c5d/ingest/bin/notify-on-record-change",
    shell:
        """
        if [[ ! -d bin ]]; then
          mkdir bin
        fi
        if [[ ! -f bin/notify-on-record-change ]]; then
          cd bin
          wget {params.notify_on_record_change_url}
          chmod 755
          cd ..
        fi

        ./bin/notify-on-record-change {input.genbank_ndjson} {params.s3_src:q}/genbank.ndjson.xz Genbank
        """


rule notify_on_metadata_diff:
    input:
        metadata="data/metadata.tsv",
    output:
        touch("data/notify/metadata-diff.done"),
    params:
        s3_src=S3_SRC,
        notify_on_diff_url = "https://raw.githubusercontent.com/nextstrain/monkeypox/644d07ebe3fa5ded64d27d0964064fb722797c5d/ingest/bin/notify-on-diff",
    shell:
        """
        if [[ ! -d bin ]]; then
          mkdir bin
        fi
        if [[ ! -f bin/notify-on-diff ]]; then
          cd bin
          wget {params.notify_on_diff_url}
          chmod 755
          cd ..
        fi
        ./bin/notify-on-diff {input.metadata} {params.s3_src:q}/metadata.tsv.gz
        """


onstart:
    shell("./bin/notify-on-job-start")


onerror:
    shell("./bin/notify-on-job-fail")
