#!/bin/sh
#
# Common commands to set up Complement's prerequisites in a GitHub Actions CI run.
#
# Must be called after Synapse has been checked out to `synapse/`.
#
set -eu

# This is presentation-only: the runner keeps the raw Go JSONL log separate
# from its human-readable progress output, and CI filters that JSONL again
# before handing it to gotestfmt.
go install -v github.com/gotesttools/gotestfmt/v2/cmd/gotestfmt@latest
mkdir -p .gotestfmt/github
cp synapse/.ci/complement_package.gotpl .gotestfmt/github/package.gotpl

# Attempt to check out the same branch of Complement as the PR. If it
# doesn't exist, fallback to HEAD.
synapse/.ci/scripts/checkout_complement.sh
