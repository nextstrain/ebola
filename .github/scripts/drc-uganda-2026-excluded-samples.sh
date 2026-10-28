#!/usr/bin/env bash
# Emit a GitHub Actions job summary (markdown) describing the samples
# excluded from the bdbv/drc-uganda-2026 phylogenetic build. STDOUT is
# intended to be redirected to "$GITHUB_STEP_SUMMARY".
#
# Usage: drc-uganda-2026-excluded-samples.sh [LOG_DIR]
# LOG_DIR defaults to the build's log directory relative to the repo root.
set -euo pipefail

log_dir="${1:-phylogenetic/logs/bdbv/drc-uganda-2026}"

subsample_log="${log_dir}/subsample.txt"
per_strain_log="${log_dir}/subsample-per-strain.tsv"
refine_log="${log_dir}/refine.txt"
refine_adar_log="${log_dir}/refine_adar_mutations_removed.txt"

# Print a file verbatim inside a fenced code block.
code_block() {
  local file="$1"
  echo '```'
  if [[ -f "$file" ]]; then
    cat "$file"
  else
    echo "(${file} not found)"
  fi
  echo '```'
}

# Print the lines of a file matching a fixed pattern inside a fenced code block.
grep_block() {
  local file="$1" pattern="$2"
  echo '```'
  if [[ -f "$file" ]]; then
    grep -F "$pattern" "$file" || echo "(no lines matching '${pattern}')"
  else
    echo "(${file} not found)"
  fi
  echo '```'
}

echo "## Samples excluded from bdbv/drc-uganda-2026"
echo
echo "### Subsampling"
echo
code_block "$subsample_log"
echo
echo '<details>'
echo '<summary>subsample-per-strain.tsv</summary>'
echo
code_block "$per_strain_log"
echo
echo '</details>'
echo
echo "### First refine step"
echo
grep_block "$refine_log" 'pruning leaf'
echo
echo "### Post-ADAR refine step"
echo
grep_block "$refine_adar_log" 'pruning leaf'
