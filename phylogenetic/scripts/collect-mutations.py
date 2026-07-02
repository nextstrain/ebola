"""
Reads an `augur ancestral` node-data JSON and outputs a node-data JSON with
per-node counts of nucleotide mutations (`muts`) and, for each requested CDS,
the number of amino-acid mutations (`{cds}_mut_count`), as integer strings.

When `--counts` is supplied the raw counts are instead binned into the provided
inclusive ranges and the matching range label is emitted (e.g. "0", "1-5", "6+").
"""
import json
import argparse
import re


def parse_counts(spec):
    """Parse a comma-separated list of inclusive ranges into ordered
    (lo, hi, label) tuples. Each value is one of:
      - a single integer, e.g. "2"       -> [2, 2]
      - a closed range,   e.g. "1-5"     -> [1, 5]
      - an open range,    e.g. "6+"      -> [6, inf] (only allowed as the last value)
    The ranges must start at 0 and be contiguous (no holes).
    """
    ranges = []
    tokens = spec.split(',')
    for i, token in enumerate(tokens):
        if m := re.fullmatch(r'(\d+)', token):
            lo = hi = int(m.group(1))
        elif m := re.fullmatch(r'(\d+)-(\d+)', token):
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo > hi:
                raise ValueError(f"Range {token!r} has a start greater than its end")
        elif m := re.fullmatch(r'(\d+)\+', token):
            if i != len(tokens) - 1:
                raise ValueError(f"Open-ended range {token!r} is only allowed as the final value")
            lo, hi = int(m.group(1)), float('inf')
        else:
            raise ValueError(f"Invalid --counts value {token!r}")

        expected = 0 if not ranges else ranges[-1][1] + 1
        if lo != expected:
            raise ValueError(f"--counts has a hole: expected {token!r} to start at {expected}, got {lo}")

        ranges.append((lo, hi, token))
    return ranges


def bin_count(n, ranges):
    for lo, hi, label in ranges:
        if lo <= n <= hi:
            return label
    raise ValueError(f"Count {n} does not fall within any --counts range")


def count_mutations(nodes, cds, ranges):
    fmt = (lambda n: bin_count(n, ranges)) if ranges else str
    counts = {}
    for name, node in nodes.items():
        aa_muts = node.get('aa_muts', {})
        node_counts = {'nuc_mut_count': fmt(len(node.get('muts', [])))}
        for gene in cds or []:
            node_counts[f'{gene}_mut_count'] = fmt(len(aa_muts.get(gene, [])))
        counts[name] = node_counts
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--muts", required=True, help="Node Data JSON from `augur ancestral`")
    parser.add_argument("--cds", required=False, nargs="+", help="CDS/genes to count amino-acid mutations for")
    parser.add_argument("--counts", required=False,
                        help="Comma-separated inclusive ranges to bin counts into, e.g. '0,1-5,6+'")
    parser.add_argument("--output", required=True, help="Node Data JSON output")

    args = parser.parse_args()

    ranges = parse_counts(args.counts) if args.counts else None

    with open(args.muts) as fh:
        nodes = json.load(fh)['nodes']

    with open(args.output, 'w') as fh:
        json.dump({"nodes": count_mutations(nodes, args.cds, ranges)}, fh, indent=2)
