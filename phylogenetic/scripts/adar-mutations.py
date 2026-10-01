import argparse
import json
from typing import Tuple

from Bio import Phylo, SeqIO
from Bio.Seq import Seq


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyse ADAR-associated mutations across a phylogeny.",
    )
    parser.add_argument(
        "--tree",
        required=True,
        help="Newick tree file.",
    )
    parser.add_argument(
        "--muts",
        required=True,
        help="Ancestral mutations node-data JSON (from `augur ancestral`).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output node data JSON",
    )
    parser.add_argument(
        "--alignment",
        help="FASTA alignment to mask ADAR clusters in (optional).",
    )
    parser.add_argument(
        "--output-alignment",
        help="Output FASTA alignment with ADAR clusters masked (optional).",
    )
    return parser.parse_args()

def parse_muts_json(node_data_json):

    muts = {}
    for name, data in node_data_json['nodes'].items():
        muts[name] = []
        for mut in data.get('muts', []):
            muts[name].append((mut[0].upper(), int(mut[1:-1]), mut[-1].upper()))
    return muts


def is_adar(mut):
    """ADAR-mediated A-to-I editing, observed as T>C in sequenced genomes"""
    return mut[0] == 'T' and mut[2] == 'C'

def adar_clusters(node_name, muts, inherited=None, max_gap=50, min_count=2):
    """Group ADAR-like (T>C) edits into clusters where each edit is within
    `max_gap` nt of the previous one, returning clusters of >= min_count.

    Mutations are relative to the parent, so `inherited` (the parent node's
    clusters) is merged with this node's `muts`:
      - a new T>C edit is added, extending a cluster if close enough;
      - a mutation to 'N' at an inherited edit's position is ignored, so the
        inherited edit is kept (the base is ambiguous, not reverted);
      - any other mutation at an inherited edit's position reverts it, dropping
        that edit before re-clustering.
    """
    # Inherited ADAR edits still present at this node, keyed by position.
    surviving = {}
    if inherited:
        for cluster in inherited:
            for edit in cluster:
                surviving[edit[1]] = edit
    inherited_edits = dict(surviving)

    new_edits = {}
    for m in muts:
        pos = m[1]
        if pos in surviving:
            if m[2] == 'N':
                continue  # ambiguous call, not a reversion: keep inherited edit
            del surviving[pos]  # reverted away from C: inherited edit is lost
        elif is_adar(m):
            new_edits[pos] = m

    edits = sorted({**surviving, **new_edits}.values(), key=lambda m: m[1])

    clusters, current = [], []
    for m in edits:
        if current and m[1] - current[-1][1] > max_gap:
            if len(current) >= min_count:
                clusters.append(current)
            current = []
        current.append(m)
    if len(current) >= min_count:
        clusters.append(current)

    # Report new ADAR edits (added at this node) that survived into a final
    # cluster, distinguishing an edit joining an inherited cluster ("extend")
    # from a cluster made entirely of this node's new edits ("new").
    inherited_pos = set(inherited_edits)
    msgs = []
    for cluster in clusters:
        new_in_cluster = [m for m in cluster if m[1] in new_edits]
        if not new_in_cluster:
            continue  # cluster inherited unchanged
        label = "extend" if inherited_pos & {m[1] for m in cluster} else "new"
        shown = new_in_cluster if label == "extend" else cluster
        msgs.append(f"{label}: " + " ".join(f"{m[0]}{m[1]}{m[2]}" for m in shown))
    if msgs:
        print(f"{node_name}\t" + "  ".join(msgs))

    return clusters

type Mutation = Tuple[str,int,str]

def node_data_fmt(c: list[list[Mutation]]):
    s = " | ".join([" ".join([f"{m[0]}{m[1]}{m[2]}" for m in run]) for run in c])
    return s

def mask_clusters(records, found_clusters):
    """Mask every ADAR cluster edit except the first to 'N', in place.

    Cluster positions are 1-based (augur convention). Each edit is asserted to
    be realised in the sequence (base is the edited base, or already an 'N')."""
    for record in records:
        if record.id not in found_clusters:
            continue
        seq = list(str(record.seq).upper())
        for cluster in found_clusters[record.id]:
            for m in cluster:
                base = seq[m[1] - 1]
                assert base in (m[2], 'N'), (
                    f"{record.id}: expected {m[2]} or N at pos {m[1]}, found {base}"
                )
            masked = already_n = 0
            for m in cluster[1:]:
                i = m[1] - 1
                if seq[i] == 'N':
                    already_n += 1
                else:
                    seq[i] = 'N'
                    masked += 1
            note = ""
            if already_n:
                verb = "was" if already_n == 1 else "were"
                plural = "an N" if already_n == 1 else "Ns"
                note = f" ({already_n} {verb} already {plural})"
            print(f"{record.id} - one cluster of n={len(cluster)}. "
                  f"Changed {masked} bases to Ns{note}.")
        record.seq = Seq("".join(seq))
    return records

def main():
    args = parse_args()

    tree = Phylo.read(args.tree, "newick")

    
    with open(args.muts) as fh:
        muts = parse_muts_json(json.load(fh))

    found_clusters: dict[str,list[list[Mutation]]] = {}
    max_gap=50
    min_count=3

    print(f"\nFinding clusters of ADAR edits. Criteria {min_count} or more C>T " +
        f"mutations, where each edit is within {max_gap}bp of the previous one." +
        "\n\n'new': new cluster detected\n'extend': addition to parent's cluster\n")

    def traverse(node, parent_clusters):
        clusters = adar_clusters(node.name, muts.get(node.name, []), inherited=parent_clusters, max_gap=max_gap, min_count=min_count)
        if len(clusters):
            found_clusters[node.name] = clusters
        for child in node.clades:
            traverse(child, clusters)

    traverse(tree.root, [])

    with open(args.output, 'w') as fh:
        output = {
            name: {
                "adar_script": node_data_fmt(c),
                "adar_script_bool": "yes"
            } for name,c in found_clusters.items()
        }
        json.dump({"nodes": output}, fh, indent=2)

    if args.alignment or args.output_alignment:
        if not (args.alignment and args.output_alignment):
            raise SystemExit(
                "--alignment and --output-alignment must be provided together"
            )
        print("\nMasking ADAR clusters in the alignment:\n")
        records = list(SeqIO.parse(args.alignment, "fasta"))
        mask_clusters(records, found_clusters)
        SeqIO.write(records, args.output_alignment, "fasta")

if __name__ == "__main__":
    main()
