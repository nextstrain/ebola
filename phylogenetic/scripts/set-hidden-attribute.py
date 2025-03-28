#!/usr/bin/env python3

import argparse
import json


def main():
    parser = argparse.ArgumentParser(
        description="Mark certain nodes with a hidden flag."
    )
    parser.add_argument("node_attrs_in", help="Path to a node attributes file.")
    parser.add_argument("node_attrs_out", help="Path for the output node attributes file.")

    args = parser.parse_args()
    process_json(args.node_attrs_in, args.node_attrs_out)


def process_json(node_attrs_in, node_attrs_out):
    with open(node_attrs_in, "r") as f:
        data = json.load(f)

    nodes_in = data.get("nodes", {})
    nodes_out = {}

    for node_id, node_info in nodes_in.items():
        # Hide if the node has no clade info.
        if node_info.get("clade_membership") == "unassigned":
            nodes_out[node_id] = {"hidden": "timetree"}
            continue

    with open(node_attrs_out, "w") as f:
        json.dump({"nodes": nodes_out}, f, indent=2)


if __name__ == "__main__":
    main()
