import argparse
import json
import sys

from lineage import DbtLineage, CsvLineage, prettify_critical_path
from print_dag import export


def main(input_args):
    graph = CsvLineage(
        "resources/csv/edges.csv",
        "resources/csv/runtimes.csv"
    )

    with open("resources/json/corrections.json") as corrections:
        graph.update_graph(json.load(corrections))

    path, subgraph = graph.find_critical_path(input_args.model)
    export(subgraph, path, "pdf")


def parse_args(input_args):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--model", type=str, help="Model to find the upstream critical path"
    )

    return parser.parse_args(input_args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main(args)
