from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime

import networkx as nx
from dbt_artifacts_parser.parser import parse_manifest_v7, parse_run_results_v4
from networkx import DiGraph
from tabulate import tabulate


def prettify_critical_path(path: list(), graph: DiGraph):
    output = list()
    for entity in path:
        output.append(
            [
                entity,
                graph.nodes[entity]["start_time"],
                graph.nodes[entity]["end_time"],
                graph.nodes[entity]["weight"],
                graph.nodes[entity]["potential_optimisation"],
                graph.nodes[entity]["next_longest_path"],
            ]
        )

    print(tabulate(output, headers=["entity","start_time","end_time","duration","potential_optimisation","next_longest_path"]))

@dataclass
class Lineage(object):
    """
    Main class that represents the DAG as a networkx graph.
    If the input graph is not a DAG this class will fail at
    creation time.
    """

    def __init__(self, nodes: dict, edges: list[tuple[str, str]]):
        """
        Builds a lineage class.
        1. List of nodes: dict
            Each node should contain the following information:
            key: unique_id
            vales: a dict with the following info:
                {
                    "start_time"=datetime,
                    "end_time"=datetime
                    "weight"=float
                }
        2. List of edges: list(Tuple(str,str))
        The list of edges should contain the same unique_ids as the nodes
        """
        self._build_digraph(nodes, edges)

    def _build_digraph(self, nodes: dict, edges: list):
        """
        Creates a weighted DAG. The weight of the edges is based
        on the duration of the source task of each edge.
        For example, if TaskA takes 10 minutes to run,
        and there's an edge defined such as (TaskA,TaskB),
        the edge will have 10minutes weight.
        """
        self.nx_graph = nx.DiGraph()

        for key, metadata in nodes.items():
            self.nx_graph.add_node(
                key,
                start_time=metadata["start_time"],
                end_time=metadata["end_time"],
                weight=metadata["weight"],
            )

        for edge in edges:
            self.nx_graph.add_edge(
                edge[0],
                edge[1],
                weight=self.nx_graph.nodes[edge[0]]["weight"],
            )

        if not nx.is_directed_acyclic_graph(self.nx_graph):
            raise Exception(
                "After its creation, the current dependency graph is not a DAG"
            )

    def update_graph(self, corrections: dict):
        """
        Re-builds the networkx DAG based on the
        changes made in corrections.json
        """
        if "nodes_delete" in corrections:
            for node_to_remove in corrections["nodes_delete"]:
                self.nx_graph.remove_node(
                    node_to_remove["task_id"],
                )
        if "nodes_upster" in corrections:
            for node_to_add in corrections["nodes_upster"]:
                start_datetime = datetime.strptime(node_to_add.get("task_start_ts").strip(), '%Y-%m-%dT%H:%M:%S')
                end_datetime = datetime.strptime(node_to_add["task_end_ts"].strip(), '%Y-%m-%dT%H:%M:%S')
                weight = (end_datetime - start_datetime).total_seconds()
                self.nx_graph.add_node(
                    node_to_add["task_id"],
                    start_time=start_datetime,
                    end_time=end_datetime,
                    weight=weight,
                )

        if "edges_delete" in corrections:
            for edge_to_delete in corrections["edges_delete"]:
                if edge_to_delete["source"] and edge_to_delete["target"]:
                    self.nx_graph.remove_edge(
                        edge_to_delete["source"], edge_to_delete["target"]
                    )

        if "edges_add" in corrections:
            for edge_to_add in corrections["edges_add"]:
                if edge_to_add["source"] and edge_to_add["target"]:
                    self.nx_graph.add_edge(edge_to_add["source"], edge_to_add["target"])

        for edge in self.nx_graph.edges:
            self.nx_graph[edge[0]][edge[1]]["weight"] = float(
                self.nx_graph.nodes[edge[0]]["weight"]
            )
        if not nx.is_directed_acyclic_graph(self.nx_graph):
            raise Exception(
                "After the corrections, the current dependency graph is not a DAG"
            )

    def find_critical_path(self, task: str) -> tuple[list[str], DiGraph]:
        """
        Finds the critical (longest) to a node based on the weight of the graph
        To reduce the complexity of the problem of finding the longest path to a node,
        we reduce the graph to only have the desired node to explore, and all its upstreams.
        """
        subgraph_nodes = nx.ancestors(self.nx_graph, task)
        subgraph_nodes.add(task)
        subgraph = self.nx_graph.subgraph(subgraph_nodes)
        return nx.dag_longest_path(subgraph, weight="weight"), subgraph

    def find_potential_optimisations(self, task: str) -> tuple[list[str], DiGraph]:
        """ This is a very hacky method that tries to find the what's
        the next critical path if there were any optimisations in the graph
        """
        longest_path_nodes, subgraph = self.find_critical_path(task)

        total = 0
        for node in longest_path_nodes:
            duration = subgraph.nodes[node]["weight"]
            total = total + duration

        for current_task in longest_path_nodes:
            subtotal = 0
            optimised_subgraph = subgraph.copy()
            optimised_subgraph.nodes[current_task]["weight"] = 0
            downstream_dependencies_one = optimised_subgraph.successors(current_task)

            for downstream_dependency in downstream_dependencies_one:
                optimised_subgraph[current_task][downstream_dependency]["weight"] = 0

            longest_subpath_optimised_nodes = nx.dag_longest_path(
                optimised_subgraph, weight="weight"
            )

            for optimised_node in longest_subpath_optimised_nodes:
                duration = optimised_subgraph.nodes[optimised_node]["weight"]
                subtotal = subtotal + duration

            # In some cases we might change to 0 the weight of an edge to the last node (task)
            # that we are trying to optimise, this means that the longest_path is now only considering
            # the previous task instead of the last task of the graph.
            # For that reason, we have to correct this by adding the time of the last task to the subtotal.
            # not ideal but. FIXME
            if task not in longest_subpath_optimised_nodes:
                subtotal = subtotal + optimised_subgraph.nodes[task]["weight"]

            subgraph.nodes[current_task]["potential_optimisation"] = total - subtotal
            subgraph.nodes[current_task][
                "next_longest_path"
            ] = longest_subpath_optimised_nodes

        return longest_path_nodes, subgraph


@dataclass
class DbtLineage(Lineage):
    """
    Builds a networkx lineage graph based on the dbt artefacts.
    The current implementation considers a manifest v7 and run_results v4,
    but this can be modified.
    """

    def __init__(self, manifest_path: str, run_results_path: str):

        with open(manifest_path) as manifest, open(run_results_path) as rul_results:
            # define the right versions of the manifest and run_results here.
            manifest_obj = parse_manifest_v7(manifest=json.load(manifest))
            run_results_obj = parse_run_results_v4(run_results=json.load(rul_results))

            # We need to join both artefacts to extract:
            # 1- list of nodes and their runtimes (manifest + rul_results)
            # 2- list of edges (manifest)

            # 1. list of nodes and their runtimes (manifest + rul_results)
            times = {result.unique_id: result for result in run_results_obj.results}
            # TODO: Although tests could be a bottleneck too, they are ommited in this step
            # A solution could be to either aggregate the times of tests into a single
            # node, or consider them as separate nodes too. This really depends if the
            node_list = [
                v.unique_id
                for k, v in manifest_obj.nodes.items()
                if v.resource_type.name in ("model", "seed", "source")
            ]
            nodes = {}
            for node in node_list:
                node_times = times[node]

                started_at = None
                completed_at = None
                for timing in node_times.timing:
                    if timing.name == "execute":
                        started_at = timing.started_at
                        completed_at = timing.completed_at
                if started_at and completed_at:
                    weight = (completed_at - started_at).total_seconds()
                else:
                    raise Exception(
                        f"The start/end of {node} is missing and the weight couldn't be calculated"
                    )
                nodes[node] = {
                    "start_time": started_at,
                    "end_time": completed_at,
                    "weight": weight,
                }
            # 2. list of nodes and their runtimes (manifest + rul_results)
            edges = [
                (source, target)
                for target, v in manifest_obj.nodes.items()
                for source in v.depends_on.nodes
            ]

        super(DbtLineage, self).__init__(nodes, edges)


@dataclass
class CsvLineage(Lineage):
    """
    Builds a networkx lineage graph based on the dbt artefacts.
    The current implementation considers a manifest v7 and run_results v4,
    but this can be modified.
    """

    def __init__(self, manifest_path: str, run_results_path: str):
        with open(manifest_path) as manifest, open(run_results_path) as run_results:
            manifest_obj = csv.reader(manifest, delimiter=',', quotechar='|')

            runtimes_obj = csv.reader(run_results, delimiter=',', quotechar='|')

            next(manifest_obj)  # skip header
            edges = [
                (row[0].strip(), row[1].strip())
                for row in manifest_obj

            ]

            nodes = {}
            next(runtimes_obj)  # skip header
            for row in runtimes_obj:
                start_datetime = datetime.strptime(row[1].strip(), '%Y-%m-%dT%H:%M:%S')
                end_datetime = datetime.strptime(row[2].strip(), '%Y-%m-%dT%H:%M:%S')
                nodes[row[0].strip()] = {
                    "start_time": start_datetime,
                    "end_time": end_datetime,
                    "weight": (end_datetime - start_datetime).total_seconds(),
                }

        super(CsvLineage, self).__init__(nodes, edges)