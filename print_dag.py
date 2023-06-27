from datetime import timedelta

from graphviz import Digraph
from networkx import DiGraph

from utils.utils import suppress_stdout_stderr

INTERVAL_IN_SECONDS = 1  # selected interval in the timeline
AVOID_OVERLAP = True  # True to improve visualisation


def export(
        nx_graph: DiGraph, longest_path_nodes: list = list(), format: str = "pdf"
) -> None:
    """
    Generate a graphviz representation of the networkx graph, highlighting
    the critical path, if any.
    """
    with suppress_stdout_stderr():
        graph = generate_graph(nx_graph, longest_path_nodes)
        graph.format = format
        graph.render("./output/export", view=True)
        print("📥 Lineage delivered")


def build_graph_properties(name: str) -> Digraph:
    """
    Define the basic properties of the graph
    Most of them can be played with to create prettier graph.
    Some important ones:
    * compound: we are using a compound (clusters) graph
    * rankdir: (L)eft (R)ight. We could do a (T)op (B)ottom too.
    * node shape as plaintext, we only want to see the text of the nodes
    """
    graph = Digraph("G", filename=name, strict=True)
    graph.attr(newrank="true")
    graph.attr(compound="true")
    graph.attr(overlap="false")
    graph.attr(pad="0.5")
    graph.attr(ranksep="equally")
    graph.attr(nodesep="0.5")
    graph.attr(rankdir="LR")
    graph.attr("node", shape="plaintext")

    return graph


def find_interval(timestamp, start_timeline) -> int:
    """
    Based on the absolute start_timeline, find the relative position
    of the timestamp in the timeline.
    """
    return int(timestamp - start_timeline.timestamp()) // INTERVAL_IN_SECONDS


def generate_graph(nx_graph: DiGraph, longest_path_nodes: list) -> Digraph:
    """
    Following the logic in
    https://stackoverflow.com/questions/61550137/using-graphviz-yed-to-produce-a-timeline-graph
    this method aims to create an aligned timeline graph as a form of a gantt chart.

    Each model will be represented as a cluster, and by using ranks
    and invisible nodes we can create a bar representing the duration
    of the model by time:

            ----- cluster_nodeA -----
      ---> | node_start         node | --->
            -------------------------
                T                T+1

    In this case, node_start will be aligned with the interval
    assigned for the start of the task, and node will be aligned
    with the interval of the end of the task. We can align
    nodes in graphviz by using ranks.

    If, either because of the duration of the task or the size
    of the intervals, the node_start and node fall in the same rank,
    we will skip the creation og the node_start node:

            - cluster_nodeA -
      ---> |      node       | --->
            -----------------
                   T
    This will prevent a cluster with two nodes stacked on top of each
    other, making the cluster wider.

    By making the nodes inside the cluster invisible, we will reduce
    the overlapping of nodes and lines.

    Edges will be referenced using the internal nodes in the cluster,
    so we can create downstream edges at the end (node) of the gantt
    bar per node, and upstream edges at the start of the cluster (node_start).
    """

    graph = build_graph_properties("graph")

    # Create the data grid, based on the earliet and latests tasks in the DAG
    start_timeline = min(
        [value["start_time"] for key, value in nx_graph.nodes().items()]
    )
    end_timeline = max([value["end_time"] for key, value in nx_graph.nodes().items()])

    # Build the different interval blocks based on INTERVAL_IN_SECONDS
    # This will create a timeline in our graph as follows:
    # start_timeline --> T --> T+1 --> T+2 --> ... -> end_timeline
    # We will use each interval to rank the models by start/end
    interval = start_timeline
    rank = 0

    while interval < end_timeline:
        graph.node(str(rank), label=(interval + timedelta(seconds=1)).strftime("%I:%M:%S"), fontsize="50pt")
        interval += timedelta(seconds=INTERVAL_IN_SECONDS)
        # this  will avoid creating an empty node
        # at the end of the timeline
        if interval < end_timeline:
            graph.edge(str(rank), str(rank + 1))
            rank = rank + 1

    for node, metadata in nx_graph.nodes().items():
        # find the alignments with respect to the start_time
        start_align = find_interval(metadata["start_time"].timestamp(), start_timeline)
        end_align = find_interval(metadata["end_time"].timestamp(), start_timeline)

        # Both the end and start of two task might overlap
        # sharing the same timeline block.
        # To improve visualisation we can move the end block by one,
        # but this is going to shift the whole runtimes by 1 block left
        if AVOID_OVERLAP:
            end_align = end_align - 1
            # avoid the end being longer than the start
            if end_align < start_align:
                end_align = start_align

        # clusters in graphviz are an extension of subgraphs
        # where the name of the node must start with cluster_
        with graph.subgraph(name="cluster_" + node) as c:
            c.attr(style="rounded,filled")
            if node in longest_path_nodes:
                c.attr(fillcolor="/set39/8")
            else:
                c.attr(fillcolor="/spectral3/3")

            node_start = node + "_start"
            node_end = node
            if start_align == end_align:
                node_start = node_end
            else:
                # we define this node invisible,
                # otherwise we will have the name of the node
                # appearing twice within the cluster
                c.node(node_end, style="invis")
                # Note: to define ranks between nodes,
                # we need to use subgraphs.
                # otherwise the ranks are not applied.
                with graph.subgraph() as s:
                    s.attr(rank="same")
                    s.node(str(end_align))
                    s.node(node_end)

            # with a subgraph we create the rank to
            # align the node with the timeline
            c.node(node_start, fontsize="24", label=node)
            with graph.subgraph() as s:
                s.attr(rank="same")
                s.node(str(start_align))
                s.node(node_start)

            # create all the edges
            for upstream in nx_graph.predecessors(node):
                graph.edge(
                    upstream,
                    node_start,
                    ltail="cluster_" + upstream,
                    lhead="cluster_" + node,
                )

    return graph
