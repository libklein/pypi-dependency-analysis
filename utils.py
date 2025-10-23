import polars as pl
import networkx as nx

def extract_dependencies(package_metadata: pl.DataFrame) -> pl.DataFrame:
    return package_metadata.explode("requires_dist").with_columns(
        requires_dist=pl.col.requires_dist.str.extract(r"([\w_-]+)", 1)
    )


def build_dependency_graph(dependency_graph_edges: pl.DataFrame) -> nx.DiGraph:
    dependency_graph = nx.DiGraph()

    for name, depends_on in dependency_graph_edges.select(
        "name", "requires_dist"
    ).iter_rows():
        if depends_on is None:
            dependency_graph.add_node(name)
        else:
            dependency_graph.add_edge(name, depends_on)

    return dependency_graph

