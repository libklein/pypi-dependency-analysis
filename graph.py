import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import networkx as nx
    from pathlib import Path
    from utils import build_dependency_graph, extract_dependencies
    return build_dependency_graph, extract_dependencies, nx, pl


@app.cell
def _(build_dependency_graph, extract_dependencies, nx, pl):
    def _() -> nx.DiGraph:
        # Latest version for now
        package_metadata = (
            pl.scan_parquet("./pypi-package-metadata.parquet")
            .filter(pl.col.upload_time == pl.col.upload_time.max().over("name"))
            .collect()
        ).pipe(extract_dependencies)

        return build_dependency_graph(package_metadata)


    dependency_graph = _()
    return (dependency_graph,)


@app.cell
def _(dependency_graph, nx):
    communities = nx.community.louvain_communities(dependency_graph)
    return (communities,)


@app.cell
def _(communities):
    communities
    return


if __name__ == "__main__":
    app.run()
