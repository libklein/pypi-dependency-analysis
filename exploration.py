import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pathlib import Path
    import networkx as nx
    import polars as pl
    import plotly.express as px
    from utils import build_dependency_graph, extract_dependencies
    return build_dependency_graph, extract_dependencies, mo, nx, pl, px


@app.cell
def _(pl):
    # Latest version for now
    package_metadata = (
        pl.scan_parquet("./pypi-package-metadata.parquet")
        .filter(pl.col.upload_time == pl.col.upload_time.max().over("name"))
        .collect()
    )
    return (package_metadata,)


@app.cell
def _(package_metadata):
    package_metadata.select("size")
    return


@app.cell(hide_code=True)
def _(mo, package_metadata):
    mo.md(rf"""Total of {package_metadata.height} packages.""")
    return


@app.cell
def _(build_dependency_graph, extract_dependencies, package_metadata):
    # Edge represents "depends on" relationship
    dependency_graph = build_dependency_graph(
        package_metadata.pipe(extract_dependencies)
    )
    # Edge represents "required by" relationship
    required_by_graph = dependency_graph.reverse(copy=True)
    return dependency_graph, required_by_graph


@app.cell
def _(dependency_graph, nx, package_metadata, pl, required_by_graph):
    def resolve_dependency(dependency_graph: nx.DiGraph, package_name: str):
        # Reachability search, just depth/breadth first search
        return [v for _, v in nx.bfs_edges(dependency_graph, package_name)]


    # Package name, list of (transitive) dependencies
    transitive_dependencies = (
        package_metadata.select(
            "name",
            "size",
            depends_on=pl.col.name.map_elements(
                lambda x: resolve_dependency(dependency_graph, x),
                return_dtype=pl.List(pl.String),
            ),
        )
        .explode("depends_on")
        .join(
            package_metadata.select("name", dependency_size="size"),
            left_on="depends_on",
            right_on="name",
            how="left",
            validate="m:1",
        )
        .group_by("name", "size")
        .agg(depends_on=pl.col.depends_on, total_size=pl.col.dependency_size)
        .with_columns(total_size=pl.col.size + pl.col.total_size.list.sum())
    )
    # Package name, transitive list of packages that require this package
    transitive_requirements = package_metadata.select(
        "name",
        provides_for=pl.col.name.map_elements(
            lambda x: resolve_dependency(required_by_graph, x),
            return_dtype=pl.List(pl.String),
        ),
    )

    transitive_package_metadata = transitive_dependencies.select(
        "name", "total_size", num_requirements=pl.col.depends_on.list.len()
    ).join(
        transitive_requirements.select(
            "name", num_provides_for=pl.col.provides_for.list.len()
        ),
        on="name",
        how="inner",
        validate="1:1",
    )
    return (
        transitive_dependencies,
        transitive_package_metadata,
        transitive_requirements,
    )


@app.cell(hide_code=True)
def _(pl, px, transitive_dependencies):
    def _():
        plt = px.histogram(
            transitive_dependencies.with_columns(
                num_dependencies=pl.col.depends_on.list.len()
            ),
            x="num_dependencies",
        )
        return plt


    _()
    return


@app.cell(hide_code=True)
def _(pl, px, transitive_requirements):
    def _():
        plt = px.histogram(
            transitive_requirements.with_columns(
                num_packages_that_require=pl.col.provides_for.list.len()
            ),
            x="num_packages_that_require",
        )
        plt.update_yaxes(type="log")
        return plt


    _()
    return


@app.cell(hide_code=True)
def _(pl, px, transitive_requirements):
    px.bar(
        transitive_requirements.with_columns(
            num_packages_that_require=pl.col.provides_for.list.len()
        )
        .sort("num_packages_that_require", descending=False)
        .tail(100),
        x="num_packages_that_require",
        y="name",
        orientation="h",
        height=2000,
        title="Most depended-upon packages",
    )
    return


@app.cell
def _(pl, px, transitive_dependencies):
    px.bar(
        transitive_dependencies.with_columns(
            num_dependencies=pl.col.depends_on.list.len()
        )
        .sort("num_dependencies", descending=False)
        .tail(100),
        x="num_dependencies",
        y="name",
        orientation="h",
        height=2000,
        title="Packages by number of dependencies",
    )
    return


@app.cell
def _(px, transitive_package_metadata):
    px.scatter(
        transitive_package_metadata,
        x="num_requirements",
        y="num_provides_for",
        title="Requirements vs Provides",
        hover_data=["name"],
    )
    return


@app.cell
def _(transitive_package_metadata):
    transitive_package_metadata
    return


if __name__ == "__main__":
    app.run()
