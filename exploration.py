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
    import mixology as mx
    return nx, pl, px


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
def _(nx, package_metadata, pl):
    def extract_dependencies(package_metadata: pl.DataFrame) -> pl.DataFrame:
        return package_metadata.explode("requires_dist").with_columns(
            requires_dist=pl.col.requires_dist.str.extract(r"([\w_-]+)", 1)
        )


    def build_dependency_graph(dependency_graph_edges: pl.DataFrame):
        dependency_graph = nx.DiGraph()

        for name, depends_on in dependency_graph_edges.select(
            "name", "requires_dist"
        ).iter_rows():
            if depends_on is None:
                dependency_graph.add_node(name)
            else:
                dependency_graph.add_edge(name, depends_on)

        return dependency_graph


    dependency_graph = build_dependency_graph(
        package_metadata.pipe(extract_dependencies)
    )
    required_by_graph = dependency_graph.reverse(copy=True)
    return dependency_graph, required_by_graph


@app.cell
def _(dependency_graph, nx, package_metadata, pl, required_by_graph):
    def resolve_dependency(dependency_graph: nx.DiGraph, package_name: str):
        # Reachability search, just depth/breadth first search
        return [v for _, v in nx.bfs_edges(dependency_graph, package_name)]


    transitive_dependencies = package_metadata.select(
        "name",
        depends_on=pl.col.name.map_elements(
            lambda x: resolve_dependency(dependency_graph, x),
            return_dtype=pl.List(pl.String),
        ),
    )
    transitive_requirements = package_metadata.select(
        "name",
        depends_on=pl.col.name.map_elements(
            lambda x: resolve_dependency(required_by_graph, x),
            return_dtype=pl.List(pl.String),
        ),
    )
    return transitive_dependencies, transitive_requirements


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
                num_packages_that_require=pl.col.depends_on.list.len()
            ),
            x="num_packages_that_require",
        )
        plt.update_yaxes(type='log')
        return plt


    _()
    return


@app.cell(hide_code=True)
def _(pl, px, transitive_requirements):
    px.bar(transitive_requirements.with_columns(
        num_packages_that_require=pl.col.depends_on.list.len()
    ).sort('num_packages_that_require', descending=False).tail(100),
        x='num_packages_that_require',
        y='name',
        orientation='h', 
        height=2000
    )
    return


@app.cell
def _(package_m):
    package_m
    return


@app.cell
def _():
    from semver import Version
    from semver import VersionRange
    from semver import parse_constraint

    from mixology.constraint import Constraint
    from mixology.package_source import PackageSource as BasePackageSource
    from mixology.range import Range
    from mixology.union import Union


    class Dependency:
        def __init__(self, name, constraint):  # type: (str, str) -> None
            self.name = name
            self.constraint = parse_constraint(constraint)
            self.pretty_constraint = constraint

        def __str__(self):  # type: () -> str
            return self.pretty_constraint


    class PackageSource(BasePackageSource):
        def __init__(self):  # type: () -> None
            self._root_version = Version.parse("0.0.0")
            self._root_dependencies = []
            self._packages = {}

            super(PackageSource, self).__init__()

        @property
        def root_version(self):
            return self._root_version

        def add(self, name, version, deps=None):  # type: (str, str, Optional[Dict[str, str]]) -> None
            if deps is None:
                deps = {}

            version = Version.parse(version)
            if name not in self._packages:
                self._packages[name] = {}

            if version in self._packages[name]:
                raise ValueError("{} ({}) already exists".format(name, version))

            dependencies = []
            for dep_name, spec in deps.items():
                dependencies.append(Dependency(dep_name, spec))

            self._packages[name][version] = dependencies

        def root_dep(self, name, constraint):  # type: (str, str) -> None
            self._root_dependencies.append(Dependency(name, constraint))

        def _versions_for(self, package, constraint=None):  # type: (Hashable, Any) -> List[Hashable]
            if package not in self._packages:
                return []

            versions = []
            for version in self._packages[package].keys():
                if not constraint or constraint.allows_any(
                    Range(version, version, True, True)
                ):
                    versions.append(version)

            return sorted(versions, reverse=True)

        def dependencies_for(self, package, version):  # type: (Hashable, Any) -> List[Any]
            if package == self.root:
                return self._root_dependencies

            return self._packages[package][version]

        def convert_dependency(self, dependency):  # type: (Dependency) -> Constraint
            if isinstance(dependency.constraint, VersionRange):
                constraint = Range(
                    dependency.constraint.min,
                    dependency.constraint.max,
                    dependency.constraint.include_min,
                    dependency.constraint.include_max,
                    dependency.pretty_constraint,
                )
            else:
                # VersionUnion
                ranges = [
                    Range(
                        range.min,
                        range.max,
                        range.include_min,
                        range.include_max,
                        str(range),
                    )
                    for range in dependency.constraint.ranges
                ]
                constraint = Union.of(ranges)

            return Constraint(dependency.name, constraint)
    return (PackageSource,)


@app.cell
def _(PackageSource, package_metadata):
    def _():
        source = PackageSource()

        for package in package_metadata.iter_rows(named=True):
            name = package["name"]
            version = package["version"]
            requires = package["requires_dist"] or {}
            source.add(name, version, requires)

        source.add("a", "1.0.0", deps={"shared": ">=2.0.0 <4.0.0"})
        source.add("b", "1.0.0", deps={"shared": ">=3.0.0 <5.0.0"})
        source.add("shared", "2.0.0")
        source.add("shared", "3.0.0")
        source.add("shared", "3.6.9")
        source.add("shared", "4.0.0")
        source.add("shared", "5.0.0")
    return


if __name__ == "__main__":
    app.run()
