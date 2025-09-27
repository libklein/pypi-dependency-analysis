"""
Dependency graph module for analyzing PyPI package dependencies.

This module provides functionality to create and manage dependency graphs
for Python packages based on PyPI metadata.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

import networkx as nx


class DependencyGraph:
    """
    A graph representation of package dependencies using NetworkX DiGraph.

    This class encapsulates a directed graph where nodes represent packages
    and edges represent dependency relationships. The graph is immutable
    after construction.
    """

    def __init__(self, network: nx.DiGraph) -> None:
        """
        Initialize a dependency graph with the provided network.

        Args:
            network: A NetworkX DiGraph representing the dependency relationships
        """
        self._network: nx.DiGraph = network

    def get_dependencies(self, package_name: str) -> List[str]:
        """
        Get the direct dependencies of a package.

        Args:
            package_name: The package to get dependencies for

        Returns:
            List of direct dependencies
        """
        return list(self._network.successors(package_name))

    def get_dependents(self, package_name: str) -> List[str]:
        """
        Get the packages that depend on this package.

        Args:
            package_name: The package to get dependents for

        Returns:
            List of packages that depend on this package
        """
        return list(self._network.predecessors(package_name))

    def has_package(self, package_name: str) -> bool:
        """
        Check if a package exists in the graph.

        Args:
            package_name: The package name to check

        Returns:
            True if the package exists in the graph
        """
        return self._network.has_node(package_name)

    def get_all_packages(self) -> List[str]:
        """
        Get all package names in the graph.

        Returns:
            List of all package names
        """
        return list(self._network.nodes())

    def package_count(self) -> int:
        """Get the total number of packages in the graph."""
        return self._network.number_of_nodes()

    def dependency_count(self) -> int:
        """Get the total number of dependency relationships in the graph."""
        return self._network.number_of_edges()


def _normalize_package_name(name: str) -> str:
    """
    Normalize package name by converting to lowercase and replacing
    underscores/hyphens with a consistent format.

    Args:
        name: The package name to normalize

    Returns:
        Normalized package name
    """
    return re.sub(r"[-_.]+", "-", name.lower())


def _extract_package_name_from_requirement(requirement: str) -> str:
    """
    Extract the package name from a requirement string, removing version constraints.

    Args:
        requirement: A requirement string like "requests>=2.0.0" or "numpy"

    Returns:
        The package name without version information
    """
    # Remove version specifiers and extras
    # Examples: "requests>=2.0.0", "numpy", "django[postgres]>=3.0"
    name = re.split(r"[<>=!;[]", requirement)[0].strip()
    return _normalize_package_name(name)


def _parse_metadata_file(metadata_path: Path) -> Optional[Dict[str, Any]]:
    """
    Parse a PyPI metadata JSON file.

    Args:
        metadata_path: Path to the metadata JSON file

    Returns:
        Dictionary containing parsed metadata, or None if parsing failed
    """
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError, UnicodeDecodeError):
        return None


def _extract_dependencies(metadata: Dict[str, Any]) -> Set[str]:
    """
    Extract normalized dependency names from package metadata.

    Args:
        metadata: Parsed metadata dictionary

    Returns:
        Set of normalized dependency names
    """
    requires_dist = metadata.get("info", {}).get("requires_dist")
    if not requires_dist:
        return set()

    dependencies = set()
    for requirement in requires_dist:
        if requirement and isinstance(requirement, str):
            dep_name = _extract_package_name_from_requirement(requirement)
            if dep_name:
                dependencies.add(dep_name)

    return dependencies


def create_dependency_graph(directory_path: Path) -> DependencyGraph:
    """
    Create a dependency graph by scanning a directory for PyPI metadata files.

    This function scans the given directory for JSON metadata files,
    extracts package information and dependencies, and builds a directed
    graph representing the dependency relationships.

    Args:
        directory_path: Path to directory containing PyPI metadata files

    Returns:
        DependencyGraph instance containing the parsed dependencies

    Raises:
        FileNotFoundError: If the directory doesn't exist
        ValueError: If the path is not a directory
    """
    if not directory_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")

    if not directory_path.is_dir():
        raise ValueError(f"directory_path is not a directory: {directory_path}")

    network = nx.DiGraph()

    for metadata_file in (directory_path / "web" / "json").iterdir():
        if not metadata_file.is_file():
            continue

        metadata = _parse_metadata_file(metadata_file)
        if not metadata:
            continue

        # Extract package info
        info = metadata.get("info", {})
        package_name = info.get("name")
        if not package_name:
            continue

        # Normalize package name
        normalized_name = _normalize_package_name(package_name)

        # Add package to graph
        network.add_node(normalized_name)

        # Extract and add dependencies
        dependencies = _extract_dependencies(metadata)
        for dependency in dependencies:
            # Add dependency as a node (if not already present)
            network.add_node(dependency)
            # Add edge: package depends on dependency
            network.add_edge(normalized_name, dependency)

    return DependencyGraph(network)

