import polars as pl
from google.cloud import bigquery
import typer
from typing import Annotated, Optional
from pathlib import Path


def fetch_package_metadata(
    output_path: Annotated[Path, typer.Argument(file_okay=True)],
    project_id: Annotated[Optional[str], typer.Option(help="GCP Project ID")] = None,
):
    query = """
        SELECT 
name, version, author, author_email, maintainer, maintainer_email, license, keywords, classifiers, platform, home_page, download_url, requires_python, requires, provides, obsoletes, requires_dist, provides_dist, obsoletes_dist, requires_external, project_urls, uploaded_via, upload_time, size, python_version
FROM `bigquery-public-data.pypi.distribution_metadata`
    """

    client = bigquery.Client(project=project_id)

    query_job = client.query(query)
    rows = query_job.result()

    pl.from_arrow(rows.to_arrow()).write_parquet(output_path)


if __name__ == "__main__":
    typer.run(fetch_package_metadata)
