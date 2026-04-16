"""CLI for BLS statistics aggregation.

Provides subcommands for each BLS program mapping pipeline.
"""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(
    name="bls-stats",
    help="Map BLS program data to CES industry groups.",
)


@app.command()
def qcew(
    bulk_path: Path = typer.Option(
        "data/qcew_bulk.parquet",
        help="Path to the QCEW bulk parquet file.",
    ),
    output: Path = typer.Option(
        "data/qcew_ces.parquet",
        help="Path to write the output parquet file.",
    ),
) -> None:
    """Map QCEW bulk data to CES industry groups."""
    from .qcew.mapping import map_bulk_to_ces

    result = map_bulk_to_ces(bulk_path)
    if result.height > 0:
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)
        typer.echo(f"Wrote {output} ({result.height:,} rows)")
    else:
        typer.echo("No data to write.", err=True)
        raise typer.Exit(code=1)


@app.command()
def jolts(
    jolts_path: Path = typer.Option(
        "data/jolts.parquet",
        help="Path to the JOLTS parquet file.",
    ),
    output: Path = typer.Option(
        "data/jolts_ces.parquet",
        help="Path to write the output parquet file.",
    ),
) -> None:
    """Map JOLTS data to CES industry groups."""
    from .jolts.mapping import map_jolts_to_ces

    result = map_jolts_to_ces(jolts_path)
    if result.height > 0:
        output.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output)
        typer.echo(f"Wrote {output} ({result.height:,} rows)")
    else:
        typer.echo("No data to write.", err=True)
        raise typer.Exit(code=1)


@app.command()
def ces() -> None:
    """Display the CES industry hierarchy."""
    from .ces.industry import INDUSTRY_HIERARCHY

    df = INDUSTRY_HIERARCHY.collect()
    typer.echo(str(df))


@app.command()
def sae() -> None:
    """Map SAE data to CES industry groups (not yet implemented)."""
    typer.echo("SAE mapping is not yet implemented.", err=True)
    raise typer.Exit(code=1)


@app.command()
def bed() -> None:
    """Map BED data to CES industry groups (not yet implemented)."""
    typer.echo("BED mapping is not yet implemented.", err=True)
    raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
