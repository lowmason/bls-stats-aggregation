"""Map JOLTS data to CES industry groups.

Maps JOLTS (Job Openings and Labor Turnover Survey) data to the CES
industry hierarchy at the domain and supersector levels for national
and state geographies.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .industry import (
    JOLTS_DATA_ELEMENTS,
    JOLTS_TO_CES,
    _GOODS_SUPERSECTORS,
)


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


def map_jolts_to_ces(
    jolts_path: Path | str = "data/jolts.parquet",
) -> pl.DataFrame:
    """Map JOLTS parquet data to CES industry groups.

    Reads the filtered parquet, maps JOLTS industry codes to CES codes,
    and derives goods-producing (``06``) and private service-providing
    (``08``) domains from national supersector estimates.

    Args:
        jolts_path: Path to the JOLTS parquet file.

    Returns:
        Long-format DataFrame with columns ``geographic_type``,
        ``geographic_code``, ``industry_type``, ``industry_code``,
        ``rate_or_level``, ``data_element``, ``ref_date``, ``value``.
    """
    jolts_path = Path(jolts_path)
    if not jolts_path.exists():
        print(f"  JOLTS file not found: {jolts_path}")
        return pl.DataFrame()

    df = pl.read_parquet(jolts_path)

    # Map state_code → geographic_type / geographic_code
    df = df.with_columns(
        geographic_type=pl.when(pl.col("state_code") == "00")
        .then(pl.lit("national"))
        .otherwise(pl.lit("state")),
        geographic_code=pl.col("state_code"),
    ).drop("state_code")

    # Map industry_code → CES code and industry_type
    ces_map = pl.DataFrame(
        {
            "industry_code": list(JOLTS_TO_CES.keys()),
            "ces_code": [v[0] for v in JOLTS_TO_CES.values()],
            "industry_type": [v[1] for v in JOLTS_TO_CES.values()],
        }
    )
    df = (
        df.join(ces_map, on="industry_code", how="inner")
        .drop("industry_code")
        .rename({"ces_code": "industry_code"})
    )

    # Map dataelement_code → human-readable name
    de_map = pl.DataFrame(
        {
            "dataelement_code": list(JOLTS_DATA_ELEMENTS.keys()),
            "data_element": list(JOLTS_DATA_ELEMENTS.values()),
        }
    )
    df = df.join(de_map, on="dataelement_code", how="inner").drop("dataelement_code")

    # Map ratelevel_code → rate_or_level
    df = df.with_columns(
        rate_or_level=pl.when(pl.col("ratelevel_code") == "L")
        .then(pl.lit("level"))
        .otherwise(pl.lit("rate")),
    ).drop("ratelevel_code")

    output_cols = [
        "geographic_type",
        "geographic_code",
        "industry_type",
        "industry_code",
        "rate_or_level",
        "data_element",
        "ref_date",
        "value",
    ]
    mapped = df.select(output_cols)

    geo_cols = ["geographic_type", "geographic_code"]

    # ------------------------------------------------------------------
    # Derive domain 06 / 08 from national supersector data.
    # Supersector breakdowns are only published at the national level.
    # JOLTS rate = (level / employment) * 100  →  employment = level * 100 / rate
    # ------------------------------------------------------------------
    national = mapped.filter(pl.col("geographic_type") == "national")

    ss_levels = national.filter(
        (pl.col("industry_type") == "supersector")
        & (pl.col("rate_or_level") == "level")
    ).select(
        *geo_cols,
        "industry_code",
        "data_element",
        "ref_date",
        pl.col("value").alias("level_value"),
    )
    ss_rates = national.filter(
        (pl.col("industry_type") == "supersector") & (pl.col("rate_or_level") == "rate")
    ).select(
        *geo_cols,
        "industry_code",
        "data_element",
        "ref_date",
        pl.col("value").alias("rate_value"),
    )
    join_keys = [*geo_cols, "industry_code", "data_element", "ref_date"]
    ss_employment = ss_levels.join(ss_rates, on=join_keys, how="inner").with_columns(
        employment=(pl.col("level_value") * 100.0 / pl.col("rate_value")),
    )

    geo_group = [*geo_cols, "data_element", "ref_date"]

    # ------------------------------------------------------------------
    # Derive domain 06 (Goods-Producing) — levels and rates
    # ------------------------------------------------------------------
    goods_emp = ss_employment.filter(pl.col("industry_code").is_in(_GOODS_SUPERSECTORS))

    domain_06_levels = (
        goods_emp.group_by(geo_group)
        .agg(value=pl.col("level_value").sum())
        .with_columns(
            industry_type=pl.lit("domain"),
            industry_code=pl.lit("06"),
            rate_or_level=pl.lit("level"),
        )
        .select(output_cols)
    )

    domain_06_rates = (
        goods_emp.group_by(geo_group)
        .agg(
            level_sum=pl.col("level_value").sum(),
            emp_sum=pl.col("employment").sum(),
        )
        .with_columns(
            value=(pl.col("level_sum") / pl.col("emp_sum") * 100.0),
        )
        .drop("level_sum", "emp_sum")
        .with_columns(
            industry_type=pl.lit("domain"),
            industry_code=pl.lit("06"),
            rate_or_level=pl.lit("rate"),
        )
        .select(output_cols)
    )

    # ------------------------------------------------------------------
    # Derive domain 08 (Private Service-Providing) — levels and rates
    # Domain 08 = domain 05 (Total Private) - domain 06 (Goods-Producing)
    # ------------------------------------------------------------------
    d05_levels = national.filter(
        (pl.col("industry_code") == "05") & (pl.col("rate_or_level") == "level")
    ).select(*geo_cols, "data_element", "ref_date", pl.col("value").alias("d05_level"))

    d05_rates = national.filter(
        (pl.col("industry_code") == "05") & (pl.col("rate_or_level") == "rate")
    ).select(*geo_cols, "data_element", "ref_date", pl.col("value").alias("d05_rate"))

    d05_emp = d05_levels.join(d05_rates, on=geo_group, how="inner").with_columns(
        d05_employment=(pl.col("d05_level") * 100.0 / pl.col("d05_rate")),
    )

    d06_agg = goods_emp.group_by(geo_group).agg(
        d06_level=pl.col("level_value").sum(),
        d06_employment=pl.col("employment").sum(),
    )

    d08_calc = d05_emp.join(d06_agg, on=geo_group, how="inner").with_columns(
        d08_level=pl.col("d05_level") - pl.col("d06_level"),
        d08_employment=pl.col("d05_employment") - pl.col("d06_employment"),
    )

    domain_08_levels = (
        d08_calc.select(
            *geo_cols,
            "data_element",
            "ref_date",
            pl.col("d08_level").alias("value"),
        )
        .with_columns(
            industry_type=pl.lit("domain"),
            industry_code=pl.lit("08"),
            rate_or_level=pl.lit("level"),
        )
        .select(output_cols)
    )

    domain_08_rates = (
        d08_calc.select(
            *geo_cols,
            "data_element",
            "ref_date",
            (pl.col("d08_level") / pl.col("d08_employment") * 100.0).alias("value"),
        )
        .with_columns(
            industry_type=pl.lit("domain"),
            industry_code=pl.lit("08"),
            rate_or_level=pl.lit("rate"),
        )
        .select(output_cols)
    )

    combined = (
        pl
        .concat([
            mapped,
            domain_06_levels,
            domain_06_rates,
            domain_08_levels,
            domain_08_rates,
        ])
        .sort(
            'geographic_type', 'geographic_code',
            'industry_type', 'industry_code',
            'ref_date'
        )
        .rename({'rate_or_level': 'rate_level'})
        .with_columns(
            data_element=pl.col('data_element')
                           .replace({'hires': 'entries', 'total_separations': 'exits'})
        )
    )

    print(
        f"  jolts: {combined.height:,} rows across "
        f"{combined['industry_code'].n_unique()} industries, "
        f"{combined['geographic_code'].n_unique()} geographies",
    )
    return combined
