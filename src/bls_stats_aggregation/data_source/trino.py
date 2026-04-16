"""Trino data lake connector for BLS program data.

Provides ``TrinoSource`` for querying QCEW, JOLTS, CES, SAE, and BED
program data from a Trino data lake. Trino connectivity is implemented
in the data lake environment where the ``trino`` driver is available.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass
class TrinoSource:
    """Connection configuration for a Trino data lake.

    Attributes:
        host: Trino coordinator hostname.
        port: Trino coordinator port.
        catalog: Trino catalog name.
        schema: Trino schema name.
    """

    host: str = "localhost"
    port: int = 8080
    catalog: str = "hive"
    schema: str = "default"

    def read_qcew(
        self,
        start_year: int = 2003,
        end_year: int = 2025,
    ) -> pl.DataFrame:
        """Read QCEW bulk data from Trino.

        Expected output schema::

            area_fips:       Utf8   -- 'US000' or 5-digit state FIPS
            own_code:        Utf8   -- '0', '1', '2', '3', '5'
            industry_code:   Utf8   -- NAICS code
            agglvl_code:     Utf8   -- aggregation level
            year:            Utf8
            qtr:             Utf8
            month1_emplvl:   Utf8
            month2_emplvl:   Utf8
            month3_emplvl:   Utf8

        Args:
            start_year: First year to include.
            end_year: Last year to include.

        Returns:
            Filtered QCEW bulk data as a Polars DataFrame.

        Raises:
            NotImplementedError: Trino connectivity is not yet available.
        """
        raise NotImplementedError(
            "Trino connectivity is not available in this environment. "
            "This will be implemented in the data lake environment."
        )

    def read_jolts(self) -> pl.DataFrame:
        """Read JOLTS data from Trino.

        Expected output schema::

            state_code:        Utf8   -- '00' (national) or 2-digit FIPS
            industry_code:     Utf8   -- 6-digit JOLTS industry code
            dataelement_code:  Utf8   -- 'HI' or 'TS'
            ratelevel_code:    Utf8   -- 'L' or 'R'
            ref_date:          Date
            value:             Float64

        Returns:
            Filtered JOLTS data as a Polars DataFrame.

        Raises:
            NotImplementedError: Trino connectivity is not yet available.
        """
        raise NotImplementedError(
            "Trino connectivity is not available in this environment. "
            "This will be implemented in the data lake environment."
        )
