"""Tests for bls_stats_aggregation.cli."""

from __future__ import annotations

from typer.testing import CliRunner

from bls_stats_aggregation.cli import app

runner = CliRunner()


class TestCesCli:
    def test_ces_exits_cleanly(self):
        result = runner.invoke(app, ["ces"])
        assert result.exit_code == 0
        assert "sector_code" in result.output


class TestSaeCli:
    def test_sae_not_implemented(self):
        result = runner.invoke(app, ["sae"])
        assert result.exit_code == 1


class TestBedCli:
    def test_bed_not_implemented(self):
        result = runner.invoke(app, ["bed"])
        assert result.exit_code == 1


class TestQcewCli:
    def test_qcew_missing_file(self):
        result = runner.invoke(app, ["qcew", "--bulk-path", "/nonexistent.parquet"])
        assert result.exit_code == 1


class TestJoltsCli:
    def test_jolts_missing_file(self):
        result = runner.invoke(app, ["jolts", "--jolts-path", "/nonexistent.parquet"])
        assert result.exit_code == 1
