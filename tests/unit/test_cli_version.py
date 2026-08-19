"""`plane --version` (v1.0.0 release identity — control_plane/version.py is
the single authoritative version source the CLI reads through)."""

from typer.testing import CliRunner

from cli.main import app

runner = CliRunner()


def test_version_flag_prints_portage_1_0_0():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "Portage" in result.stdout
    assert "1.0.0" in result.stdout


def test_no_args_still_shows_help_not_version():
    result = runner.invoke(app, [])
    assert "1.0.0" not in result.stdout
