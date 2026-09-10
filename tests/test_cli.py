import csv
import io
import json

import pytest

from declank import __version__
from declank.cli import main


def test_json_is_sorted_complete_and_machine_readable(tmp_path, capsys):
    for index in range(25):
        (tmp_path / f"a{index}.py").write_text("x = " + " + ".join(["1"] * (index + 1)))
    assert main(["scan", str(tmp_path), "--format", "json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert len(report["records"]) == 25
    assert report["records"][0]["path"] == "a24.py"
    assert [r["rank"] for r in report["records"]] == list(range(1, 26))
    assert report["schema_version"] == 1


def test_thresholds_consider_records_outside_top(tmp_path, capsys):
    # Sort by SLOC but fail the larger-volume single-line expression hidden by --top.
    (tmp_path / "tall.py").write_text("x=1\n" * 10)
    (tmp_path / "wide.py").write_text("x=" + "+".join(str(i) for i in range(100)))
    status = main(
        [
            "scan",
            str(tmp_path),
            "--format",
            "json",
            "--sort",
            "sloc",
            "--top",
            "1",
            "--fail-above",
            "length=100",
        ]
    )
    report = json.loads(capsys.readouterr().out)
    assert status == 1
    assert report["records"][0]["path"] == "tall.py"
    assert report["threshold_breaches"][0]["path"] == "wide.py"


def test_csv_and_output_file(tmp_path, capsys):
    (tmp_path / "a,b.py").write_text("x=1")
    output = tmp_path / "reports" / "scan.csv"
    assert main(["scan", str(tmp_path), "--format", "csv", "-o", str(output)]) == 0
    assert capsys.readouterr().out == ""
    rows = list(csv.DictReader(io.StringIO(output.read_text())))
    assert rows[0]["path"] == "a,b.py"
    assert float(rows[0]["bugs"]) > 0


def test_partial_results_are_exported_but_exit_two(tmp_path, capsys):
    (tmp_path / "a.py").write_text("x=1")
    (tmp_path / "bad.py").write_text("def broken(")
    args = ["scan", str(tmp_path), "--format", "json"]
    assert main(args) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["complete"] is False
    assert len(report["records"]) == 1
    assert main([*args, "--allow-partial"]) == 0


def test_no_records_is_not_success_even_with_allow_partial(tmp_path, capsys):
    assert main(["scan", str(tmp_path), "--allow-partial"]) == 2
    assert "No analyzable records" in capsys.readouterr().err


def test_missing_path_is_a_clean_cli_error(tmp_path, capsys):
    assert main(["scan", str(tmp_path / "missing")]) == 2
    assert "Path does not exist" in capsys.readouterr().err


@pytest.mark.parametrize(
    "args",
    [
        ["--top", "-1"],
        ["--max-bytes", "0"],
        ["--fail-above", "bugs=nan"],
        ["--fail-above", "bugs=-1"],
        ["--language", "not-a-language"],
    ],
)
def test_invalid_options_exit_two(args):
    with pytest.raises(SystemExit) as error:
        main(["scan", *args])
    assert error.value.code == 2


def test_invalid_mapping_is_a_clean_error(capsys):
    assert main(["scan", "--map", ".x=wrong"]) == 2
    assert "Invalid mapping" in capsys.readouterr().err


def test_table_prints_paths_literally(tmp_path, capsys):
    (tmp_path / "[red].py").write_text("x=1")
    assert main(["scan", str(tmp_path)]) == 0
    assert "[red].py" in capsys.readouterr().out


def test_language_registry_export(capsys):
    assert main(["languages", "--all", "--format", "json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 173


@pytest.mark.parametrize("width", [60, 180])
@pytest.mark.parametrize(
    "filename,source",
    [
        ("app.py", "def f(x):\n if x: return x + 1\n return 0\n"),
        ("schema.sql", "SELECT 1 + 2;"),
    ],
)
def test_default_output_keeps_maintainability_and_all_halstead_metrics_visible(
    tmp_path, capsys, monkeypatch, width, filename, source
):
    monkeypatch.setenv("COLUMNS", str(width))
    (tmp_path / filename).write_text(source)
    assert main(["scan", str(tmp_path)]) == 0
    output = " ".join(capsys.readouterr().out.split())
    assert f"declank {__version__}" in output
    assert "Maintainability (MI):" in output
    for label in (
        "distinct operators=",
        "distinct operands=",
        "total operators=",
        "total operands=",
        "Vocabulary:",
        "Length:",
        "Calculated length:",
        "Volume:",
        "Difficulty:",
        "Effort:",
        "Time estimate:",
        "Delivered-bug estimate (B):",
    ):
        assert label in output
    assert "not bug probability" in output
    assert "Parsed with:" in output
    assert "--sort" not in output
