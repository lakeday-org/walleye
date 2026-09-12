import subprocess

from walleye.discovery import ScanOptions, discover


def test_git_discovery_counts_ignored_untracked_files(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text("ignored.py\n")
    (tmp_path / "ignored.py").write_text("x = 1\n")
    (tmp_path / "kept.py").write_text("x = 2\n")

    result = discover(tmp_path, ScanOptions())
    paths = {relative for _, relative, _ in result.files}

    assert result.method == "git-working-tree"
    assert "ignored.py" not in paths
    assert "kept.py" in paths
    assert result.skipped["gitignored"] == 1
