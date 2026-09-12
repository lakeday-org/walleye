import os
import subprocess
import sys

from walleye.cli import main


def test_languages_returns_zero_when_stdout_closes(monkeypatch):
    class ClosedStdout:
        def write(self, _value):
            raise BrokenPipeError("stdout closed")

    monkeypatch.setattr(sys, "stdout", ClosedStdout())
    result = None
    try:
        result = main(["languages", "--all"])
    except BrokenPipeError:
        pass
    assert result == 0


def test_languages_process_exits_successfully_when_pipe_reader_is_closed():
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "walleye", "languages", "--all"],
            stdout=write_fd,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            timeout=20,
            check=False,
        )
    finally:
        os.close(write_fd)
    assert completed.returncode == 0, completed.stderr
    assert "Traceback" not in completed.stderr
