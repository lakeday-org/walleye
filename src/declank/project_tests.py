"""Native project checks inside the caller-provided sandbox, with frozen JUnit cases."""

import json
import os
import signal
import subprocess
import xml.etree.ElementTree as ET

from .review import write_json
from .workflow import safe_path
from .workflow_validation import digest


def project_config(root):
    path = safe_path(root, ".declank.json")
    if path.exists():
        config = json.loads(path.read_text())
    elif (root / "uv.lock").exists() and (root / "pyproject.toml").exists():
        config = {
            "setup": [["uv", "sync", "--frozen"]],
            "checks": [["uv", "run", "pytest", "-q"]],
            "test_command": ["uv", "run", "pytest", "-q", "{test_file}", "--junitxml={report}"],
        }
    else:
        raise ValueError(
            "Native tests need .declank.json with setup, checks, and test_command (JUnit output)"
        )
    if (
        not isinstance(config, dict)
        or set(config) - {"setup", "checks", "test_command", "format"}
        or not {"setup", "checks", "test_command"}.issubset(config)
    ):
        raise ValueError(".declank.json needs setup, checks, test_command, and optional format")
    if not all(isinstance(config[k], list) for k in ("setup", "checks", "test_command")):
        raise ValueError("Project command settings must be arrays")
    commands = [*config["setup"], *config["checks"], config["test_command"]]
    if not config["checks"] or not all(
        isinstance(c, list) and c and all(isinstance(a, str) and a for a in c) for c in commands
    ):
        raise ValueError(
            "Project commands must be nonempty argument arrays; at least one check is required"
        )
    joined = " ".join(config["test_command"])
    if "{test_file}" not in joined or "{report}" not in joined:
        raise ValueError("test_command must include {test_file} and {report}")
    formatter = config.get("format")
    if formatter is not None and (
        not isinstance(formatter, list)
        or not formatter
        or not all(isinstance(a, str) and a for a in formatter)
        or "{file}" not in " ".join(formatter)
    ):
        raise ValueError("format must be an argument array containing {file}")
    return config


def execute(command, root, log, *, timeout=900):
    # Publishing and model credentials are not passed to repository commands.
    env = {
        k: v
        for k, v in os.environ.items()
        if k in {"PATH", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT"}
    }
    env.update(CI="true", UV_LINK_MODE="copy", PYTHONDONTWRITEBYTECODE="1")
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as stream:
        process = subprocess.Popen(
            command,
            cwd=root,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise ValueError(f"Project command timed out: {command[0]}; see {log}") from None
    return {
        "command": command,
        "exit_code": code,
        "log": str(log),
        "tail": log.read_text(errors="replace")[-12000:],
    }


class ProjectTests:
    def __init__(self, root, directory):
        self.root, self.directory = root, directory
        self.config = project_config(root)

    def format_file(self, path, label):
        if command := self.config.get("format"):
            result = execute(
                [a.replace("{file}", path) for a in command],
                self.root,
                self.directory / (label + ".log"),
            )
            if result["exit_code"]:
                raise ValueError("Project formatter failed; see " + result["log"])

    def commands(self, kind, label):
        results = []
        for number, command in enumerate(self.config[kind], 1):
            result = execute(command, self.root, self.directory / f"{label}-{number}.log")
            results.append(result)
            if result["exit_code"]:
                break
        write_json(self.directory / f"{label}.json", results)
        return results

    def frozen(self, path, label):
        report = self.directory / (label + ".xml")
        report.unlink(missing_ok=True)
        command = [
            a.replace("{test_file}", path).replace("{report}", str(report))
            for a in self.config["test_command"]
        ]
        result = execute(command, self.root, self.directory / (label + ".log"))
        try:
            document = ET.parse(report)
            cases = []
            for case in document.iter("testcase"):
                failure, error = case.find("failure"), case.find("error")
                cases.append(
                    {
                        "name": case.get("name", ""),
                        "passed": failure is None
                        and error is None
                        and case.find("skipped") is None,
                        "assertion_failure": failure is not None
                        and (
                            "assert" in failure.get("type", "").lower()
                            or failure.get("message", "")
                            .lstrip()
                            .startswith(("assert ", "AssertionError", "Failed: DID NOT RAISE"))
                        ),
                        "error": error is not None,
                        "skipped": case.find("skipped") is not None,
                    }
                )
            result["cases"] = cases
        except (OSError, ET.ParseError):
            raise ValueError(f"Test command did not produce valid JUnit XML: {report}") from None
        write_json(self.directory / (label + ".json"), result)
        return result


def checks_passed(results):
    return bool(results) and all(r["exit_code"] == 0 for r in results)


def reproduced(result, regression_tests, objective):
    cases = result["cases"]
    names = [c["name"] for c in cases]
    if not cases or len(names) != len(set(names)) or any(c["error"] or c["skipped"] for c in cases):
        return False
    if objective == "refactor":
        return not regression_tests and result["exit_code"] == 0 and all(c["passed"] for c in cases)
    if (
        not regression_tests
        or not set(regression_tests).issubset(names)
        or result["exit_code"] != 1
    ):
        return False
    return all(
        c["assertion_failure"] if c["name"] in regression_tests else c["passed"] for c in cases
    )


def freeze_test(root, plan):
    path = plan["test_path"]
    destination = safe_path(root, path)
    if destination.exists() or not path.startswith(("tests/", "test/", "__tests__/")):
        raise ValueError("Agent tests must create a new file under tests/, test/, or __tests__/")
    if destination.suffix not in {".py", ".js", ".ts", ".tsx", ".go", ".rs", ".java", ".cs"}:
        raise ValueError("Unsupported native test file extension")
    content = plan["test_content"].encode()
    if not content or len(content) > 100000:
        raise ValueError("Invalid native test content length")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return digest(content)


def test_unchanged(root, path, expected):
    if digest(safe_path(root, path).read_bytes()) != expected:
        raise ValueError("Frozen native tests changed")
