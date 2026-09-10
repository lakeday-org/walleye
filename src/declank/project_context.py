"""Native test context: indexed project settings, production files, and test conventions."""

from pathlib import Path

from .review_context import encode, estimate_tokens
from .workflow import safe_path
from .workflow_validation import digest

CONFIG_FILES = (
    ".declank.json",
    "pyproject.toml",
    "pytest.ini",
    "package.json",
    "tsconfig.json",
    "Cargo.toml",
    "go.mod",
    "pom.xml",
)


def native_packet(packet, index, limit):
    index.tests.clear()
    index.test_coverage.update(parsed_files=0, failed_files=0, limited=False)
    index.index_tests({r["name"] for r in index.rows.values()})
    configs = []
    for name in CONFIG_FILES:
        path = safe_path(index.base, name)
        if not path.is_file() or path.stat().st_size > 100000:
            continue
        source = path.read_bytes()
        source.decode("utf-8")
        index.sources[name], index.hashes[name] = source, digest(source)
        configs.append(name)
    target = Path(packet["target"]["path"])
    tests = {path for matches in index.tests.values() for path, _ in matches}
    preferred_tests = sorted(tests, key=lambda p: (target.stem not in Path(p).stem, p))
    ordered = list(dict.fromkeys([*configs, *preferred_tests, *sorted(index.sources)]))
    existing = {r["id"] for r in packet["resources"]}
    omitted = 0
    for path in ordered:
        resource = index.resource(path)
        if resource["id"] not in existing:
            packet["resources"].append(resource)
            if estimate_tokens(encode(packet)) > limit * 0.65:
                packet["resources"].pop()
                omitted += 1
                continue
            existing.add(resource["id"])
        if path in configs or path in preferred_tests[:3]:
            excerpt = index.excerpt(
                resource,
                role="project configuration" if path in configs else "existing native tests",
            )
            packet["source"].append(excerpt)
            if estimate_tokens(encode(packet)) > limit * 0.7:
                packet["source"].pop()
    packet["native_context"] = {
        "omitted_resources": omitted,
        "meaning": "Test associations are context candidates, not measured coverage",
    }
    packet["context_budget"]["estimated_tokens"] = estimate_tokens(encode(packet))
    return packet
