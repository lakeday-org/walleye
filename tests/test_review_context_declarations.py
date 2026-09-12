import hashlib

import walleye.review_context as review_context


def test_declarations_cache_is_scoped_by_language(monkeypatch):
    path = "fixture.ts"
    source = b"interface Config {}\n"
    report = {
        "root": path,
        "source_hashes": {path: hashlib.sha256(source).hexdigest()},
        "records": [],
    }

    expected_typescript = review_context.SourceIndex(report, {path: source}).declarations(
        path, "typescript"
    )
    assert [item["name"] for item in expected_typescript[0]] == ["Config"]
    assert expected_typescript[1] == []

    languages = []
    real_syntax = review_context.syntax

    def traced_syntax(source_bytes, language):
        languages.append(language)
        return real_syntax(source_bytes, language)

    monkeypatch.setattr(review_context, "syntax", traced_syntax)
    index = review_context.SourceIndex(report, {path: source})

    javascript_result = index.declarations(path, "javascript")
    typescript_result = index.declarations(path, "typescript")
    cached_typescript_result = index.declarations(path, "typescript")

    assert javascript_result != expected_typescript
    assert typescript_result == expected_typescript
    assert cached_typescript_result == expected_typescript
    assert languages == ["javascript", "typescript"]
