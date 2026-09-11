"""Compare the same source weights, including helpers introduced by a patch."""

from collections import defaultdict

RULE = "Original source-line weights held fixed; new and removed helpers grouped with the target"
FIELDS = {"score": "risk_score", "mi": "maintainability_index", "control": "complexity_score"}


def _groups(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row["path"], row.get("qualified_name", row["name"])].append(row)
    return groups


def _weight(rows):
    return sum(max(1, row["sloc"]) for row in rows)


def _mean(rows, field):
    if not rows or any(row.get(field) is None for row in rows):
        return None
    total = sum(row[field] * max(1, row["sloc"]) for row in rows)
    value = total / _weight(rows)
    return 100 - value if field == "risk_score" else value


def paired_totals(before, after, target):
    """Return weighted totals using baseline weights on both sides.

    Existing functions retain their original influence when they shrink or grow.
    New helpers cannot enter as free, high-scoring units: they share the target's
    original weight, together with removed/renamed functions from the same file.
    """
    old, new = _groups(before), _groups(after)
    target_key = (target["path"], target["name"])
    common = (old.keys() & new.keys()) - {target_key}
    pairs = [(old[key], new[key]) for key in sorted(common)]
    unmatched = (old.keys() | new.keys()) - common
    if any(key[0] != target["path"] for key in unmatched):
        raise ValueError("Function identities changed outside the assigned source")
    pairs.append(
        (
            [row for key in sorted(unmatched) for row in old.get(key, [])],
            [row for key in sorted(unmatched) for row in new.get(key, [])],
        )
    )
    totals = {}
    for metric, field in FIELDS.items():
        values = [(_weight(left), _mean(left, field), _mean(right, field)) for left, right in pairs]
        if any(left is None or right is None for _, left, right in values):
            totals[metric] = (None, None)
        else:
            totals[metric] = (
                sum(weight * left for weight, left, _ in values),
                sum(weight * right for weight, _, right in values),
            )
    return totals, _weight(before)


def paired_quality(before, after, target):
    totals, weight = paired_totals(before, after, target)
    return tuple(
        round(value / weight, 4) if value is not None and weight else None
        for value in totals["score"]
    )
