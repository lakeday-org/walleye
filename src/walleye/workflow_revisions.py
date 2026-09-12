"""Keep the best candidate and stop spending when revisions make no progress."""

NO_PROGRESS_LIMIT = 3


def _measurements(card):
    if not card:
        return {}
    metrics = card["maintainability"]
    return {
        "target_quality": metrics["target_quality"],
        "region_quality": metrics["region"]["quality"],
        "repository_quality": metrics["repository"]["score"],
    }


def _feedback_metrics(metrics):
    result = dict(metrics)
    if "checks" in result:
        result["checks"] = [
            {
                key: value
                for key, value in check.items()
                if key in {"command", "exit_code"} or key == "tail" and check["exit_code"]
            }
            for check in result["checks"]
        ]
    return result


def _progress(card, metrics, review):
    if card is None:
        passed = metrics.get("tests", {}).get("passed", 0)
        passed += sum(case["passed"] for case in metrics.get("frozen", {}).get("cases", []))
        checks = sum(check["exit_code"] == 0 for check in metrics.get("checks", []))
        return (0, passed, checks, 0, 0)
    return (
        1,
        int(metrics["passed"]),
        sum(check["passed"] for check in review.get("checks", [])),
        -len(metrics["failures"]),
        -metrics.get("violation_distance", 0),
    )


def _history_fields(metrics, progress):
    return {
        "metrics_passed": metrics.get("passed", progress[1]),
        "failures": metrics.get("failures", []),
    }


class Revisions:
    def __init__(self):
        self.history = []
        self.best = None
        self.last = None
        self.seen = set()
        self.stalled = 0
        self.best_progress = None

    @property
    def exhausted(self):
        return self.stalled >= NO_PROGRESS_LIMIT

    def repeated(self, identity):
        if identity not in self.seen:
            self.seen.add(identity)
            return False
        self.stalled += 1
        entry = {
            "attempt": len(self.history) + 1,
            "status": "repeated",
            "progress": False,
            "failures": ["This exact candidate was already checked; return a revision"],
        }
        self.history.append(entry)
        self.last = entry
        return True

    def record(self, number, patch, card, metrics, review, artifacts):
        progress = _progress(card, metrics, review)
        improved = self.best_progress is None or progress > self.best_progress
        self.stalled = 0 if improved else self.stalled + 1
        self.last = {
            "attempt": number,
            "patch": patch,
            "metrics": _feedback_metrics(metrics),
            "review": review,
            "measurements": _measurements(card),
            "artifacts": str(artifacts),
        }
        if improved:
            self.best, self.best_progress = self.last, progress
        self.history.append(
            {
                "attempt": number,
                "status": "evaluated",
                "progress": improved,
                **_history_fields(metrics, progress),
                "review_failures": [
                    c["reason"] for c in review.get("checks", []) if not c["passed"]
                ],
                "measurements": _measurements(card),
                "artifacts": str(artifacts),
            }
        )
        return improved

    def feedback(self):
        return {
            "instruction": "Revise the best candidate, preserving the parts that already work. "
            "Address the remaining failed checks. Use the attempt history to avoid cycling back "
            "to a worse approach. Return edits against the original source as requested. "
            "The coordinator reruns the frozen tests and measures scores; do not estimate scores.",
            "best_candidate": self.best,
            "last_attempt": self.last,
            "history": self.history,
            "consecutive_attempts_without_progress": self.stalled,
            "stop_after_attempts_without_progress": NO_PROGRESS_LIMIT,
        }
