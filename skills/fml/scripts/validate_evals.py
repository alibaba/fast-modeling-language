#!/usr/bin/env python3
"""Validate the FML skill's task and trigger evaluation datasets."""

import json
import sys
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[1]
TASK_EVALS = SKILL_ROOT / "evals" / "evals.json"
TRIGGER_EVALS = SKILL_ROOT / "evals" / "trigger-evals.json"


def fail(message):
    print("ERROR: " + message, file=sys.stderr)
    raise SystemExit(1)


def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail("{}: {}".format(path, error))


def validate_task_evals():
    document = load_json(TASK_EVALS)
    if document.get("skill_name") != "fml":
        fail("evals.json skill_name must be fml")

    evals = document.get("evals")
    if not isinstance(evals, list) or len(evals) < 6:
        fail("evals.json must contain at least 6 task evaluations")

    ids = set()
    combined_prompts = []
    for item in evals:
        eval_id = item.get("id")
        if not isinstance(eval_id, int) or eval_id in ids:
            fail("task evaluation ids must be unique integers")
        ids.add(eval_id)
        for field in ("prompt", "expected_output"):
            if not isinstance(item.get(field), str) or not item[field].strip():
                fail("task evaluation {} requires {}".format(eval_id, field))
        if not isinstance(item.get("files"), list):
            fail("task evaluation {} files must be an array".format(eval_id))
        for relative_file in item["files"]:
            if not isinstance(relative_file, str) or not relative_file.strip():
                fail("task evaluation {} has an invalid file path".format(eval_id))
            if not (TASK_EVALS.parent / relative_file).is_file():
                fail("task evaluation {} file does not exist: {}".format(eval_id, relative_file))
        assertions = item.get("assertions")
        if not isinstance(assertions, list) or len(assertions) < 3:
            fail("task evaluation {} requires at least 3 assertions".format(eval_id))
        if any(not isinstance(assertion, str) or not assertion.strip() for assertion in assertions):
            fail("task evaluation {} has an invalid assertion".format(eval_id))
        combined_prompts.append(json.dumps(item, ensure_ascii=False).lower())

    prompts = " ".join(combined_prompts)
    for scenario in ("validate", "mysql", "postgresql", "inspect", "format", "plantuml", "capabilities"):
        if scenario not in prompts:
            fail("task evaluations do not cover scenario: {}".format(scenario))


def validate_trigger_evals():
    evals = load_json(TRIGGER_EVALS)
    if not isinstance(evals, list) or len(evals) < 16:
        fail("trigger-evals.json must contain at least 16 evaluations")

    queries = set()
    counts = {True: 0, False: 0}
    for index, item in enumerate(evals, start=1):
        query = item.get("query")
        should_trigger = item.get("should_trigger")
        if not isinstance(query, str) or not query.strip():
            fail("trigger evaluation {} requires query".format(index))
        if query in queries:
            fail("trigger evaluation queries must be unique")
        queries.add(query)
        if not isinstance(should_trigger, bool):
            fail("trigger evaluation {} should_trigger must be boolean".format(index))
        counts[should_trigger] += 1

    if min(counts.values()) < 8:
        fail("trigger evaluations need at least 8 positive and 8 negative cases")


def main():
    validate_task_evals()
    validate_trigger_evals()
    print("Validated FML skill evals: task cases and trigger cases are structurally sound.")


if __name__ == "__main__":
    main()
