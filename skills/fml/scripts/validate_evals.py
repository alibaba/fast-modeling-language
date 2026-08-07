#!/usr/bin/env python3
"""Validate FML skill evals, run CLI smoke cases, and write a data report."""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


SKILL_ROOT = Path(__file__).resolve().parents[1]
TASK_EVALS = SKILL_ROOT / "evals" / "evals.json"
TRIGGER_EVALS = SKILL_ROOT / "evals" / "trigger-evals.json"


SCENARIOS = ("validate", "mysql", "postgresql", "inspect", "format", "plantuml", "capabilities")


def fail(message: str) -> None:
    print("ERROR: " + message, file=sys.stderr)
    raise SystemExit(1)


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail("{}: {}".format(path, error))


def validate_task_evals() -> Dict[str, int]:
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
            fixture = (TASK_EVALS.parent / relative_file).resolve()
            try:
                fixture.relative_to(TASK_EVALS.parent.resolve())
            except ValueError:
                fail("task evaluation {} file escapes the eval directory: {}".format(
                    eval_id, relative_file))
            if not fixture.is_file():
                fail("task evaluation {} file does not exist: {}".format(eval_id, relative_file))
        assertions = item.get("assertions")
        if not isinstance(assertions, list) or len(assertions) < 3:
            fail("task evaluation {} requires at least 3 assertions".format(eval_id))
        if any(not isinstance(assertion, str) or not assertion.strip() for assertion in assertions):
            fail("task evaluation {} has an invalid assertion".format(eval_id))
        combined_prompts.append(json.dumps(item, ensure_ascii=False).lower())

    prompts = " ".join(combined_prompts)
    for scenario in SCENARIOS:
        if scenario not in prompts:
            fail("task evaluations do not cover scenario: {}".format(scenario))
    return {
        "task_cases": len(evals),
        "assertions": sum(len(item["assertions"]) for item in evals),
        "scenarios": len(SCENARIOS),
    }


def validate_trigger_evals() -> Dict[str, int]:
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
    return {
        "trigger_cases": len(evals),
        "positive_triggers": counts[True],
        "negative_triggers": counts[False],
    }


def run_cli_case(jar: Path, name: str, arguments: Sequence[str],
                 expected_exit: int, expected_ok: bool,
                 stdin: Optional[str] = None) -> Dict[str, Any]:
    process = subprocess.run(
        ["java", "-jar", str(jar)] + list(arguments),
        input=stdin,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    try:
        payload = json.loads(process.stdout)
    except json.JSONDecodeError:
        payload = {}
    passed = process.returncode == expected_exit and payload.get("ok") is expected_ok
    return {
        "name": name,
        "command": " ".join(arguments),
        "expected_exit": expected_exit,
        "actual_exit": process.returncode,
        "expected_ok": expected_ok,
        "actual_ok": payload.get("ok"),
        "passed": passed,
    }


def run_cli_evals(jar: Path) -> List[Dict[str, Any]]:
    fixtures = TASK_EVALS.parent / "fixtures"
    cases = [
        ("capability discovery", ["capabilities"], 0, True, None),
        ("valid FML", ["validate", str(fixtures / "customer.fml")], 0, True, None),
        ("invalid FML", ["validate", str(fixtures / "order_model.fml")], 3, False, None),
        ("MySQL transform",
         ["transform", "--dialect", "mysql", str(fixtures / "customer.fml")], 0, True, None),
        ("PlantUML transform",
         ["transform", "--dialect", "plantuml", str(fixtures / "shop.fml")], 0, True, None),
        ("canonical format", ["format", str(fixtures / "inventory.fml")], 0, True, None),
        ("stdin AST inspect", ["inspect", "-"], 0, True,
         "create dim table dim_inline (id bigint);"),
    ]
    return [
        run_cli_case(jar, name, arguments, expected_exit, expected_ok, stdin)
        for name, arguments, expected_exit, expected_ok, stdin in cases
    ]


def write_report(path: Path, task_metrics: Dict[str, int],
                 trigger_metrics: Dict[str, int],
                 cli_results: List[Dict[str, Any]]) -> None:
    cli_passed = sum(1 for result in cli_results if result["passed"])
    total_checks = task_metrics["task_cases"] + trigger_metrics["trigger_cases"] + len(cli_results)
    passed_checks = task_metrics["task_cases"] + trigger_metrics["trigger_cases"] + cli_passed
    lines = [
        "# FML Skill Evaluation Report",
        "",
        "## Summary",
        "",
        "| Metric | Result |",
        "| --- | ---: |",
        "| Task evaluation contracts | {}/{} structurally valid |".format(
            task_metrics["task_cases"], task_metrics["task_cases"]),
        "| Quantitative assertions | {} |".format(task_metrics["assertions"]),
        "| Required workflow scenarios | {}/{} covered |".format(
            task_metrics["scenarios"], len(SCENARIOS)),
        "| Trigger evaluations | {} positive / {} negative |".format(
            trigger_metrics["positive_triggers"], trigger_metrics["negative_triggers"]),
        "| CLI smoke evaluations | {}/{} passed |".format(cli_passed, len(cli_results)),
        "| Deterministic checks | {}/{} passed |".format(passed_checks, total_checks),
        "",
        "## CLI smoke evaluations",
        "",
        "| Case | Expected | Actual | Result |",
        "| --- | --- | --- | --- |",
    ]
    for result in cli_results:
        lines.append("| {} | exit {}, ok={} | exit {}, ok={} | {} |".format(
            result["name"],
            result["expected_exit"],
            str(result["expected_ok"]).lower(),
            result["actual_exit"],
            str(result["actual_ok"]).lower(),
            "PASS" if result["passed"] else "FAIL",
        ))
    lines.extend([
        "",
        "## Coverage",
        "",
        "- Task scenarios: {}".format(", ".join(SCENARIOS)),
        "- Trigger selection: {} should-trigger and {} should-not-trigger prompts".format(
            trigger_metrics["positive_triggers"], trigger_metrics["negative_triggers"]),
        "- Fixtures: valid FML, invalid FML, formatting input, and visualization input",
        "",
        "## Interpretation",
        "",
        "This report measures dataset integrity and deterministic CLI behavior. "
        "The task assertions define expected agent behavior but are not counted as model-graded "
        "passes; model quality requires independent agent runs and assertion grading.",
        "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli-jar", type=Path, help="Executable FML CLI JAR for smoke evaluations")
    parser.add_argument("--report", type=Path, help="Write a Markdown evaluation report")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    task_metrics = validate_task_evals()
    trigger_metrics = validate_trigger_evals()
    cli_results: List[Dict[str, Any]] = []
    if args.cli_jar:
        jar = args.cli_jar.resolve()
        if not jar.is_file():
            fail("CLI JAR does not exist: {}".format(jar))
        cli_results = run_cli_evals(jar)
        failed = [result["name"] for result in cli_results if not result["passed"]]
        if failed:
            fail("CLI evaluations failed: {}".format(", ".join(failed)))
    if args.report:
        if not cli_results:
            fail("--report requires --cli-jar so the report contains runtime results")
        write_report(args.report, task_metrics, trigger_metrics, cli_results)
        print("Wrote FML skill evaluation report: {}".format(args.report))
    print("Validated FML skill evals: task cases and trigger cases are structurally sound.")


if __name__ == "__main__":
    main()
