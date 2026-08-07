# FML Skill Evaluation Report

## Summary

| Metric | Result |
| --- | ---: |
| Task evaluation contracts | 8/8 structurally valid |
| Quantitative assertions | 32 |
| Required workflow scenarios | 7/7 covered |
| Trigger evaluations | 10 positive / 10 negative |
| CLI smoke evaluations | 7/7 passed |
| Deterministic checks | 35/35 passed |

## CLI smoke evaluations

| Case | Expected | Actual | Result |
| --- | --- | --- | --- |
| capability discovery | exit 0, ok=true | exit 0, ok=true | PASS |
| valid FML | exit 0, ok=true | exit 0, ok=true | PASS |
| invalid FML | exit 3, ok=false | exit 3, ok=false | PASS |
| MySQL transform | exit 0, ok=true | exit 0, ok=true | PASS |
| PlantUML transform | exit 0, ok=true | exit 0, ok=true | PASS |
| canonical format | exit 0, ok=true | exit 0, ok=true | PASS |
| stdin AST inspect | exit 0, ok=true | exit 0, ok=true | PASS |

## Coverage

- Task scenarios: validate, mysql, postgresql, inspect, format, plantuml, capabilities
- Trigger selection: 10 should-trigger and 10 should-not-trigger prompts
- Fixtures: valid FML, invalid FML, formatting input, and visualization input

## Interpretation

This report measures dataset integrity and deterministic CLI behavior. The task assertions define expected agent behavior but are not counted as model-graded passes; model quality requires independent agent runs and assertion grading.
