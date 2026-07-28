---
name: fml
description: Work with Fast Modeling Language (FML) dimensional models through the repository's deterministic CLI. Use for creating or modifying .fml files, validating FML or supported engine-dialect syntax, diagnosing parser errors, formatting models, inspecting model AST structure, and transforming FML into any transformer dialect packaged by the project.
---

# FML

Use the `fml` CLI as the source of truth for parsing and transformation. Do not simulate parser behavior or claim that generated FML is valid without running `validate`.

## Locate the CLI

From the repository root, prefer the packaged executable JAR:

```bash
java -jar fastmodel-agent-cli/target/fastmodel-agent-cli-*-executable.jar capabilities
```

If it is unavailable, build it:

```bash
mvn -pl fastmodel-agent-cli -am package -DskipTests
```

Store the resolved JAR path once and reuse it. Every command emits one JSON object on stdout.

## Choose a command

- Run `validate` after creating or editing FML.
- Run `validate --dialect DIALECT` to parse supported native engine DDL.
- Run `format` when canonical FML source is required.
- Run `inspect` to understand statement and child-node types.
- Run `transform --dialect DIALECT` to generate engine SQL or visualization output.
- Run `capabilities` before relying on a dialect or exit code not documented here.

Read [references/cli.md](references/cli.md) when constructing commands or handling errors.

## Reliable modeling workflow

1. Read the existing model and preserve its naming and property conventions.
2. Make the smallest requested source change.
3. Run `validate FILE`.
4. If validation fails, use diagnostic `line`, `column`, and `message`; fix the source and rerun.
5. Run `format FILE` when canonical formatting is requested. Extract the JSON `output` value before writing it back.
6. Run `capabilities` and check the requested dialect's `transform` or `validate` flag.
7. Run `transform --dialect DIALECT FILE` when target output is requested.
8. Validate generated SQL with the target engine's own tooling when available. FML transformation success does not prove deployability in a specific engine environment.

## Input safety

Prefer a file argument for repository artifacts. Use stdin for generated or temporary content:

```bash
printf '%s' "$FML_SOURCE" | java -jar "$FML_CLI_JAR" validate -
```

Use `--text` only for short, controlled snippets. Never interpolate untrusted model text into a shell command.

## Interpret results

Treat `ok: true` and exit code `0` as success. Treat any nonzero exit code as failure even if partial output exists.

- Exit `2`: invalid CLI input or arguments.
- Exit `3`: FML parsing failed.
- Exit `4`: transformation or execution failed.

Return diagnostics to the user with their error code and source location. Do not discard structured errors or replace them with generic prose.
