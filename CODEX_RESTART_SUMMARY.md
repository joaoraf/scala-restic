# Codex Restart Summary

## Project

This repository implements a Scala 3 / ZIO 2.1 API for running external `restic` processes from ZIO code. The API hides process execution, command-line argument construction, and JSON decoding for restic commands.

Main package:

- `br.gov.lexml.scala_restic`

Important source areas:

- `data.*`: case classes/enums and JSON codecs for restic JSON output.
- `options.*`: command-line option data types that implement `ResticOptionSource`.
- `command.ResticCommandService`: public service methods that execute restic commands.
- `command.ResticCommandBuilder`: builds `zio.process.Command` values.

## Build And Runtime

The project uses Maven with Scala Maven Plugin.

Use these local tools when running commands in this workspace:

```bash
JAVA_HOME=/home/joao/.sdkman/candidates/java/current \
  /home/joao/.sdkman/candidates/maven/current/bin/mvn test
```

The test environment has `restic` installed at:

```bash
/usr/bin/restic
```

The current restic version observed during testing:

```text
restic 0.18.1
```

## Current Test Suite

Main test file:

- `src/test/scala/br/gov/lexml/scala_restic/command/ResticCommandServiceSpec.scala`

The spec uses ZIO Test with JUnit/Surefire discovery. `pom.xml` includes:

- `zio-test`
- `zio-test-junit`
- `maven-surefire-plugin`

Surefire includes `**/*Spec.class`.

The spec configures a DEBUG console logger with:

```scala
override val bootstrap: ZLayer[Any, Any, TestEnvironment] =
  ZLayer.make[TestEnvironment](
    testEnvironment,
    Runtime.removeDefaultLoggers,
    consoleLogger(ConsoleLoggerConfig(
      LogFormat.default,
      LogLevelByNameConfig(LogLevel.Debug)
    ))
  )
```

Decoded backup/restore stream messages are logged with `ZIO.logDebug`.

## Existing ResticCommandService Test Coverage

The current spec covers:

- repository initialization and repository existence check,
- backup and restore using temporary directories and random generated files,
- backup and restore stream decoding, including summaries,
- snapshot listing after multiple backups,
- full `init -> backup -> snapshots -> restore` lifecycle for all supported password forms.

Password forms covered:

- no password via `--insecure-no-password`,
- password via restic `--password-command`,
- password via restic `--password-file`,
- password through stdin by passing a non-empty `password` parameter to `ResticCommandService` methods.

Note: restic does not provide a literal `--password` option. In tests, “password using an argument” is represented by `--password-command`.

## Test Fixture Details

`withResticFixture` creates a temporary work directory and temporary repository path, constructs:

```scala
ResticCommandService(ResticCommandBuilderService(ResticConfig(resticExecutable)))
```

The default fixture common options are:

```scala
CommonOptions(insecureNoPassword = true, noCache = true)
```

Parameterized file generation is done by:

```scala
createRandomSourceTree(
  root: Path,
  maxFolderDepth: Int,
  maxFolderSize: Int,
  numberOfFiles: Int,
  totalSizeBytes: Long
)
```

It creates exactly `numberOfFiles` files, in a randomized folder hierarchy with at most `maxFolderDepth` levels, no more than `maxFolderSize` direct files per generated folder, and total file content size of exactly `totalSizeBytes`.

`createRandomSourceTree` can be called again on the same root to add more files. It does not delete existing files.

## Important Implementation Notes

`ResticCommandBuilder.command` currently builds command arguments in this order:

```scala
restic <repo args> <subcommand> <accumulated args>
```

This matters for commands like backup and restore, where paths/snapshot IDs must appear after the subcommand.

`CommonOptions.passwordFile` must render as:

```text
--password-file=/path/to/file
```

not:

```text
--password-file=Some(/path/to/file)
```

`RestoreOptions.target` must render as:

```text
--target=/path/to/target
```

not:

```text
--target=Some(/path/to/target)
```

`InitResult.repository` is a `String`, because `restic init --json` returns local filesystem paths such as `/tmp/...`, not necessarily URLs.

## Snapshots Behavior

For restic 0.18.1, `restic snapshots --json` returns a flat JSON array when `--group-by` is not specified.

`SnapshotsOptions.groupBy` has been changed to default to the empty string. With that default, `SnapshotsOptions()` omits `--group-by` and matches the current `Snapshots` decoder.

If `--group-by=host,path` is passed, restic returns grouped JSON records with `group_key` and nested `snapshots`, which the current `Snapshots` decoder does not handle.

## Current Verification State

The latest full test run passed:

```text
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Command used:

```bash
JAVA_HOME=/home/joao/.sdkman/candidates/java/current \
  /home/joao/.sdkman/candidates/maven/current/bin/mvn test
```

## Git / Worktree Notes

The worktree had pre-existing local changes before several test additions. Do not revert unrelated user changes.

Known touched areas include:

- `pom.xml`
- `src/main/scala/br/gov/lexml/scala_restic/command/ResticCommands.scala`
- `src/main/scala/br/gov/lexml/scala_restic/data/init/InitResult.scala`
- `src/main/scala/br/gov/lexml/scala_restic/options/common/common_options.scala`
- `src/main/scala/br/gov/lexml/scala_restic/options/restore/RestoreOptions.scala`
- `src/main/scala/br/gov/lexml/scala_restic/options/snapshots/SnapshotsOptions.scala`
- `src/test/scala/br/gov/lexml/scala_restic/command/ResticCommandServiceSpec.scala`

