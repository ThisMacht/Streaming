# Task 1: Repository Cloning and File Discovery

## Goal

The first task prepares the input repository for the incremental CPG pipeline. The pipeline must work
on a real public Python repository, avoid unnecessary Git history, discover the Python source files
that will be parsed, and record the final file list so that the rest of the workflow is reproducible.

For this lab, the selected repository is:

```text
https://github.com/huggingface/accelerate.git
```

The output of this task is a deterministic discovery manifest:

```text
data/processed/discovered_files.json
```

This manifest is the source of truth for the parser service. Later tasks consume this list one file at
a time instead of scanning the repository again during ingestion.

## Implementation approach

The repository setup is split into two small commands:

```bash
python -m src.repo_tools.clone_repo
python -m src.repo_tools.discover_files
```

The clone step uses a shallow clone because the lab only analyzes the checked-out source tree. Full
Git history is not required for AST, CFG, DFG, CALL, Kafka, Neo4j, or MongoDB ingestion. This reduces
setup time and keeps the local working directory smaller.

The discovery step walks the cloned repository and selects Python source files. To keep the pipeline
focused on source files that represent the package implementation, the project excludes common
non-target paths and generated or auxiliary files. The exclusion policy removes directories such as
`tests`, `test`, `test_utils`, `docs`, `examples`, `__pycache__`, and `.venv`. It also excludes files
such as `setup.py` and `test_*.py`.

The exclusion logic is path-aware: it compares complete path components instead of checking whether
a filename merely contains the substring `test`. This avoids accidentally removing valid files whose
names only happen to contain similar text.

After filtering, paths are sorted deterministically before being written to the manifest. This makes
the run order stable across executions, which helps compare logs, debug failures, and reproduce the
same baseline ingestion.

## Evidence and result

The recorded demo run confirms that the repository was already present locally and that the pipeline
prepared the replay target before discovery:

```text
Repository path already exists; skipping clone: data/raw/accelerate
Replay target: src/accelerate/_lab_replay_probe.py
Modified: False
Events: nodes=10 edges=18
```

The discovery stage then found 99 Python files and saved the manifest:

```text
Discovered 99 Python files
Saved manifest to data/processed/discovered_files.json
```

The first entries shown in the pipeline log include:

```text
benchmarks/big_model_inference/big_model_inference.py
benchmarks/big_model_inference/measures_util.py
benchmarks/fp8/ms_amp/ddp.py
benchmarks/fp8/ms_amp/distrib_deepspeed.py
benchmarks/fp8/ms_amp/fp8_utils.py
benchmarks/fp8/ms_amp/non_distributed.py
benchmarks/fp8/torchao/ddp.py
benchmarks/fp8/torchao/distrib_deepspeed.py
benchmarks/fp8/torchao/fp8_utils.py
benchmarks/fp8/torchao/fsdp.py
```

This confirms that the repository discovery produced a real list of Python files rather than a
placeholder count. The full manifest is stored in the project output directory and is consumed by the
parser in Task 2.

## Role in the full pipeline

The discovery manifest controls the baseline ingestion run. In the recorded pipeline execution,
Task 2 parsed exactly the discovered files and completed with:

```text
Finished: successful=99 failed=0
```

This means every file selected by Task 1 was successfully processed by the parser service during the
baseline run. Each successful file later produced metadata for MongoDB and graph events for Neo4j.

The same task also prepares the controlled replay probe:

```text
src/accelerate/_lab_replay_probe.py
```

This file is included in the discovery manifest and is later modified in Task 6 to demonstrate
single-file replay, metadata upsert, graph replacement, and duplicate checks.

## Reflection

This task is simple but important because all later results depend on a stable input set. Shallow
cloning avoids unnecessary repository history, while deterministic discovery makes the parser and
streaming results repeatable. Writing the discovered paths to a manifest also separates repository
selection from parsing, which keeps the parser service incremental: it receives one file path at a
time rather than loading the entire repository into memory.

The final discovered count is 99 Python files for the recorded repository revision and exclusion
policy. Because this count can change if the repository revision or filtering policy changes, the
manifest is treated as the authoritative evidence instead of relying only on a hard-coded number in
the report.
