# Task 1: Repository Cloning and File Discovery

## Objective

The first task is to clone the assigned Python repository and discover all Python source files that will be processed by the streaming pipeline.

The selected repository is:

```text
https://github.com/huggingface/accelerate
```

The repository is cloned locally and then scanned for `.py` files. The discovered file list is saved as a manifest so that later stages can process files consistently.

## Implementation

The project uses two source modules for this task:

| Module | Purpose |
|---|---|
| `src.repo_tools.clone_repo` | Shallow-clones the assigned GitHub repository |
| `src.repo_tools.discover_files` | Enumerates Python files and writes a discovery manifest |

The clone step uses a shallow clone to reduce download size. After cloning, the discovery step walks through the repository tree and selects Python files.

The discovery policy excludes unnecessary files where possible, such as test folders, documentation examples, setup files, virtual environments, and generated files. This keeps the parser focused on source files that are useful for CPG construction.

## Commands

The following commands were run from the project root.

```bash
python -m src.repo_tools.clone_repo
```

```bash
python -m src.repo_tools.discover_files
```

The discovery manifest is written to:

```text
data/processed/discovered_files.json
```

It can be inspected with:

```bash
cat data/processed/discovered_files.json | head
```

## Output

The discovery step produced a manifest containing the Python files selected for processing.

```text
Discovered 120 Python files.
```

The manifest is used by the parser service when running in full repository mode.

## Manifest Format

The manifest contains the repository identity and the list of discovered Python files.

Example structure:

```json
{
  "repo_name": "accelerate",
  "total_files": 120,
  "files": [
    "benchmarks/big_model_inference/measures_util.py",
    "benchmarks/fp8/ms_amp/ddp.py",
    "benchmarks/fp8/ms_amp/non_distributed.py"
  ]
}
```

The exact list depends on the selected repository revision and the exclusion policy.

## Why File Discovery Matters

File discovery is separated from parsing for two reasons:

1. **Reproducibility**  
   The parser can process the same manifest repeatedly during testing and replay.

2. **Incremental processing**  
   The parser service can process one file at a time instead of loading the entire repository into memory.

This supports the incremental design required by the lab.

## Relation to Later Tasks

The output of this task is used by later components:

| Later task | How it uses discovery output |
|---|---|
| Parser Service | Reads files from the manifest and emits CPG events |
| Kafka Topic Design | Events contain stable `repo_name` and `file_path` fields |
| Neo4j Ingestion | Graph nodes and edges are grouped by repository and file path |
| MongoDB Ingestion | Metadata documents are keyed by repository and file path |
| Replay Verification | A single file can be selected and reprocessed |

## Reflection

The repository cloning and file discovery step was straightforward, but it is important because all later stages depend on stable file paths. If the file list changes unexpectedly, node counts, edge counts, metadata counts, and replay results can also change.

To keep the demo reproducible, the discovered file list is saved to `data/processed/discovered_files.json` and reused by the parser service.
