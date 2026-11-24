"""Run the low-cost 3-task pilot or 20-task LakeQA subset with bounded catalog and search-loop guards."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT.parents[1]
CORPUS = ROOT.parent / "lakeqa-eval-corpus" / "data"
CONFIG_PATH = ROOT / "subset-config.toml"
PILOT_MANIFEST = ROOT / "pilot3-manifest.json"
SUBSET_MANIFEST = ROOT / "subset20-manifest.json"
ENV_KEY = "LAKEQA_LOCAL_API_KEY"


def load_env() -> dict[str, str]:
    """Read API key from environment or runtime/config.toml without writing secrets to disk."""
    key = os.environ.get(ENV_KEY, "").strip()
    if not key:
        cfg_file = RUNTIME / "config.toml"
        if cfg_file.is_file():
            llm = tomllib.loads(cfg_file.read_text()).get("llm", {})
            variable = llm.get("api_key_env", "")
            key = llm.get("api_key") or os.environ.get(variable, "")
            if not key and re.fullmatch(r"(?:(?:sk|ds)-|(?:codex|sk)_)[A-Za-z0-9_-]{16,}", variable):
                key = variable
    if not key:
        raise SystemExit(
            f"Missing API key. Set {ENV_KEY} in your shell or configure [llm] in runtime/config.toml."
        )
    return os.environ | {ENV_KEY: key, "PYTHONPATH": str(RUNTIME / "src")}


def launch(manifest: Path, output: Path, repeats: int, task_set_id: str, env: dict[str, str]) -> None:
    """Run one resumable shard through tests.ablation_runtime."""
    if (output / "report.json").is_file():
        return
    args = [
        sys.executable,
        "-m",
        "tests.ablation_runtime",
        "--config",
        str(CONFIG_PATH),
        "--manifest",
        str(manifest),
        "--step-builder",
        "tests.support.lakeqa:build_step",
        "--dataset-id",
        "lakeqa-mini",
        "--task-set-id",
        task_set_id,
        "--output-dir",
        str(output),
        "--repeats",
        str(repeats),
    ]
    if (output / "checkpoint.json").is_file():
        args.append("--resume")
    log_path = output.parent / f"{output.name}.log"
    with log_path.open("ab") as log:
        subprocess.run(args, cwd=RUNTIME, env=env, stdout=log, stderr=log, check=True)


def print_summary(run_dir: Path, shard_dirs: list[Path]) -> None:
    """Print a compact per-mode summary from completed shard samples.jsonl files."""
    by_mode: dict[str, dict[str, float]] = {}
    for shard in shard_dirs:
        samples_file = shard / "samples.jsonl"
        if not samples_file.is_file():
            continue
        for line in samples_file.read_text().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            mode = row["mode"]
            st = by_mode.setdefault(
                mode,
                {
                    "n": 0,
                    "completed": 0,
                    "acc_sum": 0.0,
                    "num_n": 0,
                    "num_sum": 0.0,
                    "tok_n": 0,
                    "tok_sum": 0,
                    "reqs": 0,
                },
            )
            st["n"] += 1
            res = row["result"]
            if res["status"] == "completed":
                st["completed"] += 1
            if row.get("accuracy") is not None:
                st["acc_sum"] += row["accuracy"]
            if row.get("numerical_accuracy") is not None:
                st["num_n"] += 1
                st["num_sum"] += row["numerical_accuracy"]
            usage = res.get("usage") or {}
            st["reqs"] += usage.get("provider_requests") or 0
            it, ot = usage.get("input_tokens"), usage.get("output_tokens")
            if it is not None and ot is not None:
                st["tok_n"] += 1
                st["tok_sum"] += it + ot

    print(f"\n=== SUMMARY ({run_dir.name}) ===")
    for mode in ("fixed_retrieval", "no_verifier", "full_agent"):
        if mode not in by_mode:
            continue
        st = by_mode[mode]
        n = int(st["n"])
        comp = st["completed"] / n * 100 if n else 0
        acc = st["acc_sum"] / n * 100 if n else 0
        num = (st["num_sum"] / st["num_n"] * 100) if st["num_n"] else None
        avg_tok = (st["tok_sum"] / st["tok_n"]) if st["tok_n"] else 0
        num_str = f"{num:.1f}% ({int(st['num_n'])})" if num is not None else "n/a"
        print(
            f"{mode:15s} | n={n:2d} | completed={comp:5.1f}% | acc={acc:5.1f}% "
            f"| num_acc={num_str:11s} | tokens={int(st['tok_sum']):,} (avg {avg_tok:,.0f}) | reqs={int(st['reqs'])}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot", action="store_true", help="Run the 3-task smoke test (1 shard, 1 repeat).")
    parser.add_argument("--repeats", type=int, default=1, help="Number of repeats per mode (default: 1).")
    parser.add_argument("--shards", type=int, default=2, help="Parallel shards for 20-task run (default: 2).")
    args = parser.parse_args()

    if not (CORPUS.parent / "verified-summary.json").exists():
        raise SystemExit("LakeQA corpus inventory is not finalized.")
    env = load_env()

    if args.pilot:
        out = ROOT / "pilot3-out"
        launch(PILOT_MANIFEST, out, 1, "lakeqa-pilot3-v1", env)
        print_summary(out, [out])
        return

    rows = json.loads(SUBSET_MANIFEST.read_text(), parse_float=str)
    out = ROOT / f"subset20-r{args.repeats}"
    out.mkdir(exist_ok=True)
    shards = max(1, min(args.shards, len(rows)))
    jobs = []
    shard_dirs = []
    for index in range(shards):
        subset = rows[index::shards]
        manifest = out / f"manifest-{index:02}.json"
        encoded = json.dumps(subset, ensure_ascii=False, sort_keys=True)
        if manifest.is_file() and manifest.read_text() != encoded:
            raise SystemExit(f"Saved shard membership changed: {manifest}")
        manifest.write_text(encoded)
        shard_dir = out / f"shard-{index:02}"
        shard_dirs.append(shard_dir)
        jobs.append((manifest, shard_dir, args.repeats, "lakeqa-subset20-v1", env))

    with ThreadPoolExecutor(max_workers=shards) as pool:
        futures = [pool.submit(launch, *job) for job in jobs]
        for fut in futures:
            fut.result()
    print_summary(out, shard_dirs)


if __name__ == "__main__":
    main()
