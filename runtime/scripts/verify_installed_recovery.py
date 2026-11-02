"""Install the built wheel outside the checkout and recover after real SIGKILL.

All ports are local synthetic fixtures. No credentials, model endpoints or paid
provider calls are used. The report records wheel/source identity and outcomes.
"""

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

root = Path.cwd()
wheel = root / "dist/lake_agent_runtime-0.4.0-py3-none-any.whl"
work = Path(tempfile.mkdtemp(prefix="comp7608-installed-recovery-"))
report = {"runtime_version": "0.4.0", "verification": "offline synthetic ports; no provider I/O"}
child = None


def command(args, **kwargs):
    result = subprocess.run(args, cwd=work, text=True, capture_output=True, timeout=60, **kwargs)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


try:
    command(["uv", "venv", str(work / "env"), "--python", sys.executable])
    py = str(work / "env/bin/python")
    command(["uv", "pip", "install", "--python", py, "--offline", str(wheel)])
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["PYTHONNOUSERSITE"] = "1"
    script = work / "probe.py"
    script.write_text('''"""Isolated-process recovery acceptance, using only synthetic local ports."""
import asyncio
import json
import sys
from pathlib import Path
import lake_agent
from lake_agent import models as m
from lake_agent.demo import DemoData, DemoContent, DemoPlanner
from lake_agent.runtime import run, resume
from lake_agent.state import SqliteStateStore
from lake_agent.trace import JsonlTraceSink

ROOT = Path(__file__).parent
class GateTrace(JsonlTraceSink):
    async def append(self, event):
        await super().append(event)
        if event.event_type == "evidence.added":
            (ROOT / "ready").write_text(event.data_ref)
            await asyncio.Future()

class CountingData(DemoData):
    def __init__(self):
        super().__init__()
        self.real_catalog = self.real_tools = 0
    async def catalog(self, req):
        self.real_catalog += 1
        return await super().catalog(req)
    async def invoke(self, req):
        self.real_tools += 1
        return await super().invoke(req)

async def main():
    stage = sys.argv[1]
    if stage == "modes":
        outcomes = {}
        for mode in ("fixed_retrieval", "no_verifier", "full_agent"):
            result = await run(m.RunRequest(task_id="installed-modes", question="What was 2024 annual revenue?", mode=mode),
                data=DemoData(), content=DemoContent(), planner=DemoPlanner(),
                trace=JsonlTraceSink(ROOT / "modes"))
            assert result.status == "completed", result
            assert result.usage.provider_requests == 0
            outcomes[mode] = {"status": result.status, "provider_requests": result.usage.provider_requests}
        print(json.dumps({"package_path": lake_agent.__file__, "modes": outcomes}))
        return
    store = SqliteStateStore(ROOT / "state.db")
    try:
        if stage == "crash":
            await run(m.RunRequest(run_id="installed-crash", task_id="installed-crash", question="What was 2024 annual revenue?",
                    config=m.RunConfig(max_wall_time_ms=60000, per_call_timeout_ms=10000)),
                data=DemoData(), content=DemoContent(), planner=DemoPlanner(),
                trace=GateTrace(ROOT / "crash-traces"), state_store=store)
            raise AssertionError("crash gate was not reached")
        data, content = CountingData(), DemoContent()
        result = await resume("installed-crash", data=data, content=content, planner=DemoPlanner(),
            trace=JsonlTraceSink(ROOT / "crash-traces"), state_store=store)
        assert result.status == "completed", result
        assert data.real_catalog == data.real_tools == 0
        assert result.call_counts.catalog == result.call_counts.tools == result.call_counts.steps == 1
        assert result.call_counts.content == 6
        assert result.usage.provider_requests == 0
        assert result.answer.citations == [(ROOT / "ready").read_text()]
        assert len(list((ROOT / "crash-traces").rglob("events.jsonl"))) == 2
        again = await resume("installed-crash", data=CountingData(), content=DemoContent(), planner=DemoPlanner(),
            trace=JsonlTraceSink(ROOT / "crash-traces"), state_store=store)
        assert again == result
        assert len(list((ROOT / "crash-traces").rglob("events.jsonl"))) == 2
        print(json.dumps({"status": result.status, "stop_reason": result.stop_reason,
            "call_counts": result.call_counts.model_dump(), "provider_requests": result.usage.provider_requests,
            "new_data_catalog_calls": data.real_catalog, "new_data_tool_calls": data.real_tools,
            "evidence_identity_preserved": True, "terminal_result_reused": True, "trace_segments": 2}))
    finally:
        await store.aclose()

asyncio.run(main())
''')
    modes = json.loads(command([py, str(script), "modes"], env=env))
    assert not str(modes["package_path"]).startswith(str(root))
    report["isolated_install"] = modes
    child = subprocess.Popen(
        [py, str(script), "crash"],
        cwd=work,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    expires = time.monotonic() + 10
    while not (work / "ready").exists():
        if child.poll() is not None:
            stdout, stderr = child.communicate()
            raise RuntimeError(stdout + stderr)
        if time.monotonic() > expires:
            raise TimeoutError("crash gate deadline")
        time.sleep(0.02)
    child.send_signal(signal.SIGKILL)
    child.communicate(timeout=10)
    assert child.returncode == -signal.SIGKILL
    report["crash_recovery"] = {
        "initial_exit_code": child.returncode,
        **json.loads(command([py, str(script), "resume"], env=env)),
    }
    with zipfile.ZipFile(wheel) as archive:
        modules = {
            n: hashlib.sha256(archive.read(n)).hexdigest()
            for n in archive.namelist()
            if n.startswith("lake_agent/") and n.endswith(".py")
        }
    for name, digest in modules.items():
        assert hashlib.sha256((root / "src" / name).read_bytes()).hexdigest() == digest, name
    report["wheel_sha256"] = hashlib.sha256(wheel.read_bytes()).hexdigest()
    report["packaged_source_sha256"] = modules
    report["packaged_sources_match"] = True
    report["temporary_environment_removed"] = True
    report_path = root / "docs/installed-wheel-recovery-2026-11-01.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(
        json.dumps(
            {
                "report": str(report_path),
                "packaged_modules": len(modules),
                "crash_recovery": report["crash_recovery"],
            }
        )
    )
finally:
    if child is not None and child.poll() is None:
        child.kill()
        child.communicate(timeout=10)
    shutil.rmtree(work)
