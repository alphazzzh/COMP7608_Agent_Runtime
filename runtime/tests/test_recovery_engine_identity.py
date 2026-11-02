"""Graph engine upgrades must not reuse an incompatible durable execution.

Source hashes alone cannot detect a different installed scheduler. These tests
change only package metadata, without editing source or persistent run records,
and require that recovery's implementation identity changes with the engine.
"""

from importlib import metadata

import pytest

from lake_agent.recovery import _implementation_digest


@pytest.mark.parametrize(
    "package", ["langgraph", "langchain-core", "langgraph-checkpoint", "langsmith"]
)
def test_graph_dependency_version_changes_recovery_identity(monkeypatch, package):
    """A version-only engine change cannot pass the source compatibility gate."""
    original = metadata.version
    before = _implementation_digest()

    def changed_version(name):
        if name == package:
            return "999.0.0-test"
        return original(name)

    monkeypatch.setattr(metadata, "version", changed_version)
    assert _implementation_digest() != before
