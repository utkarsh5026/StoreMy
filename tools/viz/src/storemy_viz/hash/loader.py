"""Loads hash-dump JSON.

Mirrors :mod:`storemy_viz.heap.loader`. Currently the only supported input
is a ``.json`` file produced by hand or by tests; once a
``storemy-hash-dump`` Rust binary exists, this module can grow an
on-the-fly path the same way the heap loader does.
"""

from __future__ import annotations

import json
from pathlib import Path

from .model import Dump, from_json


class DumperNotFound(Exception):
    """Raised when the (future) Rust dumper binary cannot be located."""


class DumperFailed(Exception):
    """Raised when the (future) Rust dumper exits with a non-zero status."""

    def __init__(self, returncode: int, stderr: str) -> None:
        super().__init__(f"storemy-hash-dump exited {returncode}: {stderr.strip()}")
        self.returncode = returncode
        self.stderr = stderr


def load_dump(path: Path) -> Dump:
    """Load a hash dump from ``path``.

    Today only ``.json`` files are accepted. Raw ``.hash`` files will be
    supported once the Rust dumper lands.
    """
    if path.suffix != ".json":
        raise DumperNotFound(
            "raw hash files are not yet supported; pass a .json dump "
            "(storemy-hash-dump is not implemented yet)"
        )
    obj = json.loads(path.read_text())
    return from_json(obj)
