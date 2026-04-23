"""Loads heap-dump JSON, either from a `.json` file or by invoking the Rust dumper.

The CLI accepts a path that is either:

* a ``.json`` file containing a previously-captured dump, or
* a raw ``.heap`` file which we dump on the fly via ``storemy-heap-dump``.

The on-the-fly path requires the Rust binary to be built and available on
``PATH`` (or overridden via ``STOREMY_HEAP_DUMP_BIN``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

from .model import Dump, from_json

DEFAULT_BIN = "storemy-heap-dump"
ENV_VAR = "STOREMY_HEAP_DUMP_BIN"


class DumperNotFound(Exception):
    """Raised when the Rust dumper binary cannot be located."""


class DumperFailed(Exception):
    """Raised when the Rust dumper exits with a non-zero status."""

    def __init__(self, returncode: int, stderr: str) -> None:
        super().__init__(f"storemy-heap-dump exited {returncode}: {stderr.strip()}")
        self.returncode = returncode
        self.stderr = stderr


def load_dump(
    path: Path,
    *,
    field_count: int | None = None,
    field_names: list[str] | None = None,
    page: int | None = None,
) -> Dump:
    """Loads a dump from ``path``.

    If the path ends in ``.json`` it is read directly. Otherwise we run
    the Rust dumper; in that case ``field_count`` is required.
    """

    if path.suffix == ".json":
        obj = json.loads(path.read_text())
        return from_json(obj)

    if field_count is None:
        raise ValueError("field_count is required when loading a raw heap file (pass --fields N)")
    obj = _run_dumper(path, field_count, field_names, page)
    return from_json(obj)


def _run_dumper(
    path: Path,
    field_count: int,
    field_names: list[str] | None,
    page: int | None,
) -> dict:
    """Invoke the Rust heap dumper and parse its JSON stdout.

    Args are assembled to match the CLI contract of ``storemy-heap-dump``.
    Raises :class:`DumperNotFound` if the binary cannot be located and
    :class:`DumperFailed` if the dumper exits non-zero.
    """
    binary = _resolve_binary()
    args: list[str] = [binary, str(path), "--fields", str(field_count)]
    if field_names:
        args += ["--names", ",".join(field_names)]
    if page is not None:
        args += ["--page", str(page)]

    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise DumperFailed(proc.returncode, proc.stderr)
    return json.loads(proc.stdout)


def _resolve_binary() -> str:
    """Resolve the ``storemy-heap-dump`` executable path.

    Uses ``$STOREMY_HEAP_DUMP_BIN`` when set; otherwise falls back to locating
    ``storemy-heap-dump`` on ``PATH`` via :func:`shutil.which`.
    """
    override = os.environ.get(ENV_VAR)
    if override:
        return override
    path = shutil.which(DEFAULT_BIN)
    if path is None:
        raise DumperNotFound(
            f"{DEFAULT_BIN} not on PATH. "
            f"Build it with `cargo build --bin {DEFAULT_BIN}` and add the "
            f"target dir to PATH, or set ${ENV_VAR} to the full binary path."
        )
    return path
