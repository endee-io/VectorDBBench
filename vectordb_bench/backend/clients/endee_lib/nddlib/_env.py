"""dotenv-style config file support, mirroring nav/ndd_lib's ``load_env_file``.

The engine reads most of its knobs once, via ``getenv`` at static-init time,
and caches them - so a value only takes effect if it is in the environment
**before libndd_capi is dlopen'd**. This module is therefore applied at
``nddlib`` import time (see ``nddlib/__init__.py``), which is always before the
first :func:`nddlib._ffi.lib` call maps the library.

The file is **authoritative**: it overwrites any already-exported ``NDD_*`` so a
stale shell environment cannot silently win. That matches the semantics of the
in-process ``ndd_lib`` package this mirrors.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Union

_PKG_DIR = Path(__file__).resolve().parent

#: Path of the file :func:`load_env_file` actually applied, or ``None``.
loaded_path: Optional[str] = None


def candidate_paths() -> List[Path]:
    """Where to look for ``ndd.env``, most specific first.

    ``NDD_ENV_FILE`` short-circuits the search. Otherwise the package dir wins,
    then the client package holding it, then a sibling ``ndd_lib/`` (the layout
    the in-process package ships), then the working directory.
    """
    override = os.environ.get("NDD_ENV_FILE")
    if override:
        return [Path(override).expanduser()]
    return [
        _PKG_DIR / "ndd.env",
        _PKG_DIR.parent / "ndd.env",
        _PKG_DIR.parent / "ndd_lib" / "ndd.env",
        Path.cwd() / "ndd.env",
    ]


def load_env_file(path: Optional[Union[str, Path]] = None) -> Optional[str]:
    """Apply a ``KEY=VALUE`` file into ``os.environ`` (file wins).

    With no argument the first existing :func:`candidate_paths` entry is used.
    Returns the path applied, or ``None`` when no file was found.
    """
    global loaded_path
    paths = [Path(path).expanduser()] if path is not None else candidate_paths()
    for candidate in paths:
        if candidate.is_file():
            _apply(candidate)
            loaded_path = str(candidate)
            return loaded_path
    return None


def _apply(path: Path) -> None:
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]                       # quoted: keep literal
            else:
                for sep in (" #", "\t#"):             # strip an inline comment
                    i = val.find(sep)
                    if i != -1:
                        val = val[:i]
                val = val.rstrip()
            if key:
                os.environ[key] = val   # file authoritative


def env_bool(name: str, default: bool) -> bool:
    """Read a 0/1/true/false knob, falling back to ``default`` when unset."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int) -> int:
    """Read an integer knob, falling back to ``default`` when unset or junk."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default
