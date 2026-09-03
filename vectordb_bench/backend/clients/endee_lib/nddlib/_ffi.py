"""ctypes binding for libndd_capi - the ndd C ABI (see src/api/ndd_capi.h).

Everything C-specific lives here: locating the shared library, the four entry
points, and the malloc'd-buffer discipline. Every output buffer the library
returns is copied into a Python ``bytes`` and released with ``ndd_free`` before
this layer returns, so no caller above it ever holds a raw pointer.
"""

from __future__ import annotations

import ctypes
import os
import sys
import warnings
from ctypes import (
    POINTER,
    byref,
    c_char_p,
    c_int,
    c_size_t,
    c_ubyte,
    c_void_p,
    create_string_buffer,
)
from pathlib import Path
from typing import Optional

from .errors import NddError, error_for

# The ISA suffixes the build stamps into the library name (see CMakeLists.txt).
_ISA_SUFFIXES = ("avx512", "avx2", "sve2", "neon-darwin", "neon")


def _lib_filenames() -> list[str]:
    """Candidate file names, most specific first: the bare symlink the build
    creates, then each per-ISA name."""
    if sys.platform == "darwin":
        prefix, ext = "lib", ".dylib"
    elif sys.platform == "win32":
        prefix, ext = "", ".dll"
    else:
        prefix, ext = "lib", ".so"
    names = [f"{prefix}ndd_capi{ext}"]
    names += [f"{prefix}ndd_capi-{isa}{ext}" for isa in _ISA_SUFFIXES]
    return names


def _search_dirs() -> list[Path]:
    """Directories to probe, in priority order."""
    dirs: list[Path] = []
    here = Path(__file__).resolve()
    dirs.append(here.parent)  # library shipped inside the wheel
    # Walk up looking for a repo checkout's build directory, so a source
    # checkout works with no configuration at all.
    for parent in here.parents:
        for build_dir in ("build", "build-clang"):
            candidate = parent / build_dir
            if candidate.is_dir():
                dirs.append(candidate)
        if (parent / "CMakeLists.txt").is_file():
            break
    dirs.append(Path.cwd())
    dirs.append(Path.cwd() / "build")
    return dirs


def find_library() -> str:
    """Locate libndd_capi.

    ``NDD_CAPI_PATH`` wins if set (a full path to the library, or a directory
    holding it), then ``NDD_CAPI_SO`` - the name the in-process ``ndd_lib``
    package and its ``ndd.env`` use, accepted so one config file drives both.
    Otherwise the wheel directory, then any sibling build/ directory, then the
    loader's own default paths are tried.
    """
    for var in ("NDD_CAPI_PATH", "NDD_CAPI_SO"):
        override = os.environ.get(var)
        if not override:
            continue
        path = Path(override).expanduser()
        if path.is_file():
            return str(path)
        if path.is_dir():
            for name in _lib_filenames():
                if (path / name).is_file():
                    return str(path / name)
        raise NddError(
            f"{var}={override!r} does not point at a libndd_capi shared library"
        )

    for directory in _search_dirs():
        for name in _lib_filenames():
            candidate = directory / name
            if candidate.is_file():
                return str(candidate)

    # Last resort: let the dynamic loader search its standard paths.
    for name in _lib_filenames():
        try:
            ctypes.CDLL(name)
            return name
        except OSError:
            continue

    raise NddError(
        "Could not find libndd_capi. Build it with\n"
        "    cmake -S . -B build -DNDD_BUILD_CAPI=ON -DUSE_AVX2=ON   # or -DUSE_NEON=ON\n"
        "    cmake --build build --target ndd_capi\n"
        "then set NDD_CAPI_PATH to the resulting library or its directory."
    )


class NddOpenOptions(ctypes.Structure):
    """Mirrors ``struct ndd_open_options``. Field order and types must match
    src/api/ndd_capi.h exactly."""

    _fields_ = [
        ("data_dir", c_char_p),
        ("skip_sanity", c_int),
        ("save_on_shutdown", c_int),
        ("vector_cache_max_bytes", c_size_t),
        ("parallel_insert_threads", c_int),
    ]


class _Lib:
    """Thin wrapper binding the four exported symbols with real signatures."""

    def __init__(self, path: str) -> None:
        self.path = path
        try:
            self._dll = ctypes.CDLL(path)
        except OSError as exc:
            raise NddError(f"Failed to load {path}: {exc}") from exc

        self.ndd_open = self._dll.ndd_open
        self.ndd_open.argtypes = [POINTER(NddOpenOptions), POINTER(c_char_p)]
        self.ndd_open.restype = c_void_p

        self.ndd_close = self._dll.ndd_close
        self.ndd_close.argtypes = [c_void_p]
        self.ndd_close.restype = None

        self.ndd_call = self._dll.ndd_call
        self.ndd_call.argtypes = [
            c_void_p,                 # handle
            c_char_p,                 # op
            c_char_p,                 # collection_id
            POINTER(c_ubyte),         # req
            c_size_t,                 # req_len
            POINTER(POINTER(c_ubyte)),  # out
            POINTER(c_size_t),        # out_len
            POINTER(c_char_p),        # msg
            POINTER(c_size_t),        # msg_len
        ]
        self.ndd_call.restype = c_int

        self.ndd_free = self._dll.ndd_free
        self.ndd_free.argtypes = [c_void_p]
        self.ndd_free.restype = None

        self._ndd_version = self._dll.ndd_version
        self._ndd_version.argtypes = []
        # Deliberately c_void_p, not c_char_p: the returned pointer is static
        # storage owned by the library and must NOT be freed. Reading it as
        # c_void_p keeps ctypes from auto-converting and makes that explicit.
        self._ndd_version.restype = c_void_p

    def version(self) -> str:
        """Library version + ISA, e.g. ``2.1.0+neon``. Static storage; not freed."""
        ptr = self._ndd_version()
        return ctypes.cast(ptr, c_char_p).value.decode() if ptr else ""


_lib: Optional[_Lib] = None
_loaded_data_dir: Optional[str] = None


def lib() -> _Lib:
    """Load libndd_capi once per process and cache it."""
    global _lib, _loaded_data_dir
    if _lib is None:
        _lib = _Lib(find_library())
        # Record the data dir the engine's per-TU settings initialized from; see
        # publish_data_dir() for why this matters.
        _loaded_data_dir = os.environ.get("NDD_DATA_DIR")
    return _lib


def publish_data_dir(data_dir: str) -> None:
    """Export ``NDD_DATA_DIR`` before the shared library is first loaded.

    ``ndd_open`` takes the data dir as an option, but the engine's
    ``settings::DATA_DIR`` is declared ``inline static`` at namespace scope
    (src/utils/settings.hpp), which gives it *internal linkage* - one copy per
    translation unit. ``ndd_open`` assigns only its own copy, so the startup
    sanity checks (a separate TU) still validate whatever the default was,
    typically ``/mnt/data``, and fail on any machine without it.

    Every copy initializes from ``NDD_DATA_DIR`` at load time, so setting the
    variable before ``dlopen`` makes them all agree. Storage itself is
    unaffected either way - CollectionManager is constructed with the explicit
    path - so this only fixes what the sanity checks look at.
    """
    if _lib is None:
        os.environ["NDD_DATA_DIR"] = data_dir
    elif _loaded_data_dir != data_dir:
        warnings.warn(
            f"libndd_capi was already loaded with NDD_DATA_DIR="
            f"{_loaded_data_dir!r}, so its startup sanity checks will evaluate "
            f"that path rather than {data_dir!r}. Construct the Database before "
            "calling version()/find_library() to avoid this, or set NDD_DATA_DIR "
            "before importing nddlib.",
            RuntimeWarning,
            stacklevel=3,
        )


def _take_bytes(ptr, length: int) -> bytes:
    """Copy a malloc'd buffer into Python bytes and free the original."""
    if not ptr:
        return b""
    try:
        return ctypes.string_at(ctypes.cast(ptr, c_void_p), length)
    finally:
        lib().ndd_free(ctypes.cast(ptr, c_void_p))


def open_handle(
    data_dir: str,
    *,
    skip_sanity: bool = False,
    save_on_shutdown: bool = True,
    vector_cache_max_bytes: int = 0,
    parallel_insert_threads: int = 0,
) -> int:
    """Call ``ndd_open``. Returns the opaque handle as an int address."""
    data_dir = os.fspath(data_dir)
    # Must happen before the first dlopen; see publish_data_dir().
    publish_data_dir(data_dir)
    opts = NddOpenOptions(
        data_dir=os.fspath(data_dir).encode(),
        skip_sanity=1 if skip_sanity else 0,
        save_on_shutdown=1 if save_on_shutdown else 0,
        vector_cache_max_bytes=vector_cache_max_bytes,
        parallel_insert_threads=parallel_insert_threads,
    )
    err = c_char_p()
    handle = lib().ndd_open(byref(opts), byref(err))
    if not handle:
        message = err.value.decode() if err.value else "ndd_open failed"
        if err:
            lib().ndd_free(ctypes.cast(err, c_void_p))
        raise NddError(message)
    return handle


def close_handle(handle: int) -> None:
    """Call ``ndd_close``. NULL-safe in C, so a zero handle is fine."""
    if handle:
        lib().ndd_close(c_void_p(handle))


def call(handle: int, op: str, collection_id: Optional[str], request: bytes = b"") -> bytes:
    """Invoke ``ndd_call`` and return the response body.

    Raises the mapped :class:`NddError` subclass on any non-zero code, so a
    caller only ever sees a successful payload.
    """
    if not handle:
        raise NddError("Database is closed")

    req_buf = create_string_buffer(request, len(request)) if request else None
    req_ptr = (
        ctypes.cast(req_buf, POINTER(c_ubyte)) if req_buf is not None else POINTER(c_ubyte)()
    )

    out = POINTER(c_ubyte)()
    out_len = c_size_t(0)
    msg = c_char_p()
    msg_len = c_size_t(0)

    code = lib().ndd_call(
        c_void_p(handle),
        op.encode(),
        collection_id.encode() if collection_id is not None else None,
        req_ptr,
        len(request),
        byref(out),
        byref(out_len),
        byref(msg),
        byref(msg_len),
    )

    payload = _take_bytes(out, out_len.value)
    message = ""
    if msg:
        message = _take_bytes(msg, msg_len.value).decode(errors="replace")

    if code != 0:
        raise error_for(code, message)
    return payload


def version() -> str:
    """Version string of the loaded library, e.g. ``2.1.0+avx2``."""
    return lib().version()
