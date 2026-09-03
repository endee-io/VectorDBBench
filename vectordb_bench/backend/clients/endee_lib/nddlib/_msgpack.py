"""A small, self-contained MessagePack codec.

nddlib talks msgpack to libndd_capi for the vector-heavy operations (see
src/utils/msgpack_ndd.hpp). Rather than take a hard dependency on the ``msgpack``
package for a handful of fixed struct shapes, this module implements the subset
of the format that actually crosses the boundary.

What that subset is, and why:

* msgpack-c's ``MSGPACK_DEFINE`` serializes a struct as a **positional array**,
  so ``Object`` is ``[id, meta, filter, vectors, sparses, multi_vectors]``.
* ``std::string`` -> str, ``std::vector<uint8_t>`` -> **bin** (msgpack-c
  specializes it; see adaptor/vector_unsigned_char.hpp), ``std::vector<float>``
  -> array of float32, ``std::unordered_map`` -> map (with **integer keys** for
  the id-keyed maps in a search response).

The decoder accepts every standard type so an unexpected payload fails loudly
rather than silently mis-parsing; the encoder emits only what the C++ side
declares it can read.
"""

from __future__ import annotations

import struct
from typing import Any

_F32 = struct.Struct(">f")
_BE = {
    "B": struct.Struct(">B"),
    "H": struct.Struct(">H"),
    "I": struct.Struct(">I"),
    "Q": struct.Struct(">Q"),
    "b": struct.Struct(">b"),
    "h": struct.Struct(">h"),
    "i": struct.Struct(">i"),
    "q": struct.Struct(">q"),
    "f": _F32,
    "d": struct.Struct(">d"),
}


class MsgpackError(ValueError):
    """Raised on a malformed or unsupported msgpack payload."""


# ----------------------------------------------------------------------------
# encoding
# ----------------------------------------------------------------------------


def pack_array_header(out: bytearray, n: int) -> None:
    if n < 16:
        out.append(0x90 | n)
    elif n < 1 << 16:
        out.append(0xDC)
        out += _BE["H"].pack(n)
    else:
        out.append(0xDD)
        out += _BE["I"].pack(n)


def pack_map_header(out: bytearray, n: int) -> None:
    if n < 16:
        out.append(0x80 | n)
    elif n < 1 << 16:
        out.append(0xDE)
        out += _BE["H"].pack(n)
    else:
        out.append(0xDF)
        out += _BE["I"].pack(n)


def pack_str(out: bytearray, value: str) -> None:
    raw = value.encode("utf-8")
    n = len(raw)
    if n < 32:
        out.append(0xA0 | n)
    elif n < 1 << 8:
        out += b"\xd9" + _BE["B"].pack(n)
    elif n < 1 << 16:
        out += b"\xda" + _BE["H"].pack(n)
    else:
        out += b"\xdb" + _BE["I"].pack(n)
    out += raw


def pack_bin(out: bytearray, raw: bytes) -> None:
    n = len(raw)
    if n < 1 << 8:
        out += b"\xc4" + _BE["B"].pack(n)
    elif n < 1 << 16:
        out += b"\xc5" + _BE["H"].pack(n)
    else:
        out += b"\xc6" + _BE["I"].pack(n)
    out += raw


def pack_uint(out: bytearray, value: int) -> None:
    if value < 0:
        raise MsgpackError(f"expected an unsigned value, got {value}")
    if value < 0x80:
        out.append(value)
    elif value < 1 << 8:
        out += b"\xcc" + _BE["B"].pack(value)
    elif value < 1 << 16:
        out += b"\xcd" + _BE["H"].pack(value)
    elif value < 1 << 32:
        out += b"\xce" + _BE["I"].pack(value)
    else:
        out += b"\xcf" + _BE["Q"].pack(value)


def pack_float_array(out: bytearray, values) -> None:
    """Pack a sequence of numbers as an array of float32 - the representation
    ``std::vector<float>`` expects."""
    pack_array_header(out, len(values))
    pack = _F32.pack
    for v in values:
        out += b"\xca"
        out += pack(v)


def pack_uint_array(out: bytearray, values) -> None:
    pack_array_header(out, len(values))
    for v in values:
        pack_uint(out, v)


# ----------------------------------------------------------------------------
# decoding
# ----------------------------------------------------------------------------


class _Decoder:
    __slots__ = ("data", "pos")

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def _take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise MsgpackError("truncated msgpack payload")
        chunk = self.data[self.pos : end]
        self.pos = end
        return chunk

    def _num(self, key: str):
        s = _BE[key]
        return s.unpack(self._take(s.size))[0]

    def decode(self) -> Any:
        if self.pos >= len(self.data):
            raise MsgpackError("truncated msgpack payload")
        b = self.data[self.pos]
        self.pos += 1

        # single-byte-tagged encodings
        if b <= 0x7F:
            return b                       # positive fixint
        if b >= 0xE0:
            return b - 0x100               # negative fixint
        if 0x80 <= b <= 0x8F:
            return self._map(b & 0x0F)     # fixmap
        if 0x90 <= b <= 0x9F:
            return self._array(b & 0x0F)   # fixarray
        if 0xA0 <= b <= 0xBF:
            return self._str(b & 0x1F)     # fixstr

        if b == 0xC0:
            return None
        if b == 0xC2:
            return False
        if b == 0xC3:
            return True
        if b == 0xC4:
            return self._take(self._num("B"))
        if b == 0xC5:
            return self._take(self._num("H"))
        if b == 0xC6:
            return self._take(self._num("I"))
        if b == 0xCA:
            return self._num("f")
        if b == 0xCB:
            return self._num("d")
        if b == 0xCC:
            return self._num("B")
        if b == 0xCD:
            return self._num("H")
        if b == 0xCE:
            return self._num("I")
        if b == 0xCF:
            return self._num("Q")
        if b == 0xD0:
            return self._num("b")
        if b == 0xD1:
            return self._num("h")
        if b == 0xD2:
            return self._num("i")
        if b == 0xD3:
            return self._num("q")
        if b == 0xD9:
            return self._str(self._num("B"))
        if b == 0xDA:
            return self._str(self._num("H"))
        if b == 0xDB:
            return self._str(self._num("I"))
        if b == 0xDC:
            return self._array(self._num("H"))
        if b == 0xDD:
            return self._array(self._num("I"))
        if b == 0xDE:
            return self._map(self._num("H"))
        if b == 0xDF:
            return self._map(self._num("I"))

        raise MsgpackError(f"unsupported msgpack type byte 0x{b:02x}")

    def _str(self, n: int) -> str:
        return self._take(n).decode("utf-8", errors="replace")

    def _array(self, n: int) -> list:
        return [self.decode() for _ in range(n)]

    def _map(self, n: int) -> dict:
        # Keys may be ints (the id-keyed maps in a search response) as well as
        # strings, so no key-type restriction is imposed here.
        return {self.decode(): self.decode() for _ in range(n)}


def unpackb(data: bytes) -> Any:
    """Decode one msgpack value from ``data``."""
    return _Decoder(data).decode()
