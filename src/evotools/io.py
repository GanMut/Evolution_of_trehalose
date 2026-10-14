"""Small, dependency-free I/O helpers (FASTA, plain lists, gzip/stdin transparency)."""

from __future__ import annotations

import gzip
import io
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import IO, NamedTuple


class Record(NamedTuple):
    """A FASTA record. ``header`` is the full definition line without the leading ``>``."""

    header: str
    seq: str

    @property
    def id(self) -> str:
        return self.header.split(None, 1)[0] if self.header else ""

    @property
    def description(self) -> str:
        return self.header


class _Unclosable(io.TextIOBase):
    """Wrap stdin/stdout so ``with open_text('-')`` does not close the real stream."""

    def __init__(self, stream: IO[str]):
        self._s = stream

    def __iter__(self):
        return iter(self._s)

    def read(self, *a):
        return self._s.read(*a)

    def readline(self, *a):
        return self._s.readline(*a)

    def write(self, s: str) -> int:
        return self._s.write(s)

    def close(self) -> None:
        self._s.flush()


def open_text(path: str | Path, mode: str = "r") -> IO[str]:
    """Open a text file, transparently handling ``-`` (stdin/stdout) and ``.gz``."""
    path = str(path)
    if path == "-":
        return _Unclosable(sys.stdin if "r" in mode else sys.stdout)
    if path.endswith(".gz"):
        return gzip.open(path, mode + "t" if "t" not in mode else mode, encoding="utf-8")
    if "w" in mode or "a" in mode:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    return open(path, mode, encoding="utf-8")


def read_fasta(path_or_handle: str | Path | IO[str]) -> Iterator[Record]:
    """Stream FASTA records. Much faster than Bio.SeqIO for simple header/sequence access."""
    handle = (
        open_text(path_or_handle) if isinstance(path_or_handle, (str, Path)) else path_or_handle
    )
    header: str | None = None
    chunks: list[str] = []
    try:
        for line in handle:
            if line.startswith(">"):
                if header is not None:
                    yield Record(header, "".join(chunks))
                header = line[1:].rstrip("\r\n")
                chunks = []
            elif header is not None:
                chunks.append(line.strip())
        if header is not None:
            yield Record(header, "".join(chunks))
    finally:
        if isinstance(path_or_handle, (str, Path)):
            handle.close()


def write_fasta(
    records: Iterable[Record], path_or_handle: str | Path | IO[str], width: int = 60
) -> int:
    """Write records; returns the number written. ``width=0`` writes one line per sequence."""
    own = isinstance(path_or_handle, (str, Path))
    handle = open_text(path_or_handle, "w") if own else path_or_handle
    n = 0
    try:
        for rec in records:
            handle.write(f">{rec.header}\n")
            seq = rec.seq
            if width and width > 0:
                for i in range(0, len(seq), width):
                    handle.write(seq[i : i + width] + "\n")
            else:
                handle.write(seq + "\n")
            n += 1
    finally:
        if own:
            handle.close()
    return n


def read_list(path: str | Path, unique: bool = False) -> list[str]:
    """Read a one-item-per-line file, stripping whitespace and skipping blank/comment lines."""
    out: list[str] = []
    seen: set[str] = set()
    with open_text(path) as fh:
        for line in fh:
            item = line.strip()
            if not item or item.startswith("#"):
                continue
            if unique:
                if item in seen:
                    continue
                seen.add(item)
            out.append(item)
    return out


def write_list(items: Iterable[str], path: str | Path) -> int:
    n = 0
    with open_text(path, "w") as fh:
        for item in items:
            fh.write(f"{item}\n")
            n += 1
    return n


def log(msg: str) -> None:
    """Progress messages go to stderr so stdout stays pipe-friendly."""
    print(msg, file=sys.stderr)
