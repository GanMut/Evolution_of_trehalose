"""FASTA utilities: head, subset, sample, transcribe, clean headers, tag organisms, slice, count.

Replaces: first_n_seqs.py, get_sequence.py, remove_sequence.py, randomq.py, transcribe.py,
underscore.py, edited_genomes.py, domains.py, gene_count.py, grep_headers.py
"""

from __future__ import annotations

import argparse
import random
import re
from collections.abc import Iterable, Iterator
from itertools import islice
from pathlib import Path

from .io import Record, log, read_fasta, read_list, write_fasta

# Characters Newick/iTOL tools choke on in leaf labels.
_UNSAFE = re.compile(r"[=+#()\[\]:;,'\s]")
# The narrower set the original iTOL scripts replaced (spaces kept).
_UNSAFE_ITOL = re.compile(r"[=+#()]")


def head(records: Iterable[Record], n: int) -> Iterator[Record]:
    return islice(records, n)


def subset(
    records: Iterable[Record], keys: Iterable[str], by: str = "id", exclude: bool = False
) -> Iterator[Record]:
    """Keep (or drop, with ``exclude``) records whose id/full header is in ``keys``.

    Uses a set, so lookups are O(1) instead of O(n) list scans in the old scripts.
    """
    wanted = set(keys)
    get = (lambda r: r.id) if by == "id" else (lambda r: r.header)
    for rec in records:
        if (get(rec) in wanted) != exclude:
            yield rec


def sample(records: Iterable[Record], k: int, seed: int | None = None) -> list[Record]:
    """Uniform random sample of ``k`` records in a single pass (reservoir sampling).

    The old randomq.py read the file twice and tested ``id in list`` for every record,
    which is O(n*k); this is O(n) and keeps only ``k`` records in memory.
    """
    rng = random.Random(seed)
    reservoir: list[tuple[int, Record]] = []
    for i, rec in enumerate(records):
        if i < k:
            reservoir.append((i, rec))
        else:
            j = rng.randint(0, i)
            if j < k:
                reservoir[j] = (i, rec)
    reservoir.sort(key=lambda t: t[0])  # keep original file order
    return [rec for _, rec in reservoir]


def transcribe(records: Iterable[Record]) -> Iterator[Record]:
    table = str.maketrans("Tt", "Uu")
    for rec in records:
        yield Record(rec.header, rec.seq.translate(table))


def sanitize_label(text: str, repl: str = "_", strict: bool = True) -> str:
    """Make a label tree-safe.

    ``strict``: spaces and ``=+#()[]:;,'`` become ``_``. Otherwise only ``=+#()`` are replaced,
    matching the original iTOL scripts (use whichever matches your tree's leaf names).
    """
    return (_UNSAFE if strict else _UNSAFE_ITOL).sub(repl, text.strip())


def clean_headers(records: Iterable[Record], mode: str = "underscore") -> Iterator[Record]:
    for rec in records:
        if mode == "underscore":
            header = rec.header.strip().replace(" ", "_")
        else:
            header = sanitize_label(rec.header)
        yield Record(header, rec.seq)


def tag_organism(records: Iterable[Record], organism: str) -> Iterator[Record]:
    """Append ``[organism]`` to every header (existing ``[...]`` become ``(...)``)."""
    for rec in records:
        header = rec.header.strip().replace("[", "(").replace("]", ")")
        yield Record(f"{header} [{organism}]", rec.seq)


def slice_region(rec: Record, start: int, end: int) -> Record:
    """1-based, inclusive coordinates (as reported by InterPro/Pfam)."""
    if start < 1 or end < start:
        raise ValueError("require 1 <= start <= end")
    return Record(f"{rec.id}:{start}-{end}", rec.seq[start - 1 : end])


def count(path: str | Path) -> int:
    return sum(1 for _ in read_fasta(path))


# ----------------------------------------------------------------------------- CLI
def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("fasta", help="FASTA utilities")
    s = p.add_subparsers(dest="cmd", required=True)

    c = s.add_parser("head", help="first N sequences")
    c.add_argument("fasta")
    c.add_argument("-n", type=int, required=True)
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: write_fasta(head(read_fasta(a.fasta), a.n), a.out))

    c = s.add_parser("subset", help="keep/drop sequences listed in a file")
    c.add_argument("fasta")
    c.add_argument("-l", "--list", required=True, help="file of IDs or full headers")
    c.add_argument("--by", choices=["id", "header"], default="id")
    c.add_argument("-v", "--exclude", action="store_true", help="drop listed sequences")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=_cli_subset)

    c = s.add_parser("sample", help="random sample of K sequences")
    c.add_argument("fasta")
    c.add_argument("-k", type=int, required=True)
    c.add_argument("--seed", type=int, default=None)
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: write_fasta(sample(read_fasta(a.fasta), a.k, a.seed), a.out))

    c = s.add_parser("transcribe", help="DNA -> RNA (T -> U)")
    c.add_argument("fasta")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: write_fasta(transcribe(read_fasta(a.fasta)), a.out))

    c = s.add_parser("clean-headers", help="make headers tree-friendly")
    c.add_argument("fasta")
    c.add_argument(
        "--mode",
        choices=["underscore", "safe"],
        default="underscore",
        help="underscore: spaces->_ ; safe: also replace =+#()[]:;,'",
    )
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: write_fasta(clean_headers(read_fasta(a.fasta), a.mode), a.out))

    c = s.add_parser("tag-organism", help="append [Organism] to headers")
    g = c.add_mutually_exclusive_group(required=True)
    g.add_argument("--map", help="file with lines 'path/to/genome.faa:Organism name'")
    g.add_argument("--fasta", help="single FASTA (use with --organism)")
    c.add_argument("--organism")
    c.add_argument("-d", "--outdir", default=".")
    c.set_defaults(func=_cli_tag)

    c = s.add_parser("slice", help="extract a region (e.g. a domain) from one sequence")
    c.add_argument("fasta")
    c.add_argument("--id", help="sequence id (default: first record)")
    c.add_argument("--start", type=int, required=True, help="1-based, inclusive")
    c.add_argument("--end", type=int, required=True, help="1-based, inclusive")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=_cli_slice)

    c = s.add_parser("count", help="number of sequences per file")
    c.add_argument("fasta", nargs="+")
    c.set_defaults(func=lambda a: [print(f"{f}\t{count(f)}") for f in a.fasta])


def _cli_subset(a):
    n = write_fasta(subset(read_fasta(a.fasta), read_list(a.list), a.by, a.exclude), a.out)
    log(f"wrote {n} sequences")


def _cli_tag(a):
    if a.map:
        pairs = [line.rsplit(":", 1) for line in read_list(a.map)]
    else:
        if not a.organism:
            raise SystemExit("--organism is required with --fasta")
        pairs = [(a.fasta, a.organism)]
    for path, org in pairs:
        out = Path(a.outdir) / f"{sanitize_label(org)}.fasta"
        n = write_fasta(tag_organism(read_fasta(path), org.strip()), out)
        log(f"{path} -> {out} ({n} sequences)")


def _cli_slice(a):
    for rec in read_fasta(a.fasta):
        if a.id is None or rec.id == a.id:
            write_fasta([slice_region(rec, a.start, a.end)], a.out)
            return
    raise SystemExit(f"sequence {a.id!r} not found")
