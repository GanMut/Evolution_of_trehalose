"""Organism-aware header handling for NCBI-style headers (``>XP_123.1 protein name [Organism]``).

Replaces: get_org.py, get_org_headers.py, change_headers.py, uniq_id.py, and the
"grep every organism name through the whole file" loops in auto.py.

Speed-up: the old scripts launched one ``grep`` over a multi-GB file *per organism or ID*
(O(names x file size)). Here every input is read exactly once and matched via dict/set
lookups (O(file size)).
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Iterable, Iterator
from pathlib import Path

import pandas as pd

from .fasta import sanitize_label
from .io import Record, log, open_text, read_fasta, write_fasta, write_list

_ORG = re.compile(r"\[([^\[\]]+)\]\s*$")
DEFAULT_EXCLUDE = ("hypothetical", "uncharacterized", "multispecies")


def organism_of(header: str) -> str | None:
    """Organism from the *last* ``[...]`` of a header (``None`` if absent)."""
    m = _ORG.search(header.split("<>")[0].split("\x01")[0])
    return m.group(1).strip() if m else None


def organisms_in(lines: Iterable[str]) -> list[str]:
    """Sorted unique organism names found in headers/lines."""
    found = {o for o in (organism_of(line.rstrip("\n")) for line in lines) if o}
    return sorted(found)


def id_to_organism(headers: Iterable[str], ids: set[str] | None = None) -> dict[str, str]:
    """Map sequence id -> organism (first occurrence wins). ``ids`` restricts what's kept."""
    out: dict[str, str] = {}
    for h in headers:
        h = h.lstrip(">").rstrip("\n")
        sid = h.split(None, 1)[0] if h else ""
        if ids is not None and sid not in ids:
            continue
        if sid not in out:
            org = organism_of(h)
            if org:
                out[sid] = org
    return out


def rename_to_organism(
    records: Iterable[Record],
    mapping: dict[str, str],
    safe: bool = False,
    keep_unmapped: bool = True,
) -> Iterator[Record]:
    for rec in records:
        org = mapping.get(rec.id)
        if org is None:
            if keep_unmapped:
                yield rec
            continue
        yield Record(sanitize_label(org) if safe else org, rec.seq)


def _excluded(title: str, exclude: Iterable[str]) -> bool:
    t = title.lower()
    return any(word in t for word in exclude)


def best_per_organism_blast(
    path: str | Path,
    exclude: Iterable[str] = DEFAULT_EXCLUDE,
    id_col: int = 0,
    title_col: int = -1,
    score_col: int | None = None,
) -> pd.DataFrame:
    """Best BLAST hit per organism from tabular output that includes ``salltitles``.

    Default columns: ``sseqid ... salltitles`` (title last). If ``score_col`` is given, hits are
    ranked by it (descending); otherwise BLAST's own order (best first) is used.
    """
    rows = []
    with open_text(path) as fh:
        for order, line in enumerate(fh):
            if not line.strip() or line.startswith("#"):
                continue
            f = line.rstrip("\n").split("\t")
            title = f[title_col].split("<>")[0]
            if _excluded(title, exclude):
                continue
            org = organism_of(title)
            if not org:
                continue
            score = float(f[score_col]) if score_col is not None else -order
            rows.append((f[id_col].split()[0], org, score))
    df = pd.DataFrame(rows, columns=["id", "organism", "score"])
    if df.empty:
        return df
    return (
        df.sort_values("score", ascending=False, kind="stable")
        .drop_duplicates("organism")
        .reset_index(drop=True)
    )


def read_domtblout(path: str | Path) -> pd.DataFrame:
    """Parse ``hmmsearch --domtblout`` (target, full-seq E-value/score, description)."""
    rows = []
    with open_text(path) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            f = line.split(None, 22)
            desc = f[22].rstrip("\n") if len(f) > 22 else ""
            rows.append((f[0], float(f[6]), float(f[7]), float(f[12]), desc))
    return pd.DataFrame(rows, columns=["target", "evalue", "score", "i_evalue", "description"])


def best_per_organism_hmmer(
    path: str | Path, max_evalue: float = 1e-4, exclude: Iterable[str] = ()
) -> pd.DataFrame:
    """Best-scoring target per organism from a domtblout (full-sequence E-value filter)."""
    df = read_domtblout(path)
    if df.empty:
        return df.assign(organism=[])
    df = df[df.evalue < max_evalue].copy()
    if exclude:
        df = df[~df.description.map(lambda d: _excluded(d, exclude))]
    df["organism"] = df.description.map(organism_of)
    df = df.dropna(subset=["organism"])
    return (
        df.sort_values(["evalue", "score"], ascending=[True, False])
        .drop_duplicates("target")
        .drop_duplicates("organism")
        .reset_index(drop=True)
    )


# ----------------------------------------------------------------------------- CLI
def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("headers", help="organism-aware header tools")
    s = p.add_subparsers(dest="cmd", required=True)

    c = s.add_parser("organisms", help="unique organism names from [..] in headers")
    c.add_argument("file", help="FASTA, header list or BLAST/HMMER output")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: write_list(organisms_in(open_text(a.file)), a.out))

    c = s.add_parser("rename", help="replace sequence IDs with organism names")
    c.add_argument("fasta", help="alignment / FASTA whose IDs should be renamed")
    c.add_argument(
        "--headers", required=True, help="FASTA db or header list containing '>ID ... [Organism]'"
    )
    c.add_argument("--safe", action="store_true", help="make names Newick/iTOL safe")
    c.add_argument("--drop-unmapped", action="store_true")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=_cli_rename)

    c = s.add_parser("best-per-organism", help="top hit per organism from BLAST or HMMER")
    c.add_argument("file")
    c.add_argument("--format", choices=["blast", "domtblout"], required=True)
    c.add_argument("--max-evalue", type=float, default=1e-4, help="domtblout only")
    c.add_argument(
        "--exclude",
        nargs="*",
        default=None,
        help="skip hits whose title contains these words (case-insensitive). "
        "Default for blast: hypothetical uncharacterized multispecies; "
        "for domtblout: none",
    )
    c.add_argument("-o", "--out", default="-", help="output list of IDs")
    c.add_argument("--table", help="also write id/organism table (TSV)")
    c.set_defaults(func=_cli_best)


def _cli_rename(a):
    wanted = {r.id for r in read_fasta(a.fasta)}
    with open_text(a.headers) as fh:
        # Sequence lines never contain '[', so this reads FASTA dbs and header lists alike.
        mapping = id_to_organism((ln for ln in fh if "[" in ln), wanted)
    log(f"mapped {len(mapping)}/{len(wanted)} IDs to organisms")
    write_fasta(
        rename_to_organism(read_fasta(a.fasta), mapping, a.safe, not a.drop_unmapped), a.out
    )


def _cli_best(a):
    if a.format == "blast":
        df = best_per_organism_blast(a.file, DEFAULT_EXCLUDE if a.exclude is None else a.exclude)
        ids = df["id"]
    else:
        df = best_per_organism_hmmer(a.file, a.max_evalue, a.exclude or ())
        ids = df["target"]
    write_list(ids, a.out)
    if a.table:
        df.to_csv(a.table, sep="\t", index=False)
    log(f"{len(df)} organisms")
