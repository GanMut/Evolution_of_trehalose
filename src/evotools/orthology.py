"""Reciprocal best hits (RBH) and ortholog groups from all-vs-all BLAST/DIAMOND tables.

Replaces: best_blast_hit.py (fast_best_blast_hit.py) and ortholog_finder.py (ortholougsFinder.py).

Why it is faster
----------------
* The old ``bbh()`` looped over every genome pair, re-filtered the whole table for each pair
  and appended rows one by one with ``df.loc[len(df)] = ...`` -> roughly
  O(genomes^2 x hits) plus quadratic appends. Hours on large tables.
* ``ortholog_finder.py`` copied the table into every worker process, and appended to lists
  stored inside a ``multiprocessing.Manager().dict()`` -- those in-place appends are silently
  lost (Manager proxies don't propagate nested mutation), so some pairs went missing.

Here the whole computation is three vectorised pandas operations
(sort -> drop_duplicates -> self-merge), i.e. O(hits log hits), usually seconds.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd

from .io import log

BLAST6 = [
    "qseqid",
    "sseqid",
    "pident",
    "length",
    "mismatch",
    "gapopen",
    "qstart",
    "qend",
    "sstart",
    "send",
    "evalue",
    "bitscore",
]


def genome_key(ids: pd.Series, delim: str = "_", fields: int = 2) -> pd.Series:
    """Genome/organism of each sequence id = first ``fields`` parts split on ``delim``.

    ``GCF_000005845_WP_0001`` with delim ``_`` and fields 2 -> ``GCF_000005845``;
    ``EcoK12:b0001`` with delim ``:`` and fields 1 -> ``EcoK12``.
    """
    return ids.str.split(delim, n=fields, regex=False).str[:fields].str.join(delim)


def read_hits(path: str | Path, columns: list[str] | None = None) -> pd.DataFrame:
    path = str(path)
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    df = pd.read_csv(path, sep="\t", header=None, comment="#")
    names = columns or BLAST6[: df.shape[1]]
    df.columns = list(names) + [f"c{i}" for i in range(len(names), df.shape[1])]
    return df


def reciprocal_best_hits(
    hits: pd.DataFrame,
    score: str = "pident",
    delim: str = "_",
    fields: int = 2,
    query_genome: str | None = None,
    subject_genome: str | None = None,
) -> pd.DataFrame:
    """One row per RBH pair: protein_a, protein_b, genome_a, genome_b, score_ab, score_ba.

    For every query protein, its best hit *in each other genome* is taken (highest ``score``;
    ties broken by subject id for reproducibility). A pair is kept when both proteins are each
    other's best hit. Self-genome hits are ignored. Pairs are reported once (a < b).
    """
    df = hits[["qseqid", "sseqid", score]].copy()
    df["qseqid"] = df["qseqid"].astype(str)
    df["sseqid"] = df["sseqid"].astype(str)
    df[score] = pd.to_numeric(df[score], errors="coerce")
    df["qg"] = (
        hits[query_genome].astype(str) if query_genome else genome_key(df.qseqid, delim, fields)
    )
    df["sg"] = (
        hits[subject_genome].astype(str) if subject_genome else genome_key(df.sseqid, delim, fields)
    )
    df = df[df.qg != df.sg]

    best = df.sort_values(
        [score, "sseqid"], ascending=[False, True], kind="mergesort"
    ).drop_duplicates(["qseqid", "sg"])
    rev = best.rename(
        columns={"qseqid": "sseqid", "sseqid": "qseqid", score: "score_ba", "qg": "sg", "sg": "qg"}
    )
    rbh = best.merge(rev[["qseqid", "sseqid", "score_ba"]], on=["qseqid", "sseqid"])
    rbh = rbh[rbh.qseqid < rbh.sseqid]
    return (
        rbh.rename(
            columns={
                "qseqid": "protein_a",
                "sseqid": "protein_b",
                "qg": "genome_a",
                "sg": "genome_b",
                score: "score_ab",
            }
        )[["protein_a", "protein_b", "genome_a", "genome_b", "score_ab", "score_ba"]]
        .sort_values(["protein_a", "protein_b"])
        .reset_index(drop=True)
    )


def ortholog_groups(pairs: pd.DataFrame) -> pd.DataFrame:
    """Connected components of the RBH graph (union-find). Returns ``group, protein`` rows."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in zip(pairs.protein_a, pairs.protein_b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    members: dict[str, list[str]] = {}
    for p in parent:
        members.setdefault(find(p), []).append(p)
    groups = sorted((sorted(v) for v in members.values()), key=lambda g: (-len(g), g[0]))
    rows = [(f"OG{i:06d}", p) for i, g in enumerate(groups, 1) for p in g]
    return pd.DataFrame(rows, columns=["group", "protein"])


def _run_one(job: tuple) -> str:
    path, outdir, kw, columns, groups = job
    hits = read_hits(path, columns)
    rbh = reciprocal_best_hits(hits, **kw)
    out = Path(outdir) / f"{Path(path).name}.rbh.tsv"
    out.parent.mkdir(parents=True, exist_ok=True)
    rbh.to_csv(out, sep="\t", index=False)
    if groups:
        ortholog_groups(rbh).to_csv(
            Path(outdir) / f"{Path(path).name}.groups.tsv", sep="\t", index=False
        )
    return f"{path}: {len(rbh)} RBH pairs -> {out}"


# ----------------------------------------------------------------------------- CLI
def register(sub: argparse._SubParsersAction) -> None:
    c = sub.add_parser("rbh", help="reciprocal best hits / ortholog groups from BLAST tables")
    c.add_argument("inputs", nargs="+", help="BLAST outfmt 6 TSV(s) or .parquet; dirs allowed")
    c.add_argument("-d", "--outdir", default="rbh_out")
    c.add_argument(
        "--score",
        default="pident",
        help="column to rank hits by (pident [default, as before] or bitscore)",
    )
    c.add_argument("--columns", nargs="+", help="column names if not standard outfmt 6")
    c.add_argument("--genome-delim", default="_")
    c.add_argument(
        "--genome-fields", type=int, default=2, help="genome = first N fields of the sequence id"
    )
    c.add_argument("--query-genome-col", help="use this column as query genome (e.g. qOrg)")
    c.add_argument("--subject-genome-col", help="use this column as subject genome")
    c.add_argument("--groups", action="store_true", help="also write ortholog groups")
    c.add_argument("-j", "--jobs", type=int, default=1, help="files processed in parallel")
    c.set_defaults(func=_cli)


def _cli(a):
    files: list[Path] = []
    for item in a.inputs:
        p = Path(item)
        files.extend(sorted(x for x in p.iterdir() if x.is_file()) if p.is_dir() else [p])
    kw = dict(
        score=a.score,
        delim=a.genome_delim,
        fields=a.genome_fields,
        query_genome=a.query_genome_col,
        subject_genome=a.subject_genome_col,
    )
    jobs = [(str(f), a.outdir, kw, a.columns, a.groups) for f in files]
    if a.jobs > 1 and len(jobs) > 1:
        with ProcessPoolExecutor(a.jobs) as ex:
            for msg in ex.map(_run_one, jobs):
                log(msg)
    else:
        for job in jobs:
            log(_run_one(job))
