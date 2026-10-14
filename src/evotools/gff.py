"""GFF3 helpers: rRNA genes to BED, CDS protein IDs per genome.

Replaces: rRNA_bed.py, headers.py (BED column reordering), check.py.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Iterator
from pathlib import Path

from .io import log, open_text

_ATTR = re.compile(r"([^=;]+)=([^;]*)")


def iter_features(path: str | Path) -> Iterator[list[str]]:
    with open_text(path) as fh:
        for line in fh:
            if line.startswith("##FASTA"):
                break
            if line.startswith("#") or not line.strip():
                continue
            f = line.rstrip("\n").split("\t")
            if len(f) == 9:
                yield f


def attributes(field: str) -> dict[str, str]:
    return {k.strip(): v for k, v in _ATTR.findall(field)}


def genome_name(path: str | Path) -> str:
    name = Path(path).name
    for suffix in (".gz", ".gff3", ".gff", "_genomic"):
        name = name.removesuffix(suffix)
    return name


def rrna_bed(
    path: str | Path, product: str = "18S ribosomal RNA", first_only: bool = True
) -> list[str]:
    """BED6 lines for rRNA features whose ``product`` contains ``product``."""
    out = []
    g = genome_name(path)
    for f in iter_features(path):
        if f[2] != "rRNA":
            continue
        attrs = attributes(f[8])
        if product.lower() not in attrs.get("product", "").lower():
            continue
        name = f"{g}|{attrs.get('ID', attrs.get('locus_tag', '.'))}"
        out.append("\t".join([f[0], str(int(f[3]) - 1), f[4], name, "0", f[6]]))
        if first_only:
            break
    return out


def cds_protein_ids(path: str | Path, skip_pseudo: bool = True) -> list[str]:
    ids = []
    for f in iter_features(path):
        if f[2] != "CDS":
            continue
        attrs = attributes(f[8])
        if skip_pseudo and (attrs.get("pseudo") == "true" or "pseudo" in f[8].lower()):
            continue
        pid = attrs.get("protein_id")
        if pid:
            ids.append(pid)
    return list(dict.fromkeys(ids))


def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("gff", help="GFF3 helpers")
    s = p.add_subparsers(dest="cmd", required=True)

    c = s.add_parser("rrna-bed", help="rRNA genes (e.g. 18S/16S) as BED6")
    c.add_argument("gff", nargs="+")
    c.add_argument("--product", default="18S ribosomal RNA")
    c.add_argument("--all", action="store_true", help="all copies, not just the first")
    c.add_argument("-o", "--out", default="-", help="combined BED")
    c.set_defaults(func=_cli_bed)

    c = s.add_parser("cds-ids", help="protein IDs of CDS features (genome<TAB>protein_id)")
    c.add_argument("gff", nargs="+")
    c.add_argument("--keep-pseudo", action="store_true")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=_cli_cds)


def _cli_bed(a):
    missing = 0
    with open_text(a.out, "w") as out:
        for g in a.gff:
            lines = rrna_bed(g, a.product, not a.all)
            missing += not lines
            for line in lines:
                out.write(line + "\n")
    if missing:
        log(f"{missing} file(s) had no '{a.product}' rRNA")


def _cli_cds(a):
    with open_text(a.out, "w") as out:
        for g in a.gff:
            name = genome_name(g)
            for pid in cds_protein_ids(g, not a.keep_pseudo):
                out.write(f"{name}\t{pid}\n")
