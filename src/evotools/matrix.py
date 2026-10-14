"""Gene presence/absence matrix and simple set comparisons of name lists.

Replaces: gene_presence_absence_matrix.py (matrix.py) and intersection.py.

The original matrix script built 37 lists with ``exec()`` and tested ``name in list``
(O(organisms x genes x list length)); here each gene is a ``set`` and the matrix is built
in one vectorised step. Columns are named after the gene directory (not ``gene1..geneN``),
so you can tell which column is which.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .io import log, read_list, write_list


def find_gene_files(dirs: list[str], pattern: str) -> list[Path]:
    files: list[Path] = []
    for d in dirs:
        p = Path(d)
        if not p.is_dir():
            log(f"warning: {d} is not a directory, skipped")
            continue
        files.extend(sorted(p.rglob(pattern)))
    return files


def presence_absence(organisms: list[str], genes: dict[str, set[str]]) -> pd.DataFrame:
    """Rows = organisms (input order), columns = genes, values 0/1."""
    data = {g: [int(o in members) for o in organisms] for g, members in genes.items()}
    return pd.DataFrame(data, index=pd.Index(organisms, name="organism"))


def _gene_name(path: Path, by: str) -> str:
    return path.parent.name if by == "dir" else path.stem


def compare(a: list[str], b: list[str]) -> dict[str, list[str]]:
    """Order-preserving intersection / differences of two lists."""
    sa, sb = set(a), set(b)
    return {
        "common": [x for x in dict.fromkeys(a) if x in sb],
        "only_a": [x for x in dict.fromkeys(a) if x not in sb],
        "only_b": [x for x in dict.fromkeys(b) if x not in sa],
    }


# ----------------------------------------------------------------------------- CLI
def register(sub: argparse._SubParsersAction) -> None:
    c = sub.add_parser("matrix", help="gene presence/absence matrix across organisms")
    c.add_argument("--organisms", required=True, help="file with organism names (rows)")
    src = c.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--gene-files", nargs="+", help="one file per gene listing organisms that have it"
    )
    src.add_argument("--search-dirs", nargs="+", help="directories searched recursively")
    c.add_argument(
        "--pattern",
        default="uniq_organism_final.txt",
        help="file name to look for with --search-dirs",
    )
    c.add_argument(
        "--name-by",
        choices=["dir", "file"],
        default=None,
        help="column name from parent dir or file stem "
        "(default: dir for --search-dirs, file for --gene-files)",
    )
    c.add_argument("-o", "--out", required=True, help="output TSV")
    c.set_defaults(func=_cli_matrix)

    c = sub.add_parser("compare", help="intersection / differences of two name lists")
    c.add_argument("a")
    c.add_argument("b")
    c.add_argument("--common", help="write common items here")
    c.add_argument("--only-a", help="write items only in A here")
    c.add_argument("--only-b", help="write items only in B here")
    c.set_defaults(func=_cli_compare)


def _cli_matrix(a):
    organisms = read_list(a.organisms, unique=True)
    files = (
        [Path(f) for f in a.gene_files]
        if a.gene_files
        else find_gene_files(a.search_dirs, a.pattern)
    )
    if not files:
        raise SystemExit("no gene files found")
    by = a.name_by or ("dir" if a.search_dirs else "file")
    genes: dict[str, set[str]] = {}
    for f in files:
        name = _gene_name(f, by)
        if name in genes:  # keep columns unique
            name = f"{name}_{len(genes)}"
        genes[name] = set(read_list(f))
    df = presence_absence(organisms, genes)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, sep="\t")
    log(f"matrix: {df.shape[0]} organisms x {df.shape[1]} genes -> {a.out}")


def _cli_compare(a):
    res = compare(read_list(a.a), read_list(a.b))
    print(
        f"common\t{len(res['common'])}\nonly_in_{Path(a.a).name}\t{len(res['only_a'])}\n"
        f"only_in_{Path(a.b).name}\t{len(res['only_b'])}"
    )
    for key, dest in (("common", a.common), ("only_a", a.only_a), ("only_b", a.only_b)):
        if dest:
            write_list(res[key], dest)
