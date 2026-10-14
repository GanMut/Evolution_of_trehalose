"""Write iTOL annotation datasets (colour strips, clade colours, HGT connections).

Replaces: color_strips.py, green_lines.py, HGT_donor_recipient.py.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from .fasta import sanitize_label
from .io import log, open_text, read_list

# Default palette from the original colour-strip script (taxonomic groups of eukaryotes).
DEFAULT_COLORS = [
    "#E861A0",
    "#556B2F",
    "#FFA500",
    "#673147",
    "#00CED1",
    "#FFD700",
    "#FF7F50",
    "#A020F0",
    "#0000FF",
    "#800000",
]


def parse_groups(specs: list[str]) -> list[tuple[str, str, Path]]:
    """``LABEL=FILE`` or ``LABEL:#COLOR=FILE`` -> (label, colour, path)."""
    groups = []
    for i, spec in enumerate(specs):
        left, _, path = spec.partition("=")
        if not path:
            raise SystemExit(f"bad --group {spec!r}; expected LABEL[:#COLOR]=FILE")
        label, _, color = left.partition(":")
        groups.append((label, color or DEFAULT_COLORS[i % len(DEFAULT_COLORS)], Path(path)))
    return groups


def colorstrip(
    groups: list[tuple[str, str, Path]],
    title: str = "Groups",
    strip_width: int = 25,
    safe: bool = True,
    strict: bool = False,
) -> str:
    n = len(groups)
    labels = "\t".join(g[0] for g in groups)
    colors = "\t".join(g[1] for g in groups)
    lines = [
        "DATASET_COLORSTRIP",
        "SEPARATOR TAB",
        f"DATASET_LABEL\t{title}",
        "COLOR\t#ffffff",
        "COLOR_BRANCHES\t1",
        f"STRIP_WIDTH\t{strip_width}",
        "MARGIN\t0",
        "BORDER_WIDTH\t1",
        "BORDER_COLOR\t#000",
        "SHOW_INTERNAL\t0",
        "LEGEND_TITLE\tLegend",
        "LEGEND_SHAPES\t" + "\t".join(["1"] * n),
        f"LEGEND_COLORS\t{colors}",
        f"LEGEND_LABELS\t{labels}",
        "LEGEND_SHAPE_SCALES\t" + "\t".join(["1"] * n),
        "LEGEND_SHAPE_INVERT\t" + "\t".join(["0"] * n),
        "",
        "DATA",
    ]
    for label, color, path in groups:
        for name in read_list(path):
            leaf = sanitize_label(name, strict=strict) if safe else name
            lines.append(f"{leaf}\t{color}\t{label}")
    return "\n".join(lines) + "\n"


def tree_colors(
    groups: list[tuple[str, str, Path]],
    kind: str = "clade",
    style: str = "normal",
    width: float = 1,
    safe: bool = True,
    strict: bool = False,
) -> str:
    lines = ["TREE_COLORS", "SEPARATOR TAB", "DATA"]
    for label, color, path in groups:
        for name in read_list(path):
            leaf = sanitize_label(name, strict=strict) if safe else name
            if kind == "clade":
                lines.append(f"{leaf}\tclade\t{color}\t{style}\t{width}")
            else:
                lines.append(f"{leaf}\t{kind}\t{color}\t{label}")
    return "\n".join(lines) + "\n"


_MAP = re.compile(r"Most Frequent mapping --> ([^,\]]+), (\d+) times")
_REC = re.compile(r"Most Frequent recipient --> ([^,\]]+)")


def parse_rangerdtl(path: str | Path) -> pd.DataFrame:
    """Transfers (donor != recipient) from RANGER-DTL AggregateRanger summaries."""
    rows = []
    with open_text(path) as fh:
        for line in fh:
            m, r = _MAP.search(line), _REC.search(line)
            if m and r:
                donor, recipient = m.group(1).strip(), r.group(1).strip()
                if donor != recipient:
                    rows.append((donor, recipient, int(m.group(2))))
    return pd.DataFrame(rows, columns=["donor", "recipient", "reconciliations"])


def connections(df: pd.DataFrame, title: str, color: str = "#f90d0d", max_width: int = 3) -> str:
    lines = [
        "DATASET_CONNECTION",
        "SEPARATOR TAB",
        f"DATASET_LABEL\t{title} connections",
        "COLOR\t#000000",
        "DRAW_ARROWS\t1",
        "ARROW_SIZE\t10",
        "LOOP_SIZE\t100",
        f"MAXIMUM_LINE_WIDTH\t{max_width}",
        "CENTER_CURVES\t1",
        "ALIGN_TO_LABELS\t0",
        "",
        "DATA",
    ]
    lines += [f"{d}\t{r}\t{n}\t{color}\tnormal" for d, r, n in df.itertuples(index=False)]
    return "\n".join(lines) + "\n"


def _write(text: str, out: str) -> None:
    with open_text(out, "w") as fh:
        fh.write(text)


# ----------------------------------------------------------------------------- CLI
def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("itol", help="iTOL annotation files")
    s = p.add_subparsers(dest="cmd", required=True)
    grp_help = "LABEL[:#COLOR]=FILE (file lists leaf names); repeatable"

    c = s.add_parser("colorstrip", help="DATASET_COLORSTRIP from name lists")
    c.add_argument("--group", action="append", required=True, help=grp_help)
    c.add_argument("--title", default="Groups")
    _name_opts(c)
    c.set_defaults(
        func=lambda a: _write(
            colorstrip(
                parse_groups(a.group), a.title, safe=a.names != "raw", strict=a.names == "strict"
            ),
            a.out,
        )
    )

    c = s.add_parser("clades", help="TREE_COLORS (colour clades/branches/labels)")
    c.add_argument("--group", action="append", required=True, help=grp_help)
    c.add_argument("--kind", choices=["clade", "range", "label"], default="clade")
    _name_opts(c)
    c.set_defaults(
        func=lambda a: _write(
            tree_colors(
                parse_groups(a.group), a.kind, safe=a.names != "raw", strict=a.names == "strict"
            ),
            a.out,
        )
    )

    c = s.add_parser("hgt", help="donor->recipient table + DATASET_CONNECTION from RANGER-DTL")
    c.add_argument("rangerdtl", help="AggregateRanger output")
    c.add_argument("--name", required=True, help="KO / pathway name used as dataset label")
    c.add_argument("--table", required=True, help="output TSV of transfers")
    c.add_argument("--connections", required=True, help="output iTOL connection dataset")
    c.set_defaults(func=_cli_hgt)


def _name_opts(c: argparse.ArgumentParser) -> None:
    c.add_argument(
        "--names",
        choices=["itol", "strict", "raw"],
        default="itol",
        help="leaf-name cleaning: itol replaces =+#() (original behaviour), "
        "strict also spaces/brackets/punctuation, raw leaves names as-is",
    )
    c.add_argument("-o", "--out", default="-")


def _cli_hgt(a):
    df = parse_rangerdtl(a.rangerdtl)
    Path(a.table).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.table, sep="\t", index=False)
    _write(connections(df, a.name), a.connections)
    log(f"{len(df)} transfers")
