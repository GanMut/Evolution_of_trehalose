"""Newick helpers.

Replaces: convert_es_to_decimals.py, tree_from_optroot.py.
"""

from __future__ import annotations

import argparse
import re

from .io import open_text

# Any float in scientific notation: 2.9e-06, 1E+3, -4.1e-10, 5e-7
_SCI = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?[eE][-+]?\d+")


def sci_to_decimal(text: str, precision: int = 10) -> str:
    """Rewrite scientific-notation branch lengths as plain decimals (some viewers need it).

    The old regex only matched ``d.de-d``; this also handles ``5e-7``, ``1E+3`` etc.
    """

    def fmt(m: re.Match) -> str:
        out = f"{float(m.group()):.{precision}f}"
        return out.rstrip("0").rstrip(".") if "." in out else out

    return _SCI.sub(fmt, text)


def optroot_tree(text: str) -> str:
    """The rooted Newick tree from OptRoot output (last line starting with ``(``)."""
    trees = [ln.strip() for ln in text.splitlines() if ln.startswith("(")]
    if not trees:
        raise ValueError("no Newick tree found (no line starting with '(')")
    return trees[-1] + "\n"


def _transform(fn, a):
    with open_text(a.input) as fh:
        text = fh.read()
    with open_text(a.out, "w") as fh:
        fh.write(fn(text))


def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("tree", help="Newick helpers")
    s = p.add_subparsers(dest="cmd", required=True)

    c = s.add_parser("decimals", help="scientific notation -> decimals in a tree")
    c.add_argument("input")
    c.add_argument("--precision", type=int, default=10)
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: _transform(lambda t: sci_to_decimal(t, a.precision), a))

    c = s.add_parser("optroot", help="extract the rooted tree from OptRoot output")
    c.add_argument("input")
    c.add_argument("-o", "--out", default="-")
    c.set_defaults(func=lambda a: _transform(optroot_tree, a))
