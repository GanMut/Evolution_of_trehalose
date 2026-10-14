"""``evotools`` command-line entry point."""

from __future__ import annotations

import argparse
import sys

from . import (
    __version__,
    embed,
    fasta,
    gff,
    headers,
    itol,
    matrix,
    model,
    orthology,
    pipeline,
    plots,
    trees,
)

MODULES = [fasta, headers, orthology, matrix, itol, trees, gff, embed, plots, model, pipeline]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="evotools",
        description="Comparative genomics & phylogenomics toolkit (Evolution of Trehalose).",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")
    for mod in MODULES:
        mod.register(sub)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except BrokenPipeError:  # e.g. `evotools fasta head ... | head`
        sys.stderr.close()
    except FileNotFoundError as e:
        sys.exit(f"evotools: file not found: {e.filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
