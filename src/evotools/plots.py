"""UpSet plots of which organisms carry which pathways/enzymes.

Replaces: upset_plot.py and notebooks/upset_plot_for_pathways.ipynb.

Note: in the original prokaryote block, the ``TS`` set was read from the *TPS* file
(copy-paste slip), so the prokaryote TS bar duplicated TPS. Passing each set explicitly on the
command line avoids that class of mistake; consider re-generating that figure.
"""

from __future__ import annotations

import argparse

import pandas as pd

from .io import log, read_list


def membership_table(universe: list[str], sets: dict[str, set[str]]) -> pd.DataFrame:
    """Boolean table: rows = organisms, columns = set names (vectorised ``isin``)."""
    idx = pd.Index(universe, name="organism")
    return pd.DataFrame({name: idx.isin(members) for name, members in sets.items()}, index=idx)


def intersections(table: pd.DataFrame, min_size: int = 0) -> pd.Series:
    """Counts of each exact combination of sets, largest first (the UpSet bars)."""
    counts = table.value_counts()
    counts = counts[counts >= max(min_size, 1)]
    return counts[[any(k) for k in counts.index]]  # drop "in no set"


def upset(table: pd.DataFrame, out: str, title: str | None = None, min_size: int = 0) -> None:
    """Minimal UpSet plot in plain matplotlib (no extra dependency; pandas-3 safe)."""
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    counts = intersections(table, min_size)
    sets = list(table.columns)
    set_sizes = table.sum()
    n, m = len(counts), len(sets)
    fig = plt.figure(figsize=(max(5, 0.45 * n + 3), 2.6 + 0.35 * m))
    grid = fig.add_gridspec(
        2,
        2,
        width_ratios=[1, max(n, 1) * 0.45 / 2.2 + 1],
        height_ratios=[2.2, 0.35 * m + 0.2],
        wspace=0.25,
        hspace=0.05,
    )
    ax_bar = fig.add_subplot(grid[0, 1])
    ax_dot = fig.add_subplot(grid[1, 1], sharex=ax_bar)
    ax_set = fig.add_subplot(grid[1, 0])
    x = range(n)
    ax_bar.bar(x, counts.values, color="#333333", width=0.6)
    for i, v in enumerate(counts.values):
        ax_bar.text(i, v, str(v), ha="center", va="bottom", fontsize=8)
    ax_bar.set_ylabel("Organisms")
    ax_bar.tick_params(axis="x", bottom=False, labelbottom=False)
    ax_bar.spines[["top", "right"]].set_visible(False)
    for i, combo in enumerate(counts.index):
        on = [j for j, flag in enumerate(combo) if flag]
        ax_dot.scatter([i] * m, range(m), color="#dddddd", s=60, zorder=1)
        ax_dot.scatter([i] * len(on), on, color="#333333", s=60, zorder=2)
        if len(on) > 1:
            ax_dot.plot([i, i], [min(on), max(on)], color="#333333", lw=2, zorder=1)
    ax_dot.set_xlim(-0.6, n - 0.4)
    ax_dot.set_ylim(-0.6, m - 0.4)
    ax_dot.set_yticks(range(m), sets)
    ax_dot.tick_params(axis="both", length=0, bottom=False, labelbottom=False)
    for side in ("top", "right", "bottom", "left"):
        ax_dot.spines[side].set_visible(False)
    ax_set.barh(range(m), set_sizes.values, color="#666666", height=0.5)
    ax_set.invert_xaxis()
    ax_set.set_ylim(-0.6, m - 0.4)
    ax_set.set_yticks([])
    ax_set.set_xlabel("Set size")
    ax_set.spines[["top", "left", "right"]].set_visible(False)
    if title:
        fig.suptitle(title)
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)


def register(sub: argparse._SubParsersAction) -> None:
    c = sub.add_parser("upset", help="UpSet plot of set memberships (e.g. pathways per organism)")
    c.add_argument(
        "--set",
        dest="sets",
        action="append",
        required=True,
        help="NAME=FILE (file lists organisms in the set); repeatable",
    )
    c.add_argument("--universe", required=True, help="all organisms (rows)")
    c.add_argument("--title")
    c.add_argument("--min-size", type=int, default=0)
    c.add_argument("--table", help="also save the membership table (TSV)")
    c.add_argument("-o", "--out", required=True, help="figure (.pdf/.png/.svg)")
    c.set_defaults(func=_cli)


def _cli(a):
    sets = {}
    for spec in a.sets:
        name, _, path = spec.partition("=")
        if not path:
            raise SystemExit(f"bad --set {spec!r}; expected NAME=FILE")
        sets[name] = set(read_list(path))
    table = membership_table(read_list(a.universe, unique=True), sets)
    if a.table:
        table.astype(int).to_csv(a.table, sep="\t")
    upset(table, a.out, a.title, a.min_size)
    log(f"{len(table)} organisms, {len(sets)} sets -> {a.out}")
