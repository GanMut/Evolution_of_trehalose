"""Compare the original implementations with evotools on synthetic data.

    python benchmarks/bench_speedups.py

The "original_*" functions reproduce the logic of best_blast_hit.py and umap_test.py
(matrix_prepare) as they were before the refactor.
"""

from __future__ import annotations

import itertools
import random
import tempfile
import time
import warnings
from pathlib import Path

import pandas as pd

from evotools.embed import one_hot
from evotools.orthology import reciprocal_best_hits

warnings.filterwarnings("ignore")


def synthetic_hits(genomes: int, proteins: int, hits: int, seed: int = 0) -> pd.DataFrame:
    rng = random.Random(seed)
    prots = [f"G_{g}_p{i}" for g in range(genomes) for i in range(proteins)]
    rows = [
        (q, rng.choice(prots), round(rng.uniform(20, 100), 2)) for q in prots for _ in range(hits)
    ]
    return pd.DataFrame(rows, columns=["query", "hit", "id"])


def original_bbh(df: pd.DataFrame) -> set[tuple[str, str]]:
    df = df.copy()
    df["g_q"] = df["query"].str.split("_").str[0] + "_" + df["query"].str.split("_").str[1]
    df["g_h"] = df["hit"].str.split("_").str[0] + "_" + df["hit"].str.split("_").str[1]
    df = df[df.g_q != df.g_h]
    genome = list(set(list(df.g_q.unique()) + list(df.g_h.unique())))
    out = pd.DataFrame(columns=["P1", "P2", "id"])
    for g1, g2 in itertools.combinations(genome, 2):
        dfo1 = df[(df["g_q"] == g1) & (df["g_h"] == g2)]
        dfo2 = df[(df.g_q == g2) & (df.g_h == g1)]
        if len(dfo1) and len(dfo2):
            for p in list(dfo1["query"].unique()):
                dfx = dfo1[dfo1["query"] == p].sort_values(by="id", ascending=False).iloc[0]
                back = dfo2[dfo2["query"] == dfx.hit]
                if len(back) and back.sort_values(by="id", ascending=False).iloc[0]["hit"] == p:
                    out.loc[len(out)] = [dfx.query, dfx.hit, dfx["id"]]
    return {tuple(sorted(x)) for x in zip(out.P1, out.P2)}


def original_one_hot(path: str) -> pd.DataFrame:
    seqs: dict[str, str] = {}
    name = None
    for line in open(path):
        line = line.strip()
        if line.startswith(">"):
            name = line[1:]
            seqs[name] = ""
        else:
            seqs[name] += line
    df = pd.DataFrame.from_dict(
        {k: list(v) for k, v in seqs.items()}, orient="index", dtype="category"
    )
    vals = sorted({c for row in df.values for c in row})
    cols = sorted(f"pos_{i}_{v}" for i in df.columns for v in vals[1:])
    df2 = pd.DataFrame(False, index=df.index, columns=cols)
    for col in df2.columns:
        pos, v = int(col.split("_")[1]), col.split("_")[-1]
        df2.loc[df[df.loc[:, pos] == v].index, col] = True
    return df2


def timed(fn, *args):
    t = time.perf_counter()
    res = fn(*args)
    return res, time.perf_counter() - t


def main() -> None:
    print("Reciprocal best hits")
    for g in (10, 20):
        df = synthetic_hits(g, 100, 10)
        old, t_old = timed(original_bbh, df)
        new_df = df.rename(columns={"query": "qseqid", "hit": "sseqid", "id": "pident"})
        new, t_new = timed(reciprocal_best_hits, new_df)
        same = old == set(zip(new.protein_a, new.protein_b))
        print(
            f"  {g:>3} genomes, {len(df):>7,} hits: {t_old:7.2f}s -> {t_new:6.3f}s "
            f"(x{t_old / t_new:,.0f}), identical pairs: {same}"
        )

    print("MSA one-hot encoding")
    rng = random.Random(1)
    with tempfile.TemporaryDirectory() as d:
        msa = Path(d) / "msa.faa"
        msa.write_text(
            "".join(
                f">s{i}\n"
                + "".join(rng.choice("ACDEFGHIKLMNPQRSTVWY--") for _ in range(400))
                + "\n"
                for i in range(300)
            )
        )
        _, t_old = timed(original_one_hot, str(msa))
        _, t_new = timed(one_hot, str(msa))
        print(f"  300 x 400 alignment: {t_old:7.2f}s -> {t_new:6.3f}s (x{t_old / t_new:,.0f})")


if __name__ == "__main__":
    main()
