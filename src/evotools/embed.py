"""Embed aligned sequences in 2-3D (PCA or UMAP) from a one-hot encoded MSA.

Replaces: umap_test.py.

Fixes and speed-ups vs. the original
* One-hot encoding is a single NumPy comparison per residue type instead of a Python loop that
  assigned every column with ``df.loc[...]`` (minutes -> well under a second for typical MSAs).
* ``main()`` was called with ``PCA=`` instead of ``method=`` (crashed on every run).
* PCA ignored ``--ncom`` (hard-coded 3); the default method ``pac`` produced no output.
* Gap characters are dropped explicitly, and constant columns (no information) are removed.
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from .io import log, read_fasta

GAPS = b"-.?"


def one_hot(path: str) -> pd.DataFrame:
    recs = list(read_fasta(path))
    if not recs:
        raise SystemExit("empty alignment")
    length = len(recs[0].seq)
    bad = [r.id for r in recs if len(r.seq) != length]
    if bad:
        raise SystemExit(f"not an alignment: {len(bad)} sequences differ in length, e.g. {bad[0]}")
    arr = np.frombuffer(
        "".join(r.seq.upper() for r in recs).encode("ascii"), dtype=np.uint8
    ).reshape(len(recs), length)
    blocks, keys = [], []
    for code in np.unique(arr):
        if bytes([code]) in GAPS:
            continue
        mask = arr == code
        idx = np.flatnonzero(mask.any(axis=0) & ~mask.all(axis=0))  # informative columns
        blocks.append(mask[:, idx])
        keys.extend((int(i) + 1, chr(code)) for i in idx)
    if not keys:
        raise SystemExit("alignment has no variable positions")
    data = np.concatenate(blocks, axis=1)
    order = sorted(range(len(keys)), key=keys.__getitem__)  # by position, then residue
    return pd.DataFrame(
        data[:, order],
        index=[r.id for r in recs],
        columns=[f"pos_{keys[i][0]}_{keys[i][1]}" for i in order],
    )


def embed(
    features: pd.DataFrame, method: str = "pca", n_components: int = 3, seed: int = 42
) -> pd.DataFrame:
    x = features.to_numpy(dtype=np.float32)
    if method == "pca":
        from sklearn.decomposition import PCA

        model = PCA(n_components=n_components, random_state=seed)
    elif method == "umap":
        try:
            from umap import UMAP
        except ImportError as e:  # pragma: no cover
            raise SystemExit("UMAP needs: pip install 'evotools[umap]'") from e
        model = UMAP(n_components=n_components, random_state=seed)
    else:
        raise ValueError(method)
    coords = model.fit_transform(x)
    out = pd.DataFrame(
        coords, index=features.index, columns=[f"dim_{i}" for i in range(1, n_components + 1)]
    )
    if method == "pca":
        log("explained variance: " + ", ".join(f"{v:.3f}" for v in model.explained_variance_ratio_))
    return out


def register(sub: argparse._SubParsersAction) -> None:
    c = sub.add_parser("embed", help="PCA/UMAP of an MSA (one-hot encoded)")
    c.add_argument("msa", help="aligned FASTA")
    c.add_argument("-m", "--method", choices=["pca", "umap"], default="pca")
    c.add_argument("-n", "--n-components", type=int, default=3)
    c.add_argument("--seed", type=int, default=42)
    c.add_argument("-o", "--out", required=True, help=".parquet or .tsv")
    c.set_defaults(func=_cli)


def _cli(a):
    feats = one_hot(a.msa)
    log(f"one-hot matrix: {feats.shape[0]} sequences x {feats.shape[1]} features")
    res = embed(feats, a.method, a.n_components, a.seed)
    if a.out.endswith(".parquet"):
        res.to_parquet(a.out)
    else:
        res.to_csv(a.out, sep="\t")
