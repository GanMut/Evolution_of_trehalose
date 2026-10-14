import random

import pandas as pd

from evotools.orthology import genome_key, ortholog_groups, reciprocal_best_hits


def naive_rbh(df, score="pident"):
    """Straightforward reference implementation used to validate the vectorised one."""
    g = lambda s: "_".join(s.split("_")[:2])  # noqa: E731
    best = {}
    for q, s, v in df[["qseqid", "sseqid", score]].itertuples(index=False):
        if g(q) == g(s):
            continue
        key = (q, g(s))
        cur = best.get(key)
        if cur is None or v > cur[1] or (v == cur[1] and s < cur[0]):
            best[key] = (s, v)
    pairs = set()
    for (q, _), (s, _) in best.items():
        back = best.get((s, g(q)))
        if back and back[0] == q:
            pairs.add(tuple(sorted((q, s))))
    return pairs


def test_genome_key():
    s = pd.Series(["GCF_000005845_WP_1", "GCA_1_x"])
    assert list(genome_key(s)) == ["GCF_000005845", "GCA_1"]
    assert list(genome_key(pd.Series(["Eco:b1"]), ":", 1)) == ["Eco"]


def test_rbh_toy():
    hits = pd.DataFrame(
        [
            ("G_1_a", "G_2_a", 95.0),
            ("G_1_a", "G_2_b", 60.0),
            ("G_2_a", "G_1_a", 94.0),
            ("G_2_b", "G_1_a", 99.0),  # b's best is a, but a's best is G_2_a -> not RBH
            ("G_1_a", "G_1_z", 100.0),  # same genome, ignored
            ("G_3_c", "G_1_a", 50.0),  # one-directional only
        ],
        columns=["qseqid", "sseqid", "pident"],
    )
    rbh = reciprocal_best_hits(hits)
    assert list(zip(rbh.protein_a, rbh.protein_b)) == [("G_1_a", "G_2_a")]
    assert rbh.loc[0, "score_ab"] == 95.0 and rbh.loc[0, "score_ba"] == 94.0


def test_rbh_matches_naive_on_random_data():
    rng = random.Random(0)
    prots = [f"G_{g}_p{i}" for g in range(6) for i in range(15)]
    rows = [
        (q, rng.choice(prots), round(rng.uniform(20, 100), 1)) for q in prots for _ in range(12)
    ]
    df = pd.DataFrame(rows, columns=["qseqid", "sseqid", "pident"])
    got = set(zip(*reciprocal_best_hits(df)[["protein_a", "protein_b"]].T.values))
    assert got == naive_rbh(df)
    assert len(got) > 0


def test_explicit_genome_columns_and_groups():
    hits = pd.DataFrame(
        {
            "qseqid": ["a", "b", "b", "c"],
            "sseqid": ["b", "a", "c", "b"],
            "pident": [90, 90, 80, 80],
            "qOrg": ["X", "Y", "Y", "Z"],
            "sOrg": ["Y", "X", "Z", "Y"],
        }
    )
    rbh = reciprocal_best_hits(hits, query_genome="qOrg", subject_genome="sOrg")
    assert len(rbh) == 2
    groups = ortholog_groups(rbh)
    assert groups.group.nunique() == 1 and set(groups.protein) == {"a", "b", "c"}
