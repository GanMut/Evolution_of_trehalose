"""Logistic regression: which traits (phylum, habitat, ...) predict a pathway's presence.

Replaces: logistic_regression_using_sklearn.py (hard-coded file and column names).
"""

from __future__ import annotations

import argparse

import pandas as pd

from .io import log


def fit_logistic(
    df: pd.DataFrame,
    target: str,
    categorical: list[str],
    drop: list[str],
    test_size: float = 0.2,
    seed: int = 42,
    balanced: bool = False,
):
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report
    from sklearn.model_selection import train_test_split

    data = pd.get_dummies(df.drop(columns=[c for c in drop if c in df]), columns=categorical)
    y = data.pop(target)
    x = data.astype(float)
    strat = y if y.value_counts().min() >= 2 else None
    x_tr, x_te, y_tr, y_te = train_test_split(
        x, y, test_size=test_size, random_state=seed, stratify=strat
    )
    model = LogisticRegression(max_iter=5000, class_weight="balanced" if balanced else None)
    model.fit(x_tr, y_tr)
    report = classification_report(y_te, model.predict(x_te), zero_division=0)
    coefs = (
        pd.DataFrame({"feature": x.columns, "coefficient": model.coef_[0]})
        .sort_values("coefficient", ascending=False)
        .reset_index(drop=True)
    )
    return model, report, coefs


def register(sub: argparse._SubParsersAction) -> None:
    c = sub.add_parser("logreg", help="logistic regression of a binary trait")
    c.add_argument("table", help="TSV with one row per organism")
    c.add_argument("--target", required=True, help="0/1 column to predict")
    c.add_argument("--categorical", nargs="*", default=[], help="columns to one-hot encode")
    c.add_argument("--drop", nargs="*", default=[], help="ID or other columns to ignore")
    c.add_argument("--test-size", type=float, default=0.2)
    c.add_argument("--seed", type=int, default=42)
    c.add_argument("--balanced", action="store_true", help="class_weight='balanced'")
    c.add_argument("-o", "--out", help="write coefficients TSV here")
    c.set_defaults(func=_cli)


def _cli(a):
    df = pd.read_csv(a.table, sep="\t")
    _, report, coefs = fit_logistic(
        df, a.target, a.categorical, a.drop, a.test_size, a.seed, a.balanced
    )
    print(report)
    print(coefs.to_string(index=False))
    if a.out:
        coefs.to_csv(a.out, sep="\t", index=False)
    log("note: sklearn gives no p-values; use statsmodels.Logit if you need them")
