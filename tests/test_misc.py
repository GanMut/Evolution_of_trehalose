import pandas as pd
import pytest

from evotools import cli, embed, gff, itol, matrix, trees
from evotools.io import read_fasta
from evotools.pipeline import concatenate


def test_presence_absence_and_compare():
    df = matrix.presence_absence(["A", "B", "C"], {"tps": {"A", "C"}, "treT": {"B"}})
    assert df.loc["A"].tolist() == [1, 0] and df.loc["B"].tolist() == [0, 1]
    res = matrix.compare(["a", "b", "c", "a"], ["c", "d"])
    assert res == {"common": ["c"], "only_a": ["a", "b"], "only_b": ["d"]}


def test_matrix_cli_names_columns_by_dir(tmp_path):
    for gene, orgs in {"TPS": "Homo\nMus\n", "TreT": "Mus\n"}.items():
        (tmp_path / "genes" / gene).mkdir(parents=True)
        (tmp_path / "genes" / gene / "uniq_organism_final.txt").write_text(orgs)
    (tmp_path / "orgs.txt").write_text("Homo\nMus\nYeast\n")
    out = tmp_path / "m.tsv"
    cli.main(
        [
            "matrix",
            "--organisms",
            str(tmp_path / "orgs.txt"),
            "--search-dirs",
            str(tmp_path / "genes"),
            "-o",
            str(out),
        ]
    )
    df = pd.read_csv(out, sep="\t", index_col=0)
    assert list(df.columns) == ["TPS", "TreT"]
    assert df.loc["Yeast"].sum() == 0 and df.loc["Mus"].sum() == 2


def test_itol_colorstrip_and_hgt(tmp_path):
    (tmp_path / "plants.txt").write_text("Arabidopsis thaliana (Col-0)\n")
    groups = itol.parse_groups([f"Plants:#00ff00={tmp_path / 'plants.txt'}"])
    text = itol.colorstrip(groups)
    assert text.startswith("DATASET_COLORSTRIP")
    assert "Arabidopsis thaliana _Col-0_\t#00ff00\tPlants" in text
    r = tmp_path / "ranger.txt"
    r.write_text(
        "[Most Frequent mapping --> n12, 87 times], [Most Frequent recipient --> n40, 87 times].\n"
        "[Most Frequent mapping --> n5, 10 times], [Most Frequent recipient --> n5, 10 times].\n"
        "speciation line without transfer\n"
    )
    df = itol.parse_rangerdtl(r)
    assert df.values.tolist() == [["n12", "n40", 87]]
    assert "n12\tn40\t87\t#f90d0d\tnormal" in itol.connections(df, "TPS")


def test_trees():
    assert (
        trees.sci_to_decimal("(a:2.9227e-06,b:5e-7,c:0.1);")
        == "(a:0.0000029227,b:0.0000005,c:0.1);"
    )
    assert trees.optroot_tree("log\n(a,b);\nmore\n((a,b),c);\n") == "((a,b),c);\n"
    with pytest.raises(ValueError):
        trees.optroot_tree("nothing")


def test_gff(tmp_path):
    g = tmp_path / "GCF_1_genomic.gff"
    g.write_text(
        "##gff-version 3\n"
        "chr1\tRefSeq\trRNA\t101\t1900\t.\t+\t.\tID=rna-1;product=18S ribosomal RNA\n"
        "chr1\tRefSeq\trRNA\t3001\t3100\t.\t-\t.\tID=rna-2;product=5S ribosomal RNA\n"
        "chr1\tRefSeq\tCDS\t1\t90\t.\t+\t0\tID=cds-1;protein_id=XP_1.1\n"
        "chr1\tRefSeq\tCDS\t1\t90\t.\t+\t0\tID=cds-2;pseudo=true;protein_id=XP_2.1\n"
    )
    assert gff.rrna_bed(g) == ["chr1\t100\t1900\tGCF_1|rna-1\t0\t+"]
    assert gff.cds_protein_ids(g) == ["XP_1.1"]


def test_one_hot_and_pca(tmp_path):
    msa = tmp_path / "msa.faa"
    msa.write_text(">s1\nAC-D\n>s2\nAC-E\n>s3\nGC-D\n>s4\nGCAE\n")
    feats = embed.one_hot(str(msa))
    # position 2 (C everywhere) is constant -> dropped; gaps never become features
    assert list(feats.columns) == ["pos_1_A", "pos_1_G", "pos_3_A", "pos_4_D", "pos_4_E"]
    coords = embed.embed(feats, "pca", 2)
    assert coords.shape == (4, 2)


def test_concatenate(tmp_path):
    (tmp_path / "g1.faa").write_text(">Homo\nAAA\n>Mus\nAAC\n")
    (tmp_path / "g2.faa").write_text(">Mus\nGG\n>Yeast\nGT\n")
    out, parts = tmp_path / "cat.faa", tmp_path / "p.nex"
    n_taxa, n_sites = concatenate([tmp_path / "g1.faa", tmp_path / "g2.faa"], out, parts)
    assert (n_taxa, n_sites) == (3, 5)
    seqs = {r.header: r.seq for r in read_fasta(out)}
    assert seqs == {"Homo": "AAA--", "Mus": "AACGG", "Yeast": "---GT"}
    assert "charset g2 = 4-5;" in parts.read_text()


def test_cli_smoke(fasta_file, tmp_path, capsys):
    cli.main(["fasta", "count", str(fasta_file)])
    assert capsys.readouterr().out.strip().endswith("\t4")
    out = tmp_path / "sub.faa"
    ids = tmp_path / "ids.txt"
    ids.write_text("WP_3.1\n")
    cli.main(["fasta", "subset", str(fasta_file), "-l", str(ids), "-o", str(out)])
    assert [r.id for r in read_fasta(out)] == ["WP_3.1"]


def test_pipeline_dry_run(tmp_path, capsys):
    q = tmp_path / "q.fasta"
    q.write_text(">q\nMK\n")
    cli.main(
        [
            "pipeline",
            "homologs",
            str(q),
            "--db",
            "db.faa",
            "-d",
            str(tmp_path / "o"),
            "--dry-run",
            "--muscle",
            "/nonexistent/muscle",
        ]
    )
    err = capsys.readouterr().err
    assert "blastp" in err and "hmmsearch" in err


def _fake_tool(bin_dir, name, body):
    p = bin_dir / name
    p.write_text("#!/usr/bin/env python3\nimport sys, shutil\nargs = sys.argv[1:]\n" + body)
    p.chmod(0o755)
    return str(p)


def test_pipeline_homologs_end_to_end_with_fake_tools(tmp_path):
    """Exercise every step's file handling with stand-in executables."""
    from conftest import domtbl_line

    db = tmp_path / "db.faa"
    db.write_text(
        ">XP_1.1 TPS [Homo sapiens]\nMKT\n>XP_2.1 hypothetical protein [Mus musculus]\nMKA\n"
        ">XP_3.1 TPS [Mus musculus]\nMKS\n>WP_4.1 TPS [Escherichia coli]\nMKV\n"
    )
    blast_rows = (
        "XP_1.1\t90\t1e-50\t300\tXP_1.1 TPS [Homo sapiens]\n"
        "XP_2.1\t80\t1e-40\t200\tXP_2.1 hypothetical protein [Mus musculus]\n"
        "XP_3.1\t70\t1e-30\t100\tXP_3.1 TPS [Mus musculus]\n"
    )
    dom = (
        domtbl_line("XP_1.1", 1e-90, 300, "TPS [Homo sapiens]")
        + domtbl_line("WP_4.1", 1e-20, 90, "TPS [Escherichia coli]")
        + domtbl_line("XP_3.1", 1e-30, 100, "TPS [Mus musculus]")
    )
    b = tmp_path / "bin"
    b.mkdir()
    blastp = _fake_tool(
        b, "blastp", f"open(args[args.index('-out')+1],'w').write({blast_rows!r})\n"
    )
    muscle = _fake_tool(
        b,
        "muscle",
        "if '-version' in args: print('MUSCLE v3.8.31'); sys.exit()\n"
        "shutil.copy(args[args.index('-in')+1], args[args.index('-out')+1])\n",
    )
    hb = _fake_tool(b, "hmmbuild", "open(args[0],'w').write('HMM')\n")
    hs = _fake_tool(b, "hmmsearch", f"open(args[args.index('--domtblout')+1],'w').write({dom!r})\n")
    q = tmp_path / "TPS.fasta"
    q.write_text(">q\nMKT\n")
    out = tmp_path / "out"
    cli.main(
        [
            "pipeline",
            "homologs",
            str(q),
            "--db",
            str(db),
            "-d",
            str(out),
            "--blastp",
            blastp,
            "--muscle",
            muscle,
            "--hmmbuild",
            hb,
            "--hmmsearch",
            hs,
            "--samtools",
            "/nonexistent",
        ]
    )
    seeds = [r.id for r in read_fasta(out / "TPS" / "seeds.faa")]
    assert seeds == ["XP_1.1", "XP_3.1"]  # hypothetical excluded, one per organism
    final = {r.header: r.seq for r in read_fasta(out / "TPS" / "homologs.organism.faa")}
    assert final == {"Homo sapiens": "MKT", "Mus musculus": "MKS", "Escherichia coli": "MKV"}
    orgs = (out / "TPS" / "organisms.txt").read_text().split("\n")
    assert "Escherichia coli" in orgs


def test_upset_intersections_and_plot(tmp_path):
    from evotools import plots

    table = plots.membership_table(
        ["a", "b", "c", "d", "e"], {"TPS": {"a", "b", "c"}, "TreT": {"b", "c"}}
    )
    counts = plots.intersections(table)
    assert counts[(True, True)] == 2 and counts[(True, False)] == 1
    assert (False, False) not in counts.index  # organisms in no set are not a bar
    out = tmp_path / "u.png"
    plots.upset(table, str(out), "t")
    assert out.stat().st_size > 1000
