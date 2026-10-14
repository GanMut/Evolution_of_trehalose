import gzip

from conftest import domtbl_line

from evotools import fasta, headers
from evotools.io import Record, read_fasta, write_fasta


def test_read_write_roundtrip_and_gzip(fasta_file, tmp_path):
    recs = list(read_fasta(fasta_file))
    assert [r.id for r in recs] == ["XP_1.1", "XP_2.1", "WP_3.1", "WP_4.1"]
    assert recs[0].seq == "MKTAYIAKQRQISFVK"  # multi-line sequence joined
    gz = tmp_path / "x.faa.gz"
    write_fasta(recs, gz, width=5)
    with gzip.open(gz, "rt") as fh:
        assert fh.readline().startswith(">XP_1.1")
    assert list(read_fasta(gz)) == recs


def test_head_subset_exclude(fasta_file):
    recs = list(read_fasta(fasta_file))
    assert len(list(fasta.head(recs, 2))) == 2
    keep = list(fasta.subset(recs, ["WP_3.1", "XP_1.1"]))
    assert [r.id for r in keep] == ["XP_1.1", "WP_3.1"]  # file order preserved
    drop = list(fasta.subset(recs, ["WP_3.1"], exclude=True))
    assert "WP_3.1" not in {r.id for r in drop} and len(drop) == 3
    by_header = list(fasta.subset(recs, [recs[1].header], by="header"))
    assert by_header == [recs[1]]


def test_sample_is_reproducible_and_ordered():
    recs = [Record(f"s{i}", "A") for i in range(1000)]
    a = fasta.sample(recs, 50, seed=1)
    assert a == fasta.sample(recs, 50, seed=1)
    assert len(a) == 50 and len({r.id for r in a}) == 50
    ids = [int(r.id[1:]) for r in a]
    assert ids == sorted(ids)
    assert len(fasta.sample(recs[:10], 50)) == 10


def test_transcribe_slice_tag_clean():
    rec = Record("x desc", "ACGTtt")
    assert next(fasta.transcribe([rec])).seq == "ACGUuu"
    assert fasta.slice_region(Record("p", "ABCDEFG"), 2, 4).seq == "BCD"  # 1-based inclusive
    tagged = next(fasta.tag_organism([Record("id1 x [old]", "M")], "E. coli"))
    assert tagged.header == "id1 x (old) [E. coli]"
    assert next(fasta.clean_headers([Record("a b c", "M")])).header == "a_b_c"
    assert fasta.sanitize_label("Mus (sub) = x", strict=False) == "Mus _sub_ _ x"
    assert fasta.sanitize_label("Mus (sub) = x") == "Mus__sub____x"


def test_organism_parsing(fasta_file):
    assert headers.organism_of(">a b [Homo sapiens]") == "Homo sapiens"
    assert headers.organism_of("no organism") is None
    assert headers.organism_of("x [A b]<>y [C d]") == "A b"  # first of merged BLAST titles
    with open(fasta_file) as fh:
        orgs = headers.organisms_in(fh)
    assert "Escherichia coli" in orgs and "Homo sapiens" in orgs
    with open(fasta_file) as fh:
        m = headers.id_to_organism((ln for ln in fh if "[" in ln), {"XP_1.1", "WP_3.1"})
    assert m == {"XP_1.1": "Homo sapiens", "WP_3.1": "Escherichia coli"}


def test_rename_to_organism():
    recs = [Record("XP_1.1", "AA"), Record("UNK", "CC")]
    out = list(headers.rename_to_organism(recs, {"XP_1.1": "Homo sapiens"}, safe=True))
    assert [r.header for r in out] == ["Homo_sapiens", "UNK"]
    out = list(headers.rename_to_organism(recs, {"XP_1.1": "H s"}, keep_unmapped=False))
    assert [r.header for r in out] == ["H s"]


def test_best_per_organism_blast(tmp_path):
    p = tmp_path / "blast.tsv"
    p.write_text(
        "XP_1.1\t90\t1e-50\t300\tXP_1.1 TPS [Homo sapiens]\n"
        "XP_9.1\t80\t1e-40\t250\tXP_9.1 TPS isoform [Homo sapiens]\n"
        "XP_2.1\t70\t1e-30\t200\tXP_2.1 hypothetical protein [Mus musculus]\n"
        "XP_3.1\t60\t1e-20\t150\tXP_3.1 TPS [Mus musculus]\n"
        "WP_5.1\t50\t1e-10\t100\tWP_5.1 MULTISPECIES: TPS [Bacteria]\n"
    )
    df = headers.best_per_organism_blast(p)
    assert dict(zip(df.organism, df.id)) == {"Homo sapiens": "XP_1.1", "Mus musculus": "XP_3.1"}


def test_best_per_organism_hmmer(tmp_path):
    p = tmp_path / "dom.tbl"
    p.write_text(
        "# comment\n"
        + domtbl_line("A1", 1e-80, 300, "TPS [Org A]")
        + domtbl_line("A1", 1e-80, 300, "TPS [Org A]")  # second domain, same target
        + domtbl_line("A2", 1e-60, 200, "TPS-like [Org A]")
        + domtbl_line("B1", 1e-10, 50, "TPS [Org B]")
        + domtbl_line("C1", 1e-2, 10, "weak [Org C]")
    )
    df = headers.best_per_organism_hmmer(p, max_evalue=1e-4)
    assert list(df.target) == ["A1", "B1"]
