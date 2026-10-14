import pytest


@pytest.fixture
def fasta_file(tmp_path):
    p = tmp_path / "seqs.faa"
    p.write_text(
        ">XP_1.1 trehalose-6-phosphate synthase [Homo sapiens]\nMKTAYIAK\nQRQISFVK\n"
        ">XP_2.1 hypothetical protein [Mus musculus]\nMKTAYIAKQR\n"
        ">WP_3.1 trehalase [Escherichia coli]\nACGTACGT\n"
        ">WP_4.1 TPS [Saccharomyces cerevisiae (strain [S288C])]\nMMMM\n"
    )
    return p


def domtbl_line(target, evalue, score, desc):
    # 22 whitespace-separated fields followed by the free-text description
    fields = [
        target,
        "-",
        "500",
        "query",
        "-",
        "480",
        str(evalue),
        str(score),
        "0.0",
        "1",
        "1",
        "1e-50",
        str(evalue),
        str(score),
        "0.0",
        "1",
        "400",
        "1",
        "400",
        "1",
        "400",
        "0.99",
    ]
    return " ".join(fields) + " " + desc + "\n"
