"""End-to-end pipelines wrapping BLAST+, MUSCLE/MAFFT, HMMER, BMGE/trimAl and IQ-TREE.

``homologs``     query protein -> BLAST seeds (best per organism) -> MSA -> HMM profile ->
                 hmmsearch -> best homolog per organism -> FASTA with organism names
``species-tree`` per-gene alignments -> (trim) -> concatenate + partitions -> IQ-TREE

Replaces: auto.py and ids_to_hmmer.py.

Improvements over auto.py
* No hard-coded ``/softwares/...`` paths: tools are found on ``$PATH`` or passed as options.
* ``subprocess`` with argument lists (no shell-quoting bugs with organism names containing
  quotes/brackets) and a failing step stops the pipeline instead of silently continuing.
* The per-organism ``grep`` loops over BLAST/HMMER output are replaced by one pass in Python.
* Headers are renamed from the HMMER descriptions directly, so the separate
  ``all_headers.txt`` grep (get_org_headers.sh + change_headers.py) is no longer needed.
* Concatenation is done in Python with a partition file (no catfasta2phyml.pl), genes are
  trimmed *before* concatenation so partitions stay valid, and IQ-TREE is run on the trimmed
  alignment (the old script ran it on ``full_msa.faa`` and referenced an undefined variable).
* Every step is skipped if its output already exists (``--force`` to redo) -> safe to resume.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
from pathlib import Path

from .headers import DEFAULT_EXCLUDE, best_per_organism_blast, best_per_organism_hmmer
from .io import Record, log, read_fasta, write_fasta, write_list


class Runner:
    def __init__(self, dry_run: bool = False, force: bool = False):
        self.dry_run, self.force = dry_run, force

    def needed(self, out: Path) -> bool:
        if out.exists() and out.stat().st_size > 0 and not self.force:
            log(f"  [skip] {out} exists")
            return False
        return True

    def run(self, cmd: list[str], stdout: Path | None = None) -> None:
        log("  $ " + " ".join(shlex.quote(c) for c in cmd) + (f" > {stdout}" if stdout else ""))
        if self.dry_run:
            return
        if stdout:
            with open(stdout, "w") as fh:
                subprocess.run(cmd, check=True, stdout=fh)
        else:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)


def which(tool: str) -> str:
    path = shutil.which(tool)
    return path or tool  # let the subprocess fail with a clear "not found" if missing


def aligner_template(muscle: str) -> str:
    """Pick MUSCLE v3 or v5 syntax automatically."""
    try:
        ver = subprocess.run([muscle, "-version"], capture_output=True, text=True, timeout=30)
        text = (ver.stdout + ver.stderr).lower()
    except (OSError, subprocess.TimeoutExpired):
        text = ""
    if "v3" in text:
        return f"{muscle} -in {{inp}} -out {{out}}"
    return f"{muscle} -align {{inp}} -output {{out}}"


def fetch(db: str, ids: list[str], out: Path, samtools: str | None, runner: Runner) -> None:
    """Fetch sequences by ID: samtools faidx if available (indexed), else one streaming pass."""
    if runner.dry_run:
        log(f"  [fetch] {len(ids)} ids from {db} -> {out}")
        return
    if samtools and shutil.which(samtools):
        with open(out, "w") as fh:
            for i in range(0, len(ids), 500):
                subprocess.run([samtools, "faidx", db, *ids[i : i + 500]], check=True, stdout=fh)
    else:
        wanted = set(ids)
        write_fasta((r for r in read_fasta(db) if r.id in wanted), out)


def homologs(query: Path, db: str, outdir: Path, a: argparse.Namespace, runner: Runner) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    log(f"== {query.name} -> {outdir}")
    blast, seeds_ids, seeds = outdir / "blast.tsv", outdir / "seeds.ids", outdir / "seeds.faa"
    seeds_aln, hmm, dom = (
        outdir / "seeds.aln.faa",
        outdir / "profile.hmm",
        outdir / "hmmsearch.domtbl",
    )
    hits_tsv, hits_faa = outdir / "homologs.tsv", outdir / "homologs.faa"
    final = outdir / "homologs.organism.faa"

    if runner.needed(blast):
        runner.run(
            [
                which(a.blastp),
                "-query",
                str(query),
                "-db",
                db,
                "-max_target_seqs",
                str(a.max_target_seqs),
                "-evalue",
                str(a.blast_evalue),
                "-outfmt",
                "6 sseqid pident evalue bitscore salltitles",
                "-num_threads",
                str(a.threads),
                "-out",
                str(blast),
            ]
        )
    if runner.needed(seeds):
        if not runner.dry_run:
            best = best_per_organism_blast(blast, a.exclude)
            write_list(best["id"], seeds_ids)
            log(f"  {len(best)} organisms among BLAST hits")
            fetch(db, list(best["id"]), seeds, a.samtools, runner)
    if runner.needed(seeds_aln):
        tmpl = a.aligner_cmd or aligner_template(which(a.muscle))
        runner.run(shlex.split(tmpl.format(inp=seeds, out=seeds_aln)))
    if runner.needed(hmm):
        runner.run([which(a.hmmbuild), str(hmm), str(seeds_aln)])
    if runner.needed(dom):
        runner.run(
            [
                which(a.hmmsearch),
                "--cpu",
                str(a.threads),
                "--domtblout",
                str(dom),
                "-o",
                "/dev/null",
                str(hmm),
                db,
            ]
        )
    if runner.dry_run:
        return final
    if runner.needed(final):
        best = best_per_organism_hmmer(dom, a.hmm_evalue)
        best.to_csv(hits_tsv, sep="\t", index=False)
        write_list(best["organism"], outdir / "organisms.txt")
        log(f"  {len(best)} organisms with a homolog (full-seq E < {a.hmm_evalue:g})")
        fetch(db, list(best["target"]), hits_faa, a.samtools, runner)
        org = dict(zip(best["target"], best["organism"]))
        write_fasta((Record(org.get(r.id, r.id), r.seq) for r in read_fasta(hits_faa)), final)
    if a.align_final:
        aln = outdir / "homologs.organism.aln.faa"
        if runner.needed(aln):
            tmpl = a.aligner_cmd or aligner_template(which(a.muscle))
            runner.run(shlex.split(tmpl.format(inp=final, out=aln)))
    return final


def concatenate(alignments: list[Path], out: Path, partitions: Path) -> tuple[int, int]:
    """Supermatrix: union of taxa, missing genes filled with gaps; writes a NEXUS partition file."""
    genes: list[tuple[str, dict[str, str], int]] = []
    taxa: dict[str, None] = {}
    for path in alignments:
        seqs: dict[str, str] = {}
        for r in read_fasta(path):
            if r.header in seqs:
                log(f"  warning: duplicate taxon {r.header!r} in {path.name}; keeping first")
                continue
            seqs[r.header] = r.seq
        if not seqs:
            continue
        lengths = {len(s) for s in seqs.values()}
        if len(lengths) != 1:
            raise SystemExit(f"{path} is not aligned (sequence lengths differ)")
        genes.append((path.stem, seqs, lengths.pop()))
        taxa.update(dict.fromkeys(seqs))
    write_fasta(
        (Record(t, "".join(s.get(t, "-" * n) for _, s, n in genes)) for t in taxa), out, width=0
    )
    lines, pos = ["#nexus", "begin sets;"], 1
    for name, _, n in genes:
        lines.append(f"  charset {name} = {pos}-{pos + n - 1};")
        pos += n
    lines.append("end;")
    partitions.write_text("\n".join(lines) + "\n")
    return len(taxa), pos - 1


def species_tree(a: argparse.Namespace, runner: Runner) -> None:
    outdir = Path(a.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    alns = [Path(p) for p in a.alignments]
    if a.trimmer != "none":
        trimmed_dir = outdir / "trimmed"
        trimmed_dir.mkdir(exist_ok=True)
        trimmed = []
        for aln in alns:
            out = trimmed_dir / f"{aln.stem}.faa"
            if runner.needed(out):
                if a.trimmer == "bmge":
                    if not a.bmge_jar:
                        raise SystemExit("--bmge-jar is required with --trimmer bmge")
                    runner.run(
                        ["java", "-jar", a.bmge_jar, "-i", str(aln), "-t", "AA", "-of", str(out)]
                    )
                else:
                    runner.run([which("trimal"), "-in", str(aln), "-out", str(out), "-automated1"])
            trimmed.append(out)
        alns = trimmed
    supermatrix, parts = outdir / "concatenated.faa", outdir / "partitions.nex"
    if not runner.dry_run:
        n_taxa, n_sites = concatenate(alns, supermatrix, parts)
        log(f"supermatrix: {n_taxa} taxa x {n_sites} sites, {len(alns)} genes")
    cmd = [which(a.iqtree), "-s", str(supermatrix), "--prefix", str(outdir / "species_tree")]
    if a.partitioned:
        cmd += ["-p", str(parts)]
    cmd += shlex.split(a.iqtree_args)
    runner.run(cmd)


# ----------------------------------------------------------------------------- CLI
def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("pipeline", help="BLAST/HMMER homolog search and species-tree pipelines")
    s = p.add_subparsers(dest="cmd", required=True)

    c = s.add_parser("homologs", help="query protein(s) -> one homolog per organism")
    c.add_argument(
        "queries",
        nargs="+",
        help="query FASTA files, or directories each holding one *.fasta/*.faa",
    )
    c.add_argument("--db", required=True, help="protein FASTA (BLAST db with same prefix)")
    c.add_argument("-d", "--outdir", default="homologs")
    c.add_argument("-t", "--threads", type=int, default=4)
    c.add_argument("--blast-evalue", type=float, default=1e-3)
    c.add_argument("--max-target-seqs", type=int, default=500)
    c.add_argument("--hmm-evalue", type=float, default=1e-4)
    c.add_argument(
        "--exclude",
        nargs="*",
        default=list(DEFAULT_EXCLUDE),
        help="skip BLAST seeds whose title contains these words",
    )
    c.add_argument("--align-final", action="store_true", help="also align the final homologs")
    _tool_opts(c)
    c.set_defaults(func=_cli_homologs)

    c = s.add_parser("species-tree", help="concatenate gene alignments and run IQ-TREE")
    c.add_argument("alignments", nargs="+", help="per-gene alignments with organism names")
    c.add_argument("-d", "--outdir", default="species_tree")
    c.add_argument("--trimmer", choices=["bmge", "trimal", "none"], default="none")
    c.add_argument("--bmge-jar")
    c.add_argument("--iqtree", default="iqtree2")
    c.add_argument(
        "--iqtree-args",
        default="-m TEST -B 1000 -T AUTO",
        help="extra IQ-TREE arguments (default mirrors the original run)",
    )
    c.add_argument("--partitioned", action="store_true", help="use per-gene partitions (-p)")
    c.add_argument("--dry-run", action="store_true")
    c.add_argument("--force", action="store_true")
    c.set_defaults(func=lambda a: species_tree(a, Runner(a.dry_run, a.force)))


def _tool_opts(c: argparse.ArgumentParser) -> None:
    g = c.add_argument_group("tools (names on $PATH or full paths)")
    g.add_argument("--blastp", default="blastp")
    g.add_argument("--muscle", default="muscle")
    g.add_argument(
        "--aligner-cmd",
        help="aligner command with {inp}/{out} placeholders, e.g. "
        "'muscle -in {inp} -out {out} -maxiters 2' (default: auto-detect MUSCLE "
        "v3/v5). Output redirection (>) is not supported.",
    )
    g.add_argument("--hmmbuild", default="hmmbuild")
    g.add_argument("--hmmsearch", default="hmmsearch")
    g.add_argument("--samtools", default="samtools", help="used if found; else Python fetch")
    c.add_argument("--dry-run", action="store_true", help="print commands only")
    c.add_argument("--force", action="store_true", help="redo steps even if outputs exist")


def _queries(items: list[str]) -> list[tuple[Path, str]]:
    out = []
    for item in items:
        p = Path(item)
        if p.is_dir():
            hits = sorted([*p.glob("*.fasta"), *p.glob("*.faa"), *p.glob("*.fa")])
            if not hits:
                log(f"warning: no FASTA in {p}, skipped")
                continue
            out.append((hits[0], p.name))
        else:
            out.append((p, p.stem))
    return out


def _cli_homologs(a):
    runner = Runner(a.dry_run, a.force)
    finals = [
        homologs(q, a.db, Path(a.outdir) / name, a, runner) for q, name in _queries(a.queries)
    ]
    if not a.dry_run:
        write_list(map(str, finals), Path(a.outdir) / "final_files.txt")
        log(f"done: {len(finals)} queries; list in {Path(a.outdir) / 'final_files.txt'}")
