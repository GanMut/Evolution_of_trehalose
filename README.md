# Evolution of Trehalose — `evotools`

[![CI](https://github.com/GanMut/Evolution_of_trehalose/actions/workflows/ci.yml/badge.svg)](https://github.com/GanMut/Evolution_of_trehalose/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

These are the tools behind my PhD project on the **evolution of trehalose metabolism across the tree
of life**. The work covers enzyme homolog searches across hundreds of eukaryotic and prokaryotic genomes,
species trees, orthology, horizontal gene transfer (HGT), and pathway presence/absence analyses.

Everything is packaged as one command-line tool, `evotools`, which you can also import as a Python
library. It was refactored from ~40 standalone lab scripts (see [docs/migration.md](docs/migration.md)).
Along the way, the slowest steps were rewritten to be **100–3000× faster**, and several silent bugs
were fixed.

```
query enzyme ──► BLAST seeds ──► MSA ──► HMM profile ──► hmmsearch ──► 1 homolog / organism
                                                                              │
     iTOL annotations ◄── tree ◄── IQ-TREE ◄── concatenate + partitions ◄─────┘
     presence/absence matrix · UpSet plots · RBH orthologs · HGT (RANGER-DTL) · PCA/UMAP
```

## Installation

```bash
git clone https://github.com/GanMut/Evolution_of_trehalose.git
cd Evolution_of_trehalose
pip install -e .            # core (numpy, pandas)
pip install -e ".[all]"     # + parquet, scikit-learn, UMAP, plotting
```

External programs are needed only for `evotools pipeline`. Install them with conda:

```bash
conda install -c bioconda blast hmmer muscle samtools iqtree trimal
```

## Commands

| Command | What it does |
|---|---|
| `evotools fasta head / subset / sample / transcribe / clean-headers / tag-organism / slice / count` | Everyday FASTA operations (streaming, gzip-aware) |
| `evotools headers organisms / rename / best-per-organism` | Organism-aware handling of NCBI headers `>ID desc [Organism]` |
| `evotools pipeline homologs` | Query → BLAST → best seed per organism → MSA → `hmmbuild` → `hmmsearch` → best homolog per organism, renamed by organism |
| `evotools pipeline species-tree` | Trim (BMGE/trimAl) → concatenate with partitions → IQ-TREE |
| `evotools rbh` | Reciprocal best hits and ortholog groups from all-vs-all BLAST/DIAMOND |
| `evotools matrix` | Gene presence/absence matrix across organisms |
| `evotools compare` | Intersection and differences of two name lists |
| `evotools upset` | UpSet plot of pathways/enzymes per organism |
| `evotools itol colorstrip / clades / hgt` | iTOL datasets: colour strips, clade colours, HGT donor→recipient arrows |
| `evotools tree decimals / optroot` | Newick fixes: scientific → decimal branch lengths, extract an OptRoot tree |
| `evotools gff rrna-bed / cds-ids` | 18S/16S rRNA genes to BED, CDS protein IDs per genome |
| `evotools embed` | PCA/UMAP of a one-hot encoded alignment |
| `evotools logreg` | Logistic regression of a trait (e.g. pathway presence vs phylum and habitat) |

Run `evotools <command> --help` for every option.

## Examples

**Find one homolog per organism for a set of enzymes** (one directory per enzyme, each holding a query FASTA):

```bash
evotools pipeline homologs queries/TPS queries/TreT queries/TreS \
    --db proteomes/617_euk_15_bac.faa -t 16 -d homologs/ --align-final
# homologs/<enzyme>/homologs.organism.faa      sequences named by organism
# homologs/<enzyme>/homologs.tsv               chosen hit, E-value, score per organism
# homologs/<enzyme>/organisms.txt              organisms with a homolog (input for `matrix`)
```

Re-running skips steps whose output already exists, so you can resume an interrupted run. Add
`--dry-run` to print the commands without running them.

**Species tree from marker-gene alignments:**

```bash
evotools pipeline species-tree homologs/*/homologs.organism.aln.faa \
    --trimmer bmge --bmge-jar ~/tools/BMGE-1.12/BMGE.jar --partitioned -d species_tree/
```

**Pathway presence/absence and UpSet plot:**

```bash
evotools matrix --organisms euk_names.txt --search-dirs homologs/ \
    --pattern organisms.txt -o matrix.tsv          # columns named after each enzyme folder

evotools upset --universe euk_names.txt -o euk_upset.pdf \
    --set TPS=TPS/euk.txt --set TS=TS/euk.txt --set TP=TP/euk.txt \
    --set treP=treP/euk.txt --set treY/Z=treYZ/euk.txt --set treT=treT/euk.txt
```

**Orthologs by reciprocal best hits:**

```bash
evotools rbh og_blast/ -d og_bbh/ --groups -j 8            # IDs like GCF_000005845_WP_1
evotools rbh hits.parquet --query-genome-col qOrg --subject-genome-col sOrg --score bitscore
```

**iTOL annotations:**

```bash
evotools itol colorstrip --title Eukaryotes -o strips.txt \
    --group "Protists:#E861A0=protist_names.txt" --group "Plants:#556B2F=plants_names.txt" \
    --group "Fungi:#FFA500=fungi_names.txt"
evotools itol hgt ranger_out.txt --name K00697 --table transfers.tsv --connections hgt_itol.txt
```

**Use as a library:**

```python
from evotools.orthology import read_hits, reciprocal_best_hits
from evotools.io import read_fasta

rbh = reciprocal_best_hits(read_hits("all_vs_all.tsv"), score="bitscore")
lengths = {r.id: len(r.seq) for r in read_fasta("proteome.faa.gz")}
```

## Performance

Measured on synthetic data of the same shape as the project inputs (`benchmarks/` reproduces them):

| Step | Before | After | Why |
|---|---|---|---|
| Reciprocal best hits, 20 genomes / 20k hits | 9.9 s | 0.05 s (**~200×**) | Per-genome-pair DataFrame filtering and row-by-row `df.loc[len(df)]` appends replaced by one sort + `drop_duplicates` + self-merge. Same pairs returned. 200 genomes / 2M hits now takes ~9 s. |
| MSA one-hot encoding, 300 seqs × 400 columns | 38.9 s | 0.012 s (**~3000×**) | NumPy vectorised comparison instead of assigning one column at a time with `.loc` |
| Organism/ID lookups in BLAST/HMMER output | one `grep` over the whole file **per organism** | one pass | dict/set lookups, O(file) instead of O(organisms × file) |
| Random subsample of sequences | two passes + `id in list` (O(n·k)) | one pass (reservoir sampling) | |
| Subsetting by ID list | `in list` per record | `in set` | |

## Fixed along the way

- `ortholog_finder.py`: appends to lists stored inside a `multiprocessing.Manager().dict()` were silently lost, so some ortholog pairs could be missing.
- `umap_test.py` always crashed (`main(..., PCA=...)`). PCA also ignored `--ncom`, and the default method produced no output.
- `upset_plot.py`: the prokaryote **TS** set was read from the **TPS** file, so that bar duplicated TPS. **Re-check that figure.**
- `auto.py`: IQ-TREE ran on the untrimmed `full_msa.faa` instead of the BMGE-trimmed alignment, and `os.chdir(final_msa)` referenced an undefined variable.
- `check.py` labelled every genome `_bacteria` and stopped after the first CDS. It's now `evotools gff cds-ids`.
- Hard-coded cluster paths (`/data/ganeshm/...`, `/softwares/...`) are now command-line options.

## Repository layout

```
src/evotools/        package (one module per topic; each registers its CLI subcommand)
tests/               pytest suite (incl. an end-to-end pipeline test with stand-in tools)
notebooks/           GO enrichment walkthrough with ShinyGO
docs/migration.md    old script → new command mapping
benchmarks/          speed comparisons against the original implementations
```

## Development

```bash
pip install -e ".[dev]"
pytest            # run tests
ruff check .      # lint
```

## Citation

If you use this code, please cite the repository. A paper citation will be added once it's published.

## License

MIT. See [LICENSE](LICENSE).
