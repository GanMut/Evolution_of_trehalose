# Old script → `evotools` command

Every standalone script from the original `Scripts/python` folder maps to one of the entries below.
The originals in this repo are still in the git history, before the "Restructure into evotools package" commit.

| Old script | Now | Notes |
|---|---|---|
| `auto.py` | `evotools pipeline homologs` + `pipeline species-tree` | Tool paths are options, it can resume, and it fails loudly instead of continuing after a failed step |
| `ids_to_hmmer.py` | `evotools pipeline homologs` (or run `fasta subset` → aligner → `hmmbuild`/`hmmsearch` yourself) | |
| `fast_best_blast_hit.py` / `best_blast_hit.py` | `evotools rbh` | Vectorised, ~200× faster, same pairs. `-j` runs files in parallel instead of `start stop` slicing |
| `ortholougsFinder.py` / `ortholog_finder.py` | `evotools rbh --query-genome-col qOrg --subject-genome-col sOrg --groups` | Fixes lost updates in the Manager dict |
| `matrix.py` / `gene_presence_absence_matrix.py` | `evotools matrix` | Columns are named after each gene folder |
| `intersection.py` | `evotools compare A B --common out.txt` | No longer interactive |
| `upset_plot.py`, `upset_plot_for_pathways.ipynb` | `evotools upset --set NAME=FILE ...` | Fixes the prokaryote TS/TPS mix-up |
| `color_strips.py` | `evotools itol colorstrip --group "Label:#hex=file"` | Any number of groups |
| `green_lines.py` | `evotools itol clades --group "Prok:#00FF00=prok_59_1.txt"` | |
| `hgt_donor_recipient.py` | `evotools itol hgt` | |
| `convert_es_to_decimals.py` | `evotools tree decimals` | Handles `5e-7` and `1E+3` too |
| `tree_from_optroot.py` | `evotools tree optroot` | |
| `umap_test.py` | `evotools embed -m pca\|umap` | Was crashing; ~3000× faster encoding |
| `logistic_regression_using_sklearn.py` | `evotools logreg --target Trehalose_breakdown --categorical Phylum Habitat --drop Org_id` | |
| `first_n_seqs.py` | `evotools fasta head -n N` | |
| `get_sequence.py` | `evotools fasta subset --by header -l list.txt` | |
| `remove_sequence.py` | `evotools fasta subset --by header -l list.txt --exclude` | |
| `randomq.py` | `evotools fasta sample -k 40000 --seed 1` | |
| `transcribe.py` | `evotools fasta transcribe` | |
| `underscore.py` | `evotools fasta clean-headers` | |
| `edited_genomes.py` | `evotools fasta tag-organism --map filename_orgname.txt` | |
| `domains.py` | `evotools fasta slice --start S --end E` | Now **1-based inclusive** (as InterPro/Pfam report). The old script used 0-based Python slicing |
| `gene_count.py` | `evotools fasta count *.faa` | |
| `get_org.py`, `grep_headers.py` | `evotools headers organisms`, `evotools fasta subset` | |
| `get_org_headers.py` + `change_headers.py` | `evotools headers rename --headers db.faa` | Works for any accession, not only `[NWXY]P_` |
| `uniq_id.py` | `evotools headers best-per-organism --format domtblout` | |
| `rRNA_bed.py`, `headers.py` | `evotools gff rrna-bed --product "18S ribosomal RNA"` | Writes proper BED6 directly |
| `check.py` | `evotools gff cds-ids` | |
| `GO_enrichment_ShinyGO.ipynb` | `notebooks/` | Kept as a tutorial |

## Replaced by one-liners

These were thin wrappers around shell commands:

```bash
# download.py: download every URL in a list
xargs -n1 -P4 curl -#O < urls.txt

# unzip.py
gunzip *.gz

# ftps.py: keep the FTP lines matching downloaded files
ls *.gz | grep -F -f - ftps1.txt > ftps2.txt

# getnames.sh: 4th link target from each saved HTML page
while read f; do grep '<a href=' "$f" | sed -n 4p | grep -oP 'href="\K[^"]+'; done < files.txt

# extract_uniq_files.py: genome files that mention any organism in the list
grep -l -F -f bacteria_names.txt *.faa | sort -u > uniqfilenames.txt

# curate.py: in each folder, the file with the most sequences
for d in */; do grep -c '>' "$d"* | sort -t: -k2,2nr | head -1; done
```

## Not carried over

`test.ipynb` was exploratory plotting with large embedded outputs (4.6 MB) and paths from another
user's account. It is left out of the repository.
