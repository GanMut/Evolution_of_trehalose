#!/usr/bin/env python3
"""
Gene Presence Matrix Generator

This script generates a presence/absence matrix for genes across multiple organisms.
It takes a list of organisms and searches for gene presence information in specified directories,
then outputs a TSV matrix where 1 indicates presence and 0 indicates absence of a gene in an organism.

Usage:
    python gene_presence_matrix_generator.py --organism_file <path> --search_dirs <dir1> <dir2> ... --output <path>

Arguments:
    --organism_file: Path to file containing organism names (one per line)
    --search_dirs: List of directories containing uniq_organism_final.txt files
    --output: Path to output TSV file
    --pattern: Pattern to search for gene files (default: uniq_organism_final.txt)
    --gene_prefix: Prefix for gene column headers (default: gene)
"""

import os
import glob
import argparse
import sys
from pathlib import Path


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate gene presence/absence matrix")
    parser.add_argument('--organism_file', required=True, help='File containing organism names (one per line)')
    parser.add_argument('--search_dirs', required=True, nargs='+', help='Directories containing gene presence files')
    parser.add_argument('--output', required=True, help='Output TSV file path')
    parser.add_argument('--pattern', default='uniq_organism_final.txt', help='Pattern for gene files (default: uniq_organism_final.txt)')
    parser.add_argument('--gene_prefix', default='gene', help='Prefix for gene column headers (default: gene)')
    return parser.parse_args()


def read_organism_names(filename):
    """Read organism names from file."""
    try:
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        sys.exit(f"Error: Organism file '{filename}' not found")


def find_gene_files(search_dirs, pattern):
    """Find all gene files in the specified directories."""
    gene_files = []
    
    for directory in search_dirs:
        if not os.path.isdir(directory):
            print(f"Warning: Directory '{directory}' not found, skipping", file=sys.stderr)
            continue
            
        # Handle nested directory structures
        for root, _, _ in os.walk(directory):
            matches = glob.glob(os.path.join(root, pattern))
            gene_files.extend(matches)
    
    if not gene_files:
        sys.exit(f"Error: No '{pattern}' files found in the specified directories")
        
    return gene_files


def read_gene_organisms(gene_files):
    """Read organisms that have each gene."""
    gene_organisms = []
    
    for gene_file in gene_files:
        try:
            with open(gene_file, 'r') as f:
                organisms = set(line.strip() for line in f if line.strip())
                gene_organisms.append(organisms)
        except FileNotFoundError:
            print(f"Warning: Gene file '{gene_file}' not found, skipping", file=sys.stderr)
    
    return gene_organisms


def write_matrix(organism_names, gene_organisms, output_file, gene_prefix):
    """Write the presence/absence matrix to a TSV file."""
    try:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        
        with open(output_file, 'w') as f:
            # Write header
            gene_names = [f"{gene_prefix}{i+1}" for i in range(len(gene_organisms))]
            f.write("\t" + "\t".join(gene_names) + "\n")
            
            # Write organism rows
            for organism in organism_names:
                row = [organism]
                for gene_org_set in gene_organisms:
                    row.append("1" if organism in gene_org_set else "0")
                f.write("\t".join(row) + "\n")
                
        print(f"Matrix successfully written to {output_file}")
        print(f"Matrix dimensions: {len(organism_names)} organisms × {len(gene_organisms)} genes")
        
    except IOError as e:
        sys.exit(f"Error writing output file: {e}")


def main():
    """Main function to run the script."""
    args = parse_arguments()
    
    # Read organism names
    print(f"Reading organism names from {args.organism_file}")
    organism_names = read_organism_names(args.organism_file)
    print(f"Found {len(organism_names)} organisms")
    
    # Find gene files
    print(f"Searching for {args.pattern} files in {len(args.search_dirs)} directories")
    gene_files = find_gene_files(args.search_dirs, args.pattern)
    print(f"Found {len(gene_files)} gene files")
    
    # Read gene organisms
    print("Reading gene presence data...")
    gene_organisms = read_gene_organisms(gene_files)
    
    # Write matrix
    print(f"Writing presence/absence matrix to {args.output}")
    write_matrix(organism_names, gene_organisms, args.output, args.gene_prefix)


if __name__ == "__main__":
    main()
