#Extract the most frequent donor and recipient from RangerDTL output and create an iTOL dataset for visualization on phylogenetic tree
import os
import argparse
import re

parser= argparse.ArgumentParser()
parser.add_argument("-in", dest="f1", help="input file")
parser.add_argument("-csv", dest="f2", help="CSV file")
parser.add_argument("-con", dest="f3", help="Connection dataset")
parser.add_argument("-name", dest="f4", help="KO/Pathway name")

args=parser.parse_args()

donor=[]
recipient=[]

out1=open(args.f2,'w')
out1.write("Most Frequent Donor\t\tMost Frequent Recipient\tNumber of reconciliations\n")

out2=open(args.f3,'w')
out2.write("DATASET_CONNECTION\nSEPARATOR TAB\nDATASET_LABEL\t"+args.f4+" connections\nCOLOR\t#000000\nDRAW_ARROWS\t1\nARROW_SIZE\t10\nLOOP_SIZE\t100\nMAXIMUM_LINE_WIDTH\t3\nCENTER_CURVES\t1\nALIGN_TO_LABELS\t0\n\nDATA\n\n")

for i in open(args.f1):
	j=i.strip().split("Most Frequent mapping --> ")[1].split(",")[0]
	k=i.strip().split("Most Frequent recipient --> ")[1].split(",")[0]
	l=i.split("Most Frequent mapping --> ")[1].split(", ")[1].split(" times")[0] 
	if j!=k:
		out1.write(j+"\t-->"+"\t"+k+"\t"+l+"\n")
		out2.write(j+"\t"+k+"\t"+l+"\t#f90d0d\tnormal\n")
