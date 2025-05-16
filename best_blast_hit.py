import os,sys
import pandas as pd
from time import time
import dask.dataframe as dd
import itertools
from itertools import permutations
#df = dd.read_csv('large_file.csv')
start = int(sys.argv[1])
stop= int(sys.argv[2])

#df_bbh=pd.DataFrame(columns=["P1","P2","id"])
def bbh(i):
	dfo1=df[(df['g_q']==i[0])&(df['g_h']==i[1])]
	dfo1["id"]=dfo1["id"].astype(float)
	dfo2=df[(df.g_q==i[1])&(df.g_h==i[0])]
	dfo1["id"]=dfo1["id"].astype(float)
	#print(dfo1)
	#print(dfo2)
	if (len(dfo1)>=1)&(len(dfo2)>=1):
		for p in list(dfo1["query"].unique()):
			#print(p)
			dfx=dfo1[dfo1["query"]==p].sort_values(by="id",ascending = 0).iloc[0]
			#print(dfo1[dfo1["query"]==p].sort_values(by="id", ascending = 0))
			if len( dfo2[dfo2["query"]==dfx.hit])>=1:
				d=dfo2[dfo2["query"]==dfx.hit]
				if dfo2[dfo2["query"]==dfx.hit].sort_values(by="id",ascending = 0).iloc[0]["hit"]==p:
					df_bbh.loc[len(df_bbh)]=[dfx.query,dfx.hit,dfx["id"]]
		for p in list(dfo2["query"].unique()):
			dfx2=dfo2[dfo2["query"]==p].sort_values(by="id",ascending = 0).iloc[0]
			#print(dfo2[dfo2["query"]==p].sort_values(by="id"))
			if len(dfo1[dfo1["query"]==dfx2.hit])>=1:
				if dfo1[dfo1["g_h"]==dfx2.g_q].sort_values(by="id",ascending = 0).iloc[0]["hit"]==p:
			#		print(dfx2.query,dfx2.hit,dfx2["id"])
					df_bbh.loc[len(df_bbh)]=[dfx2.query,dfx2.hit,dfx2["id"]]
org=os.listdir("/data/aksharad/og_mcl/og_blast/")
#org=["IPR002831.fa_blast"]
remaining=["IPR001356.fa_blast", "IPR013087.fa_blast"]
org=[x for x in org if not x in remaining]
for f in org[start:stop]:
#	print(f)
	start=time()
	print(start)
	df=dd.read_csv("/data/aksharad/og_mcl/og_blast/%s"%f,sep="\t", header=None).compute()
	df=df.loc[:,[0,1,2]]
	df.columns=["query","hit","id"]
	df["g_q"]=df["query"].str.split("_").str[0]+"_"+df["query"].str.split("_").str[1]
	df["g_h"]=df["hit"].str.split("_").str[0]+"_"+df["hit"].str.split("_").str[1]
	df=df[df.g_q!=df.g_h]
	genome=list(set(list(df.g_q.unique())+ list(df.g_h.unique())))
	genome_comb=[list(x) for x in itertools.combinations(genome, 2) ]
	print(genome_comb)
	df_bbh=pd.DataFrame(columns=["P1","P2","id"])
	print(df)	
	for i in genome_comb:#[start:stop]:
#		print(i)
		bbh(i)	
	#print(df_bbh)
	df_bbh.to_csv("/data/aksharad/og_mcl/og_bbh/%s_bbh"%f, sep="\t", index=False, header=False)
	print(time()-start)
