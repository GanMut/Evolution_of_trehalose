import pandas as pd
from multiprocessing import Pool, Manager, cpu_count

def main(_input:str,_cpu:int,_outFile:str,_print:bool):
    finder=OrthoFinder(_input=_input,num_cores=_cpu)
    df=pd.DataFrame.from_dict(dict(finder.orthoPairDict))
    print(_print)
    if _print :
       print(df.transpose().to_string())
    else:
        print(1)
        df.transpose().to_csv(_outFile,sep='\t')

class OrthoFinder():
	def __init__(self, _input: str, num_cores=4):
		self.df = pd.read_parquet(_input)
		self.df_dict = Manager().dict()
		self.orthoPairDict = Manager().dict()
		self.num_cores = num_cores if num_cores else cpu_count()  # Use specified cores or default to all available cores
		self.dictionary_maker()
		self.orthologsPairFinder()

	def dictionary_maker(self):
		with Pool(processes=self.num_cores) as pool:
			pool.map(self.process_dictionary_maker, self.df['qOrg'].unique())

	def process_dictionary_maker(self, org):
		tempdf = {}
		org_df = self.df[self.df['qOrg'] == org][['qseqid', 'sseqid', 'pident']]
		if not org_df.empty:
			max_pident_rows = org_df.groupby('qseqid').apply(lambda x: x[x['pident'] == x['pident'].max()])

			for qseqid, rows in max_pident_rows.iterrows():
				sseqid_list = rows['sseqid'].tolist() if isinstance(rows['sseqid'], list) else [rows['sseqid']]
				pident_list = rows['pident'].tolist() if isinstance(rows['pident'], list) else [rows['pident']]

				ziped_list = {sseqid: pident for sseqid, pident in zip(sseqid_list, pident_list)}

				if qseqid in tempdf.keys():
					tempdf[qseqid[0]].update(ziped_list)
				else:
					tempdf[qseqid[0]] = ziped_list
			self.df_dict[org] = tempdf
		else:
			print(f"No data found for organism '{org}'")

	def orthologsPairFinder(self):
		with Pool(processes=self.num_cores) as pool:
			pool.starmap(self.process_orthologs_pair_finder, [(org, proteinDict) for org, proteinDict in self.df_dict.items()])

	def process_orthologs_pair_finder(self, org, proteinDict):
		for i_protein, i_proteinDict in proteinDict.items():
			potentialHit = self.potentialSubworker(i_proteinDict)
			for hit_org, hit_proteins in potentialHit.items():
				if hit_org in self.df_dict.keys():
					checkerDict = self.df_dict[hit_org]
					for protein in hit_proteins:
						if protein in checkerDict.keys():
							if i_protein not in self.orthoPairDict.keys():
								if protein not in self.orthoPairDict.keys():
									self.orthoPairDict[i_protein] = [protein]
								else:
									if i_protein not in self.orthoPairDict[protein]:
										self.orthoPairDict[protein].append(i_protein)
									else:
										continue
							else:
								if protein not in self.orthoPairDict[i_protein]:
									self.orthoPairDict[i_protein].append(protein)
								else:
									continue

	def potentialSubworker(self, _dict_: dict):
		result = {}
		for i, v in _dict_.items():
			_=i.split(':')[0]
			if _ in result.keys():
				result[_].update({i:v})
			else:
				result[_] = {i:v}
		return result

# Example usage:
if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser(prog='orthologus group finder from blast format 6')
    parser.add_argument('-i','--input',help='input file',type=str)
    parser.add_argument('-ncpu','--ncpu',help='number of cores',type=int,default=1)
    parser.add_argument('-pr','--print',help='print or not',type=bool,default=False)
    parser.add_argument('-o','--output',help='outfile',type=str)
    args=parser.parse_args()
    main(args.input,args.ncpu,args.output,args.print)
