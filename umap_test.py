import pandas as pd

def main(_input:str,_output:str,ncom:int,method:str):
    df=matrix_prepare(_input=_input)
    match method:
        case 'p':
            result=pca_process(df=df,ncom=ncom)
        case 'u':
            result=umap_process(df=df,ncom=ncom)
        case 'pac':
            pass
    result.to_parquet(_output)

def matrix_prepare(_input:str) -> pd.DataFrame:
    from Bio import SeqIO
    rawSeq={i.id:list(str(i.seq)) for i in SeqIO.parse(_input,'fasta')}
    df=pd.DataFrame.from_dict(rawSeq,orient='index',dtype='category')
    del rawSeq,SeqIO
    val=[]
    col=[]
    for i in df.values:
        for j in i:
            if j not in val:
                val.append(j)
    val.sort()
    for i in df.columns:
        for j in val[1:]:
            col.append(f'pos_{i}_{j}')
    col.sort()
    df2=pd.DataFrame(False,index=df.index,columns=col)
    del col,val
    for i in df2.columns:
        pos=int(i.split('_')[1])
        val=i.split('_')[-1]
        df2.loc[df[df.loc[:,pos]==val].index,i]=True
    return df2

def umap_process(df:pd.DataFrame,ncom:int) -> pd.DataFrame:
    from umap import UMAP
    feature=df.values
    umap=UMAP(n_components=ncom)
    raw_result=umap.fit_transform(feature)
    result_df=pd.DataFrame(raw_result,index=df.index,columns=[f'dim_{i}' for i in range(1,ncom+1)])
    return result_df

def pca_process(df:pd.DataFrame,ncom:int) -> pd.DataFrame:
    from sklearn.decomposition import PCA
    feature=df.values
    pca=PCA(n_components=3)
    raw_result=pca.fit_transform(feature)
    result_df=pd.DataFrame(raw_result,index=df.index,columns=[f'dim_{i}' for i in range(1,ncom+1)])
    return result_df

if __name__=='__main__':
    from argparse import ArgumentParser
    parser=ArgumentParser(prog='umap from msa file')
    parser.add_argument('--input','-i',type=str,help='location of msa in fasta format')
    parser.add_argument('--method','-m',type=str,help='p:pca,pac:pacmap,u:umap',default='pac')
    parser.add_argument('--output','-o',type=str,help='location of parquet which has umap file')
    parser.add_argument('--ncom','-n',type=int,help='n components in umap, default is 3 ',default=3)
    args=parser.parse_args()
    main(_input=args.input,_output=args.output,ncom=args.ncom,PCA=args.method)
