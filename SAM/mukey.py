
import pandas as pd


df = pd.read_csv(r'E:\recipes_reg07\recipes_05010001cdl_2011.txt')
df['scenarioID'].values.tolist()

print df.columns.values.tolist()
print  df.iloc[0]
print df.iloc[1]

df.columns = df.iloc[0]
df= df.reindex(df.index.drop(0))


print df.columns

scenarios = df['scenarioID'].values.tolist()
df['mukey_a']=df['scenarioID'].map(lambda x: str(x)[2:]).astype(str)
print df.iloc[1]

df['mukey_a']=df['scenarioID'].map(lambda x: str(x.split('cdl'))[0]).astype(str)

df['mukey_b']=df['mukey_b'].map(lambda x: str(x.split('cdl'))).astype(str)

df['mukey_b']=df['mukey_b'].map(lambda x: str(x.split('cdl')))

df['mukey_b']=df['mukey_a'].map(lambda x: str(x.split('cdl')))
df['mukey_b']=df['mukey_a'].map(lambda x: str(x.split('cdl')[0]))
print  df.iloc[1]

df['mukey']=df['mukey_b'].map(lambda x: str(x.split('st')[0]))
print df.iloc[1]

mukeys = list(set(df['mukey'].values.tolist()))
print len(mukeys)

df['state']=df['scenarioID'].map(lambda x: str(x)[:2]).astype(str)
states= list(set(df['state'].values.tolist()))
print states

filtered_df = df.loc[df['state']=='NY']

filtered_df = df.loc[df['state']=='NY']
mukeys = list(set(filtered_df['mukey'].values.tolist()))
print len(mukeys)

filtered_df.to_csv(r'C:\Users\JConno02\OneDrive - Environmental Protection Agency (EPA)\Documents_C_drive\Projects\SAM\mukey.csv')
