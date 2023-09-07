    
def alter_accountingtable_fields(df,dictionary):
  import pandas as pd
  cs = df.columns
  if 'AUGDT' in cs:
    df['AUGDT'] = pd.to_datetime(df['AUGDT'], format = '%Y%m%d', errors = 'coerce')
  if 'BUDAT' in cs:
    df['BUDAT'] = pd.to_datetime(df['BUDAT'], format = '%Y%m%d', errors = 'coerce')
  if 'BLDAT' in cs:
    df['BLDAT'] = pd.to_datetime(df['BLDAT'], format = '%Y%m%d', errors = 'coerce')
  if 'CPUDT' in cs:
    df['CPUDT'] = pd.to_datetime(df['CPUDT'], format = '%Y%m%d', errors = 'coerce')
  if 'DMBTR' in cs:
    df['DMBTR'] = df['DMBTR'].multiply(1/100)
    if 'SHKZG' in cs:
      df.loc[df['SHKZG']== 'S', 'DMBTR'] = -(1)* df['DMBTR']
  if 'WRBTR' in cs:
    df['WRBTR'] = df['WRBTR'].multiply(1/100)
  if 'WOGBTR' in cs:
    df['WOGBTR'] = df['WOGBTR'].multiply(1/100)
    if 'BSCHL' in cs:  
      df.loc[df['SHKZG']== 'S', 'WRBTR'] = -(1)* df['WRBTR']
  if 'UMSKZ' in cs:
    df.drop(df.loc[df['UMSKZ']== "F"].index, inplace=True)
  col_names = pd.read_csv(dictionary,encoding='ISO-8859-1').set_index('Field').to_dict()['Description']  
  df.rename(columns=col_names, inplace = True)
  df.columns = df.columns.str.replace(' ','_')
  
def rename_sap_columns(df, dictionary):
  import pandas as pd
  col_names = pd.read_csv(dictionary,encoding='ISO-8859-1').set_index('Field').to_dict()['Description']  
  df.rename(columns=col_names, inplace = True)
  df.columns = df.columns.str.replace(' ','_')
  

def sap_dateformat_to_idea(df):
  import pandas as pd
  DateColumns = df.columns[df.columns.str.contains('DAT|DT')]
  for column in DateColumns:
    df[column] = pd.to_datetime(df[column], format = '%Y%m%d', errors = 'coerce')

def accountingtable_currency_to_idea(df):
  import pandas as pd
  cs = df.columns
  if 'DMBTR' in cs:
    df['DMBTR'] = df['DMBTR'].multiply(1/100)
    if 'SHKZG' in cs:
      df.loc[df['SHKZG']== 'S', 'DMBTR'] = -(1)* df['DMBTR']
  if 'WRBTR' in cs:
    df['WRBTR'] = df['WRBTR'].multiply(1/100)
    if 'BSCHL' in cs:  
      df.loc[df['SHKZG']== 'S', 'WRBTR'] = -(1)* df['WRBTR']
  if 'UMSKZ' in cs:
    df.drop(df.loc[df['UMSKZ']== "F"].index, inplace=True)

def salestable_currency_to_idea(df):
  import pandas as pd
  CurrColumns = df.filter(like='NETWR', axis=1)
  for column in CurrColumns:
    df[column] = df[column].multiply(1/100)

def spvf_query_to_df(server,user,password,db,query,out_path):
    import subprocess
    import pandas as pd
    pw = subprocess.Popen("where powershell", stdout=subprocess.PIPE).communicate()[0].decode(errors = 'ignore').rstrip()
    subprocess.call(pw+query+" -Database "+db+" -Server "+server+" -User "+user+" -Password "+password+" |Export-Csv -NoTypeInformation -Path '{}' -Encoding UTF8".format(out_path))
    return pd.read_csv(out_path)

def export_df_to_idea(df,dbname):
    import IDEALib as ideaLib
    idea = ideaLib.idea_client()
    ideaLib.py2idea(dataframe= df, databaseName= dbname, client=idea)
    ideaLib.refresh_file_explorer() 
