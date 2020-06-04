import pandas as pd
import numpy as np


df = pd.read_excel('redcap_data.xlsx')
df_t = df.pivot_table(index=['project_id', 'event_id'], columns='field_name', values='value', aggfunc=np.sum)
df_t_new = df_t.reset_index()
df_t_new['UniqueID'] = df_t_new['project_id'].map(str) + '-' + df_t_new['event_id'].map(str)
df_t_new['Fiscal_Dec'] = np.where(pd.DatetimeIndex(df_t_new['startdate']).month < 4,0,1)
df_t_new['Year'] = pd.DatetimeIndex(df_t_new['startdate']).year + df_t_new['Fiscal_Dec']
df_t_new['Federal_Tax_Benefits'] = df_t_new['fed_taxcbc'] + df_t_new['fed_taxhstgst'] + df_t_new['fed_taxwitb']
df_t_new['Provincial_Tax_Benefits'] = df_t_new['prov_taxotb'] + df_t_new['prov_taxcai']
df_t_new['Data_Source'] = 'OFEC'
df_t_new.rename(columns={'quarter':'Quarter', 'clients_coaching':'Number_of_people_receiving_financial_coaching',
                  'fl_tttworkshops':'Number_of_financial_literacy_trainings_conducted',
                  'ben_claimed_total':'Other_Benefits_Secured'}, inplace=True)


######## INSERT DATA INTO A NEW DF NAMED KPI ##########
KPI = pd.DataFrame(df_t_new, columns=['UniqueID', 'Year', 'Quarter', 'Data_Source', 'Number_of_people_receiving_financial_coaching',
                                       'Number_of_financial_literacy_trainings_conducted', 'Federal_Tax_Benefits',
                                       'Provincial_Tax_Benefits', 'Other_Benefits_Secured'])


# print(KPI.head(10))

KPI.to_excel('KPI.xlsx')
