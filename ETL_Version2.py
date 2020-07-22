# Setting up
# Please check is import/export path and import/export filename is correct.

try:
    #in_path = 'D:/TD/TD MindPower/'
    # out_path = 'D:/TD/TD MindPower/output/MasterTable.xlsx'
    #print('FEPS_dboOutcomes.csv')

    import subprocess
    import sys

    def install(package):
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

    install("pandas")
    install("numpy")
    install("xlrd")
    install("openpyxl")

    import os
    import pandas as pd
    import numpy as np

    os.chdir(os.path.dirname(__file__))
    print(os.path.dirname(__file__))
    #OFEC

    df = pd.read_excel('redcap_data.xlsx')
    df_t = df.pivot_table(index=['project_id', 'event_id'], columns='field_name', values='value', aggfunc=np.sum)
    df_t_new = df_t.reset_index()
    df_t_new['ProjectID'] = df_t_new['project_id']
    df_t_new['Year'] = pd.DatetimeIndex(df_t_new['startdate']).year
    df_t_new['Federal_Tax_Benefits'] = df_t_new['fed_taxcbc'] + df_t_new['fed_taxhstgst'] +df_t_new['fed_taxwitb']
    df_t_new['Provincial_Tax_Benefits'] = df_t_new['prov_taxotb'] + df_t_new['prov_taxcai']
    df_t_new['Data_Source'] = 'OFEC'
    df_t_new.rename(columns={'quarter':'Quarter', 'clients_coaching':'Number_of_people_receiving_financial_coaching',
                    'fl_tttworkshops':'Number_of_financial_literacy_trainings_conducted',
                    'ben_claimed_total':'Other_Benefits_Secured'}, inplace=True)

    project = pd.read_excel('project_id.xlsx')
    df_t_new = df_t_new.set_index('ProjectID').join(project.set_index('project_id'))
    ######## INSERT DATA INTO A NEW DF NAMED KPI ##########
    KPI_OFEC = pd.DataFrame(df_t_new, columns=['project_id', 'site_name', 'Year', 'Quarter', 'Data_Source', 'Number_of_people_receiving_financial_coaching',
                                        'Number_of_financial_literacy_trainings_conducted', 'Federal_Tax_Benefits',
                                        'Provincial_Tax_Benefits', 'Other_Benefits_Secured'])

    #FEPS
    #Import service info
    df = pd.read_csv("FEPS_dboServices.csv",
                    skipinitialspace = True,
                    usecols = list(range(0,5)) + list(range(6,186)),
                    dtype = {"Agency":"str"},
                    keep_default_na = True)

    df2 = df.iloc[:,0:7]
    fis_year = pd.PeriodIndex(pd.to_datetime(df2.iloc[:,3], format='%d-%m-%Y', errors='coerce'),freq='Q-MAR')

    df2['year'] = pd.DataFrame(fis_year.astype(str).str[0:4].astype(int)) -1
    df2['quarter'] = pd.DataFrame(fis_year.astype(str).str[5])
    df2['SessionID'] = df2['SessionID'].astype(str)

    #import outcome info
    df_2 = pd.read_csv('FEPS_dboOutcomes.csv',
                    skipinitialspace = True,
                    usecols = range(0,7),
                    dtype = {"Agency":"str",
                            "SessionID":"str",
                            "OCMSID":"str",
                            "Area":"str",
                            "Description":"str",
                            "Amount":"float64"
                            },
                        parse_dates=['TaxYear'],
                    keep_default_na = True)

    table = pd.pivot_table(df_2, values='Amount', index=['SessionID'],
                        columns=['Description'], aggfunc=np.sum).reset_index().fillna(0).loc[:,['SessionID']]
    tax = pd.pivot_table(df_2[df_2['Area'] == 'Income Tax Secured'], values='Amount', index=['SessionID'],
                        columns=['Description'], aggfunc=np.sum).reset_index().fillna(0)
    tax['Federal Tax Benefits'] = tax['Tax Refund'] + tax['GST'] + tax['Canada Child Benefit']
    tax['Provincial Tax Benefits'] = tax['OTB'] + tax['OSTC'] + tax['OEPTC'] + tax['Provincial Child'] 
    OtherBenefits = pd.pivot_table(df_2[df_2['Area'] == 'Other Benefits Secured'], values='Amount', index=['SessionID'],
                        columns=['Area'], aggfunc=np.sum).reset_index().fillna(0)
    table1 = pd.merge(table, tax,  how='left', left_on=['SessionID'], right_on = ['SessionID'])
    table1 = pd.merge(table1, OtherBenefits,  how='left', left_on=['SessionID'], right_on = ['SessionID'])
    table2 = table1.loc[:,['SessionID','Federal Tax Benefits','Provincial Tax Benefits','Other Benefits Secured']].fillna(0)

    #Join two tables
    master = df2.set_index('SessionID').join(table2.set_index('SessionID')).reset_index()
    master[master.duplicated(subset=['SessionID','OCMSID'], keep= False)]
    FEPS = master.loc[:,['Agency','year','quarter']].sort_values(by=['Agency','year','quarter'])
    FEPS['Data_Source'] = 'FEPS'
    FEPS.drop_duplicates(keep="first",inplace=True)
    a = master.groupby(['Agency','year','quarter']).OCMSID.nunique().reset_index()
    FEPS['Number_of_people_receiving_financial_coaching'] = a.iloc[:,3].values
    b = master.groupby(['Agency','year','quarter'])['Federal Tax Benefits'].sum().reset_index()
    FEPS['Federal Tax Benefits'] = b.iloc[:,3].values
    c = master.groupby(['Agency','year','quarter'])['Provincial Tax Benefits'].sum().reset_index()
    FEPS['Provincial Tax Benefits'] = c.iloc[:,3].values
    d = master.groupby(['Agency','year','quarter'])['Other Benefits Secured'].sum().reset_index()
    FEPS['Other Benefits Secured'] = d.iloc[:,3].values
    FEPS.fillna(0)

    agency = pd.read_excel('Agency_id.xlsx')
    FEPS = FEPS.set_index('Agency').join(agency.set_index('Agency')).reset_index()
    FEPS.rename(columns={'Agency': 'site_name',
                        'Agency_id': 'project_id',
                        'year':'Year',
                        'quarter':'Quarter',
                        'Federal Tax Benefits': 'Federal_Tax_Benefits',
                        'Provincial Tax Benefits': 'Provincial_Tax_Benefits', 
                        'Other Benefits Secured': 'Other_Benefits_Secured'
                        }, inplace = True)

    ######## INSERT DATA INTO A NEW DF NAMED KPI ##########
    KPI_FEPS = pd.DataFrame(FEPS, columns=['project_id', 'site_name','Year', 'Quarter', 'Data_Source', 'Number_of_people_receiving_financial_coaching',
                                    'Federal_Tax_Benefits','Provincial_Tax_Benefits', 'Other_Benefits_Secured'])

    KPI_FEPS['Number_of_people_receiving_financial_coaching'].astype(str)
    KPI_FEPS['Federal_Tax_Benefits'].astype(str)
    KPI_FEPS['Provincial_Tax_Benefits'].astype(str)
    KPI_FEPS['Other_Benefits_Secured'].astype(str)
    mastertable = pd.concat([KPI_OFEC, KPI_FEPS])
    mastertable = mastertable[["Data_Source",
                            "Year", 
                            "Quarter", 
                            "site_name", 
                            "project_id",
                            "Number_of_financial_literacy_trainings_conducted",
                            "Number_of_people_receiving_financial_coaching",
                            "Federal_Tax_Benefits",
                            "Provincial_Tax_Benefits",
                            "Other_Benefits_Secured"]]

    mastertable.to_excel('MasterTable.out.xlsx', sheet_name='Master')
except Exception as err:
    print(err)
finally:
    input("Done")