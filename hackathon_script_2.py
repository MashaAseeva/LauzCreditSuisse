import pandas as pd
from functools import reduce
import numpy as np


def main():

    atm_df = pd.read_csv('atms.small.csv', index_col='id')
    clients_df = pd.read_csv('clients.small.csv', index_col='id')
    companies_df = pd.read_csv('companies.small.csv', index_col='id')
    transactions_df = pd.read_csv('transactions.small.csv', index_col='id')


    #merge companies and clients, then check if any sources or targets are not a company/client

    ccDf = pd.merge(clients_df, companies_df, how='outer', left_index=True, right_index=True, suffixes=('_client', '_company'))

    uniqueSources = set(transactions_df['source'].unique())
    uniqueTargets = set(transactions_df['target'].unique())

    uniqueCc = set(ccDf.index.values)

    diffTargets = [item for item in uniqueTargets if item not in uniqueCc]
    diffSources = [item for item in uniqueSources if item not in uniqueCc]


    #data frame of all possible targets 
    targetDf = pd.merge(ccDf, atm_df, how='outer', left_index=True, right_index=True)

    #merge transactions with sources and targets
    transactions_source_df = pd.merge(transactions_df, targetDf, how='left', left_on='source', right_index=True, suffixes=('', '_source'))
    full_df = pd.merge(transactions_source_df, targetDf, how='left', left_on='target', right_index=True, suffixes=('_source', '_target'))

    #create flow pattern detection data frame

    suspectDf = pd.DataFrame(columns=['sum1', 'sum2', 'ratio', 'client A', 'node B', 'client C', 'common receivers'])

    testDf = targetDf.head(n=100)

    #iterate through all senders
    for client in testDf.index.values.tolist():
        #check all recievers B, C, D
        recieverList = list(full_df[full_df.source == str(client)]['target'])
        for index,row in (full_df[(full_df.source == str(client))]).iterrows():
            #print(str(client))
            #sum1 = np.sum(full_df[(full_df.source == str(client))]['amount'])
            #iterate through all receivers F of the second sender B
            for index2, row2 in (full_df[(full_df.source == row['target'])]).iterrows():
                #print(row['target'])
                #final destination = row2['target']
                #for receiver F, iterate through all senders B
                sum2 = 0
                commonReceivers = []
                for index3, row3 in (full_df[(full_df.target == row2['target']) & (full_df.source.isin(recieverList))]).iterrows():
                    #print (row2['target'])
                    sum2 += row3['amount']
                    commonReceivers.append(row3['source'])

                sum1 = np.sum(full_df[(full_df.source == str(client)) & (full_df.target.isin(commonReceivers))]['amount'])
                ratio = sum1/sum2
                if (.9 < ratio < 1.1) & (sum1 > 1000):
                    if (len(suspectDf) > 0):
                        print(client, row['target'], row2['target'])                    
                        suspectDf.loc[suspectDf.index.max() + 1] = [sum1, sum2, ratio, str(client), row['target'], row2['target'], commonReceivers]
                    else:
                        print(client,row['target'], row2['target'])
                        suspectDf.loc[0] = [sum1, sum2, ratio, str(client), row['target'], row2['target'], commonReceivers]



    suspectDf
    newSusDf = suspectDf
    newSusDf['common receivers'] = newSusDf['common receivers'].apply(lambda x: tuple(x) if type(x) is list else x)
    newSusDf = newSusDf.drop_duplicates()
    newSusDf['common receivers'] = newSusDf['common receivers'].apply(lambda x: list(x) if type(x) is tuple else x)
    newSusDf.to_csv("flow_pattern_medium.txt", sep="|")


    #time pattern data frame 

    #look at transactions with the same source and target 
    paired_transactions = full_df.groupby(['source', 'target']).size().reset_index(name='Freq')

    #sort by transactions of this type that have the highest frequency
    paired_transactions = paired_transactions.sort_values(by='Freq', ascending=False)

    #check for equal time intervals

    #try top 10 for now
    top_paired_df = paired_transactions[(paired_transactions.Freq > 5)]

    flaggedDf = pd.DataFrame(columns=['sender', 'reason','incremental amt','total amt','reciever'])

    for index, row in top_paired_df.iterrows():
        paired_transaction_list = full_df[(full_df.source ==row['source']) & (full_df.target==row['target'])]
        total_sum = np.sum(list(paired_transaction_list['amount']))
        #check if amount is equal for most of them. if amounts vary, we ignore this
        if (len(paired_transaction_list['amount'].unique()) >3):
            continue
        #check if the time interval seems sketchy: should be within a day or two 
        paired_transaction_list['times'] = paired_transaction_list['date']+','+paired_transaction_list['time']
        paired_transaction_list['times'] = pd.to_datetime(paired_transaction_list['times'], format='%d/%m/%y,%h:%m:%s', errors='ignore')

        paired_transactions_time_counter = paired_transaction_list.groupby(['times']).size().reset_index(name='Freq')
        paired_transactions_time_counter = paired_transactions_time_counter.sort_values(by='Freq', ascending=False)

        #get frequency of most duplicated transaction. if it is less than 5, we ignore this 
        if (paired_transactions_time_counter.loc[paired_transactions_time_counter.Freq.idxmax()]['Freq'] < 5):
            continue
        
        if (len(flaggedDf) > 0):
            flaggedDf.loc[flaggedDf.index.max() + 1] = [row["source"], "paired transfer",list(paired_transaction_list['amount'])[1], total_sum, row["target"]]

        else:
            flaggedDf.loc[0] = [row["source"], "paired transfer",list(paired_transaction_list['amount'])[1], total_sum, row["target"]]

        
        #print("red flag!", paired_transaction_list['source'], paired_transaction_list['target'])

        #now, check if that money is summed and given elsewhere; check the target's id
    #     full_df[(full_df.source==row['target'])]

    #     #original sum is 29530; 
    #     full_df[(full_df.source==row['target'])&(full_df.target=="9a364c09-2081-48ac-a526-de6a0657f717")]['amount']

    #     #check if these sum to near 29530
    #     full_df[(full_df.source==row['target'])&(full_df.target=="f464172d-5e75-47ae-b8f1-be3cb23f6453")]['amount']
    #     full_df[(full_df.source==row['target'])&(full_df.target=="b5148c78-57a3-4418-8e88-82416d164e1f")]['amount']


    flaggedDf.to_csv("time_pattern_output.txt", sep="|")
  

main()