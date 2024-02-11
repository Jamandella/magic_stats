import sqlite3, time
import pandas as pd
import numpy as np
from statfunctions import cardInfo, rankToNum
from sqlalchemy import Integer


#These will only run properly if the draft data set is in the same directory 
chunksize = 42*45*3
#carddf=cardInfo(set_abbr=setName)
#id_dict={'pack_card_'+carddf.at[i,'name']:str(i) for i in carddf.index}

def makeDraftInfo(conn, set_abbr='ltr'):
    t0=time.time()
    draft_table=set_abbr+"DraftInfo"
    address=r".\draft_data_public."+set_abbr.upper()+".PremierDraft.csv"
    draftdf=pd.DataFrame({'draft_id':[],'draft_time':[], 'rank':[], 'event_match_wins':[],'event_match_losses':[]})
    print("Started reading draft csv")
    progresscount=0
    for chunk in pd.read_csv(address,chunksize=chunksize):
        df = pd.DataFrame(chunk)
        progresscount+=1
        dfp1p1=df[df['pack_number']+df['pick_number']==0]
        draftdf=pd.concat([draftdf,dfp1p1[['draft_id','draft_time','rank','event_match_wins','event_match_losses']]],axis=0)
        if progresscount%100==0:
             print("Processed {} lines in {} seconds".format((progresscount*chunksize),round(time.time()-t0,3)))
    shorter_names={'event_match_wins':'wins','event_match_losses':'losses'}
    df['rank']=df['rank'].apply(lambda x: rankToNum(x))
    draftdf.rename(columns=shorter_names,inplace=True)
    t1=time.time()
    draftdf.to_sql(draft_table,con=conn,if_exists='replace',index_label='draft_id',index=False)
    conn.commit()
    t2=time.time()
    print("Built draft info table in ",round(t2-t1,3))
def processPacks(conn, set_abbr='ltr'): 
    pack_table=set_abbr+"DraftPacks"
    address=r".\draft_data_public."+set_abbr.upper()+".PremierDraft.csv"
    t0=time.time()
    top=pd.read_csv(address,nrows=2)
    col_indices=[]
    colnames=top.keys().to_list()
    for i in range(len(colnames)):
        key=colnames[i]
        if key[:5]=='pack_' or key=='pick_number': #includes pack_number and pack_card_[cardname]
            col_indices.append(i)
    count=0
    for chunk in pd.read_csv(address,chunksize=chunksize):
        df = pd.DataFrame(chunk)    
        df.fillna(0,inplace=True)  
        #df.rename(columns=id_dict,inplace=True) #for if we want keys to be the idx of [cardname] in cardInfo rather than pack_card_[cardname]
        contentdf=df.iloc[:,col_indices].groupby(['pack_number','pick_number']).sum()
        if count==0:
            totaldf=contentdf
            count+=1
        else:
            if contentdf.shape==totaldf.shape:
                totaldf=totaldf+contentdf
                count+=1
            else:
                print("Mismatch. Incomplete draft data somewhere.")
                for idx in contentdf.index:
                    totaldf.loc[idx]+=contentdf.loc[idx] 
                    count+=1
            if count%100==0:
                print("Processed {} packs in {} seconds".format((count*chunksize),round(time.time()-t0,3)))
                print(totaldf.shape)
    totaldf=totaldf.astype('int64')
    totaldf.to_sql(pack_table,con=conn,if_exists='replace',index=True,dtype=Integer)
    conn.commit()
    print("Finished pack table")
     


