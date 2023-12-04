import sqlite3
import pandas as pd
chunksize = 1000
conn = sqlite3.connect("23Spells.db")
cur = conn.cursor()
progresscount=0
maybe_droppable=["expansion","event_type", "game_time", "game_number", "opp_rank", "opp_num_mulligans", "opp_colors", "build_index"]
#experimentally, 1k rows is about 1MB after columns get dropped. currently has nearly 1M rows, so 1GB.
setName="ltr" #available sets are currently 'ltr',bro' and 'dmu'
tbl_name=setName+"GameData"
address=r".\game_data_public."+setName.upper()+".PremierDraft.csv"
#data is not ordered chronologically. In each chunk, earliest game gets gradually later, but recent games are scattered throughout the data 
top=pd.read_csv(address,nrows=2)
droppable=[]
print(top.keys())
for column in top.keys():
    if column[:9]=="sideboard" or column[:7]=="tutored":
        droppable.append(column)
    elif column in maybe_droppable:
         droppable.append(column)
conn.execute("DROP TABLE IF EXISTS {}".format(tbl_name))
conn.execute("DROP TABLE IF EXISTS game_data")
conn.commit()
for chunk in pd.read_csv(address,chunksize=chunksize):
        df = pd.DataFrame(chunk)
        #uncomment the following line to filter to the most recent ~25% of drafts 
        #df=df[df['draft_time']>2023-07-20]
        progresscount+=1
        df=df.drop(droppable, axis=1)
        df.to_sql(tbl_name, con=conn, if_exists='append') 
        if progresscount%10==0:
            print("Finished {}K lines of {} game data csv".format(progresscount,setName))
conn.commit()
print("Done")
conn.close()

