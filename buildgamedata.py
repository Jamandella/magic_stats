import sqlite3
import pandas as pd
chunksize = 1000
conn = sqlite3.connect("23Spells.db")
cur = conn.cursor()
progresscount=0
droppable=["expansion","event_type", "game_time", "game_number", "opp_rank", "opp_num_mulligans", "opp_colors", "build_index"]
#experimentally, 1k rows is about 1MB after columns get dropped
address=r".\game_data_public.LTR.PremierDraft.csv"
#data is not ordered chronologically. In each region, earliest game gets gradually later, but recent games are scattered throughout the data 
#To get ~200K lines requiring "draft_time">2023-07-15 works
top=pd.read_csv(address,nrows=2)
for column in top.keys():
    if column[:9]=="sideboard" or column[:7]=="tutored":
        droppable.append(column)
conn.execute("DROP TABLE IF EXISTS game_data")
conn.commit()
for chunk in pd.read_csv(address,chunksize=chunksize):
        df = pd.DataFrame(chunk)
        #df=df[df['draft_time']>2023-07-15]
        progresscount+=1
        df=df.drop(droppable, axis=1)
        df.to_sql('game_data', con=conn, if_exists='append') 
        if progresscount%10==0:
            print("Finished {}K lines of game data csv".format(progresscount))
conn.commit()
print("Done")
conn.close()

