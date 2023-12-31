import pandas as pd
from sqlalchemy import MetaData, Column, ForeignKey, Integer, SmallInteger, String, Boolean, select, insert, create_engine, delete
from sqlalchemy.orm import sessionmaker, mapped_column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship
from statfunctions import *
from setinfo import scrape_scryfall
from dbpgstrings import host, database, user, password

set_abbr='ltr'#This determines which set we are working with. Current options: ltr, dmu, bro
engine1 = create_engine("sqlite:///23spells.db", echo=False) #used to build tables locally
conn1 = engine1.connect()
"""port='5432'
engine2=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database))
conn2=engine2.connect()"""

metadata = MetaData()
metadata.reflect(bind=engine1)
game_data_table=metadata.tables[set_abbr+'GameData']

#ToDo: Most Integers in tables could be SmallInteger instead to save some memory

# Table Definitions
class Base(DeclarativeBase):
    pass
class ArchRank(Base):
    __tablename__=set_abbr+"ArchRank"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    arc_num=mapped_column(SmallInteger)
    rank=mapped_column(SmallInteger)
    #id encodes both archetype and rank by id=arc_num*7+rank
    #currently rank 0=None and rank 1 is Bronze. <1.5% of matches have no recorded rank, and I don't really know how a game ends up unranked.
    #maybe just scrap unranked? 
    
    

class CardInfo(Base):
    __tablename__=set_abbr+"CardInfo"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    mana_value=mapped_column(SmallInteger)
    color=mapped_column(SmallInteger)
    card_type=mapped_column(String)
    rarity=mapped_column(String)
    # color: W=1, U=2, B=4, R=8, G=16, so color=22 means UBG, color=0 means colorless.
    # card type-A(rtifact), C(reature), E(nchantment), L(and), P(laneswalker), I(nstant), S(orcery), B(attle) 
    # Rarity- C(ommon), U(ncommon), R(are), M(ythic), B(asic land)

class ArchGameStats(Base):
    __tablename__=set_abbr+"ArchGameStats"
    arid=mapped_column(SmallInteger, ForeignKey(set_abbr+"ArchRank.id"), primary_key=True)
    won=mapped_column(Boolean, primary_key=True)
    turns=mapped_column(SmallInteger, primary_key=True) #how long the game lasted
    game_count=mapped_column(Integer) #number of games that fit the preceding criteria
    lands=mapped_column(Integer) #total number of lands in decks that played those games
    n0_drops=mapped_column(Integer) #total number of 0 mana spells
    n1_drops=mapped_column(Integer)
    n2_drops=mapped_column(Integer) 
    n3_drops=mapped_column(Integer)
    n4_drops=mapped_column(Integer)
    n5_drops=mapped_column(Integer)
    n6_drops=mapped_column(Integer)
    n7_drops=mapped_column(Integer)
    n8p_drops=mapped_column(Integer)

class CardGameStats(Base):
    #better structure id/arid/copies : wins/losses
    __tablename__=set_abbr+"CardGameStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id'), primary_key=True)
    arid=mapped_column(SmallInteger, ForeignKey(set_abbr+'ArchRank.id'), primary_key=True)
    copies=mapped_column(SmallInteger, primary_key=True)
    win_count=mapped_column(Integer)
    game_count=mapped_column(Integer)


"""class CardDerivedStats(Base):
    __tablename__=set_abbr+"CardDerivedStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id'), primary_key=True)
    arc_num=mapped_column(SmallInteger, ForeignKey(set_abbr+'ArchRank.arc_num'), primary_key=True)
    min_rank=mapped_column(SmallInteger)
    #more stuff here"""


#Table Building
def populateARID(conn):
    arcs=['W','U','B','R','G','WU','WB','WR','WG','UB','UR','UG','BR','BG','RG','WUB','WUR','WUG','WBR','WBG','WRG',
          'UBR','UBG','URG','BRG','WUBR','WUBG','WURG','WBRG','UBRG','WUBRG'] 
    id=0
    arc_num=0
    df=pd.DataFrame({'id':[],'name':[],'arc_num':[],'rank':[]})
    for arc in arcs:
        for rank in range(0,7):
            df.loc[id]=(id, arc, arc_num, rank)
            id+=1
        arc_num+=1
    df.to_sql(set_abbr+'ArchRank',conn, index=False, if_exists='replace')
    conn.commit()

def populateCardTable(conn):
    df=pd.DataFrame.from_dict(scrape_scryfall(set_abbr=set_abbr),orient='index')
    df.columns=['name','mana_value','color','card_type','rarity']
    df['id']=df.index
    df.to_sql(set_abbr+'CardInfo',conn, if_exists='replace',index=False)
    conn.commit()

def populateArchGameTable(conn): 
    arID=Base.metadata.tables[set_abbr+'ArchRank']
    ag=Base.metadata.tables[set_abbr+'ArchGameStats']
    d=delete(ag)
    conn.execute(d)
    conn.commit()
    ag_name=set_abbr+'ArchGameStats'
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic'] 
    q1=select(arID)
    aridDF=pd.read_sql_query(q1,conn)
    row_count=aridDF.shape[0]
    for id in range(row_count):
        archLabel=aridDF.at[id,'name']
        rank=aridDF.at[id,'rank']
        rankName=ranks[rank]
        print("Filling in {} data with rank {}".format(archLabel,rankName))
        df=getGameDataFrame(archLabel, minRank=rank, maxRank=rank,set_abbr=set_abbr)
        insertdf=pd.DataFrame({ 'arid': [],'won': [],'turns': [],'game_count': [],'lands': [],'n0_drops': [], 'n1_drops': [],
                                                'n2_drops': [], 'n3_drops': [], 'n4_drops': [],
                                                'n5_drops': [], 'n6_drops': [], 'n7_drops': [],
                                                'n8p_drops': []})
        for won in {True, False}:
            df2=df[df["won"]==won]
            maxTurns=max(0,df2.loc[:,'num_turns'].max())
            for turns in range(1,int(maxTurns)+1):
                df3=df2[df2["num_turns"]==turns]
                curve=countCurve(df3,set_abbr=set_abbr)
                games=len(df3.index)
                insertdf.loc[len(insertdf.index)]=[id, won, turns, games,
                                                curve[9], curve[0], curve[1],
                                                curve[2], curve[3], curve[4],
                                                curve[5], curve[6], curve[7],
                                                curve[8]]
        insertdf.to_sql(ag_name,conn,if_exists='append',index=False)
                         
    print("Done")
    conn.commit()

def populateCardGameTable(conn): #this takes a while. could be worth looking for optimizations. #I think df.value_counts() could save time?
    cg_table_name=set_abbr+'CardGameStats'
    cg_table=Base.metadata.tables[cg_table_name]
    d=delete(cg_table)
    conn.execute(d)
    conn.commit()
    arID_table=Base.metadata.tables[set_abbr+'ArchRank']
    card_table=Base.metadata.tables[set_abbr+'CardInfo']
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic'] 
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn)
    arid_count=aridDF.shape[0]
    q2=select(card_table.c.name.label('card_name'),card_table.c.id.label('card_id'))
    cardDF=pd.read_sql_query(q2,conn)
    for arID in range(arid_count):
        archLabel=aridDF.at[arID,'name']
        rank=aridDF.at[arID,'rank']
        rankName=ranks[rank]
        df=getGameDataFrame(archLabel, minRank=rank, maxRank=rank,set_abbr=set_abbr)
        print("Counting cards in {} decks at rank {}".format(archLabel, rankName))
        insertdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
        for n in range(cardDF.shape[0]):
            card=cardDF.at[n,'card_name']
            card_id=cardDF.at[n,'card_id']
            col='deck_'+card 
            partialdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
            partialdf.set_index('copies')
            valdf=df[[col,'won']].value_counts()
            indices=valdf.index.difference({(0,0),(0,1)})
            card_counts={i[0] for i in indices}
            for c in card_counts:
                partialdf.loc[c]=[card_id,arID,c,0,0]
            for (copies, won) in indices:
                partialdf.loc[copies,['win_count','game_count']]+=[valdf[copies,won]*won,valdf[copies,won]]
            insertdf=pd.concat([insertdf,partialdf],axis=0)
        
        insertdf.to_sql(cg_table_name,conn,if_exists='append', index=False)    
        conn.commit()
    conn.commit()
def populateCardStats(): #todo: make the actual table, fill with all stats that would be indexed by arc/card/rankrange
    #incomplete
    arID_table=Base.metadata.tables[set_abbr+'ArchRank']
    card_table=Base.metadata.tables[set_abbr+'CardInfo']
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn)
    arcs=aridDF['name'].unique()
    q2=select(card_table.c.name.label('card_name'),card_table.c.id.label('card_id'))
    cardDF=pd.read_sql_query(q2,conn)
    for arcLabel in arcs:
        df=getGameDataFrame(arcLabel,set_abbr=set_abbr)
        minRank=0
        while minRank<=6:
            0

def populateImpacts():
    #todo: rank splitting, test reasonable samples, maybe other impacts, do I want to handle >1 separately from =1?
    #still in experimental mode, might actually be a stat function as impacts could go in cardstats table
    #for now, better archetypes first. then check correlation of various card/deck metrics with success metrics (i.e. wins)
    MIN_SAMPLE=500
    #arcs=['W','U','B','R','G','WU','WB','WR','WG','UB','UR','UG','BR','BG','RG','WUB','WUR','WUG','WBR','WBG','WRG',
    #      'UBR','UBG','URG','BRG','WUBR','WUBG','WURG','WBRG','UBRG','WUBRG']
    arcs=['UB','WR']
    df=pd.DataFrame({'card_id':[], 'arc_num':[], 'IWD':[], 'aIWD':[], 'aIWD2':[], 'incImpact':[], 'IWND':[]}) #could do rank range too if rank filtering seems okay
    index=0
    card_table=Base.metadata.tables[set_abbr+'CardInfo']
    for arc_num in range(len(arcs)):
        arcdf=getGameDataFrame(arcs[arc_num],set_abbr=set_abbr)
        cards=getCardsWithEnoughGames(arcdf,MIN_SAMPLE)
        arcdist=gameLengthDistDF(arcdf)
        for cardName in cards:
            q=select(card_table.c.id).where(card_table.c.name==cardName)
            card_id=pd.read_sql_query(q,conn).at[0,'id']
            col_deck='deck_'+cardName
            col_drawn='drawn_'+cardName
            col_open='opening_hand_'+cardName
            carddf=arcdf[arcdf[col_deck]>0]
            cardAvgWR=winRate(carddf)
            noCardWR=winRate(arcdf[arcdf[col_deck]==0]) #0 if card is in every deck, which happens for basic lands
            incImpact=round((noCardWR!=0)*(cardAvgWR-noCardWR)*100,3) #inc impact is 0 if it's in every deck
            gldist=gameLengthDistDF(carddf) 
            rate_seen=carddf[carddf[col_drawn]+carddf[col_open]>0].shape[0]/carddf.shape[0]
            record_seen=getRecordByLength(carddf[carddf[col_drawn]+carddf[col_open]>0])
            record_not=getRecordByLength(carddf[carddf[col_drawn]+carddf[col_open]==0])
            gih=record_seen['wins'].sum()/(record_seen['wins'].sum()+record_seen['losses'].sum())
            gns=record_not['wins'].sum()/(record_not['wins'].sum()+record_not['losses'].sum())
            basicIWD=round((gih-gns)*100,3)
            wr_seen=record_seen['wins']/(record_seen['wins']+record_seen['losses'])
            wr_not=record_not['wins']/(record_not['wins']+record_not['losses'])
            impacts=wr_seen-wr_not
            aIWD=round((impacts*gldist).sum()*100,3)
            aIWD2=round((impacts*arcdist).sum()*100,3)
            iWND=round((cardAvgWR-(wr_not*gldist).sum())*100/rate_seen,3)
            if cardName not in ["Island", "Swamp", "Mountain", "Forest", "Plains"]:
                df.loc[index]=(card_id, arc_num, basicIWD, aIWD, aIWD2, incImpact, iWND)
            print(cardName, basicIWD, aIWD, aIWD2, incImpact, iWND)
            index+=1
    return df #temporarily outputs a dataframe rather than building a table
def buildDB(conn):
    Base.metadata.create_all(bind=conn)
    conn.commit()
    populateARID(conn)
    print("Built Archetype-Rank Table")
    populateCardTable(conn)
    print("Built Card Info Table")
    populateArchGameTable(conn)
    print("Built Archetype Game Stats Table")
    populateCardGameTable(conn)
    print("Built Card Game Stats Table")
    print("Done")
    conn.commit()
Base.metadata.create_all(bind=conn)
conn.commit()
populateCardTable(conn)
print("Built Card Info Table")
conn1.close()