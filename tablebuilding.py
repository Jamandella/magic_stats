import pandas as pd
from sqlalchemy import MetaData, Column, ForeignKey, Integer, SmallInteger, String, Boolean, select, insert, create_engine, delete
from sqlalchemy.orm import sessionmaker, mapped_column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import relationship
from statfunctions import *
from dbpgstrings import host, database, user, password

engine1 = create_engine("sqlite:///23spells.db", echo=False) #used to build tables locally
conn1 = engine1.connect()
port='5432'
engine2=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database))
conn2=engine2.connect()

metadata = MetaData()
metadata.reflect(bind=engine1)
game_data_table=metadata.tables['game_data']

#ToDo: Most Integers in tables could be SmallInteger instead to save some memory

# Table Definitions
class Base(DeclarativeBase):
    pass
class LtrArchRank(Base):
    __tablename__="ltrArchRank"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    arc_num=mapped_column(SmallInteger)
    rank=mapped_column(SmallInteger)
    #id encodes both archetype and rank by id=arc_num*7+rank
    
    

class LtrCardInfo(Base):
    __tablename__="ltrCardInfo"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    mana_value=mapped_column(SmallInteger)
    color=mapped_column(String)
    card_type=mapped_column(String)
    rarity=mapped_column(String)
    # color: WUBRG+C for colorless
    # card type-A(rtifact), C(reature), E(nchantment), L(and), P(laneswalker), I(nstant), S(orcery), B(attle) 
    # lower case letters may be appended to card type to flag particular cards. currently y for land cyclers and x for x spells
    # Rarity- C(ommon), U(ncommon), R(are), M(ythic), B(asic land)

class LtrArchGameStats(Base):
    __tablename__="ltrArchGameStats"
    arid=mapped_column(SmallInteger, ForeignKey("ltrArchRank.id"), primary_key=True)
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

class LtrCardGameStats(Base):
    #better structure id/arid/copies : wins/losses
    __tablename__="ltrCardGameStats"
    id=mapped_column(SmallInteger, ForeignKey('ltrCardInfo.id'), primary_key=True)
    arid=mapped_column(SmallInteger, ForeignKey('ltrArchRank.id'), primary_key=True)
    copies=mapped_column(SmallInteger, primary_key=True)
    win_count=mapped_column(Integer)
    game_count=mapped_column(Integer)

class LtrCardDerivedStats(Base):
    __tablename__="ltrCardDerivedStats"
    id=mapped_column(SmallInteger, ForeignKey('ltrCardInfo.id'), primary_key=True)
    arc_num=mapped_column(SmallInteger, ForeignKey('ltrArchRank.arc_num'), primary_key=True)
    min_rank=mapped_column(SmallInteger)
    #more stuff here


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
    df.to_sql('ltrArchRank',conn, index=False, if_exists='replace')
    conn.commit()

def populateCardTable(conn):
    df=pd.DataFrame.from_dict(cardInfo(),orient='index')
    df.columns=['mana_value','color','card_type','rarity']
    df['name']=df.index
    df['id']=range(df.shape[0])
    df=df.set_index('id')
    df.to_sql('ltrCardInfo',conn, if_exists='replace')
    conn.commit()

def populateArchGameTable(conn): 
    arID=Base.metadata.tables['ltrArchRank']
    ag=Base.metadata.tables['ltrArchGameStats']
    d=delete(ag)
    conn.execute(d)
    conn.commit()
    ag_name='ltrArchGameStats'
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic'] 
    q1=select(arID)
    aridDF=pd.read_sql_query(q1,conn)
    row_count=aridDF.shape[0]
    for id in range(row_count):
        archLabel=aridDF.at[id,'name']
        rank=aridDF.at[id,'rank']
        rankName=ranks[rank]
        print("Filling in {} data with rank {}".format(archLabel,rankName))
        df=getGameDataFrame(archLabel, minRank=rank, maxRank=rank)
        insertdf=pd.DataFrame({ 'arid': [],'won': [],'turns': [],'game_count': [],'lands': [],'n0_drops': [], 'n1_drops': [],
                                                'n2_drops': [], 'n3_drops': [], 'n4_drops': [],
                                                'n5_drops': [], 'n6_drops': [], 'n7_drops': [],
                                                'n8p_drops': []})
        for won in {True, False}:
            df2=df[df["won"]==won]
            maxTurns=max(0,df2.loc[:,'num_turns'].max())
            for turns in range(1,int(maxTurns)+1):
                df3=df2[df2["num_turns"]==turns]
                curve=countCurve(df3)
                games=len(df3.index)
                insertdf.loc[len(insertdf.index)]=[id, won, turns, games,
                                                curve[9], curve[0], curve[1],
                                                curve[2], curve[3], curve[4],
                                                curve[5], curve[6], curve[7],
                                                curve[8]]
        insertdf.to_sql(ag_name,conn,if_exists='append',index=False)
                         
    print("Done")
    conn.commit()

def populateCardGameTable(conn, set='ltr'):
    if set=='ltr': 
        cgTable_name='ltrCardGameStats'
    else:
        raise RuntimeError("Invalid set")
    cg_table=Base.metadata.tables[cgTable_name]
    d=delete(cg_table)
    conn.execute(d)
    conn.commit()
    arID_table=Base.metadata.tables['ltrArchRank']
    card_table=Base.metadata.tables['ltrCardInfo']
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
        df=getGameDataFrame(archLabel, minRank=rank, maxRank=rank)
        print("Counting cards in {} decks at rank {}".format(archLabel, rankName))
        for n in range(cardDF.shape[0]):
            card=cardDF.at[n,'card_name']
            card_id=cardDF.at[n,'card_id']
            col='deck_'+card 
            cdf=df[df[col]>0] #selects rows corresponding to games running at least 1 copy of given card
            maxCopies=int(max(0,cdf[col].max())) #yields 0 if no games in cdf
            insertdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
            for copies in range (1, maxCopies+1):
                cdf2=cdf[cdf[col]==copies]
                cdf2w=cdf2[cdf2['won']==True]
                gamesWon=cdf2w.shape[0]
                gamesTotal=cdf2.shape[0]
                insertdf.loc[len(insertdf.index)]=[card_id, arID, copies, gamesWon, gamesTotal]
            insertdf.to_sql(cgTable_name,conn,if_exists='append', index=False)    
        conn.commit()
    conn.commit()
def populateCardStats(): #todo: make the actual table, fill with all stats that would be indexed by arc/card/rankrange
    #incomplete
    arID_table=Base.metadata.tables['ltrArchRank']
    card_table=Base.metadata.tables['ltrCardInfo']
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn)
    arcs=aridDF['name'].unique()
    q2=select(card_table.c.name.label('card_name'),card_table.c.id.label('card_id'))
    cardDF=pd.read_sql_query(q2,conn)
    for arcLabel in arcs:
        df=getGameDataFrame(arcLabel)
        minRank=0
        while minRank<=6:
            0

def populateImpacts():
    #todo: rank splitting, test reasonable samples, maybe other impacts, do I want to handle >1 separately from =1?
    #still in experimental mode
    MIN_SAMPLE=500
    #arcs=['W','U','B','R','G','WU','WB','WR','WG','UB','UR','UG','BR','BG','RG','WUB','WUR','WUG','WBR','WBG','WRG',
    #      'UBR','UBG','URG','BRG','WUBR','WUBG','WURG','WBRG','UBRG','WUBRG']
    arcs=['UB']
    df=pd.DataFrame({'card_id':[], 'arc_num':[], 'IWD':[], 'aIWD':[], 'aIWD2':[], 'incImpact':[], 'IWND':[]}) #could do rank range too if rank filtering seems okay
    index=0
    card_table=Base.metadata.tables['ltrCardInfo']
    for arc_num in range(len(arcs)):
        arcdf=getGameDataFrame(arcs[arc_num])
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

Base.metadata.create_all(conn1)
conn1.commit()
populateCardGameTable(conn1)

conn2.close()
conn1.close()