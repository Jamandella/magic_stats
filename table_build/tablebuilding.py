import pandas as pd
from sqlalchemy import MetaData, ForeignKey, Integer, SmallInteger, String, Boolean, DateTime
from sqlalchemy import Column, Table, select, create_engine, delete
from sqlalchemy.orm import mapped_column, DeclarativeBase
from statfunctions import *
from processdraftdata import *
from setinfo import scrape_scryfall
from dbpgstrings import host, database, user, password
import time

set_abbr='ltr'#This determines which set we are working with. Current options: ltr, dmu, bro
engine1 = create_engine("sqlite:///23spells.db", echo=False) #used to build tables locally
conn1 = engine1.connect()
port='5432'
engine2=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database))
conn2=engine2.connect()
metadata = MetaData()

# Table Definitions
class Base(DeclarativeBase):
    pass


class ArchRank(Base):
    __tablename__=set_abbr+"ArchRank"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    arc_num=mapped_column(SmallInteger)
    rank=mapped_column(SmallInteger)
    #lots of things are split up by both archetype and rank, this table encodes both as an int
    #name: main colors as a string (e.g. "B", "WU", "URG", etc.) (this will change after archetypes get refined)
    #arcnum: an int corresponding to the archetype
    #ranks: 0=None, 1=Bronze, 2=Silver, 3=Gold, 4=Platinum, 5=Diamond, 6=Mythic
    #id encodes both archetype and rank by id=arc_num*7+rank, so id//7=arc_num and id%7=rank
    

class CardInfo(Base):
    #The information describing each card in the set
    __tablename__=set_abbr+"CardInfo"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    mana_value=mapped_column(SmallInteger)
    color=mapped_column(SmallInteger)
    card_type=mapped_column(String)
    rarity=mapped_column(String)
    # name: Card name
    # color: 5 bit int with each bit representing presence of a color
    # W=1, U=2, B=4, R=8, G=16, so color:22=10110 means UBG, color=0 means colorless.
    # card type-A(rtifact), C(reature), E(nchantment), L(and), P(laneswalker), I(nstant), S(orcery), B(attle) 
    # multi-type cards are stored like 'AC' for 'Artifact Creature'.
    # Rarity- C(ommon), U(ncommon), R(are), M(ythic), B(asic land)
class DraftInfo(Base):
    __tablename__=set_abbr+'DraftInfo'
    draft_id=mapped_column(String, primary_key=True)
    draft_time=mapped_column(DateTime)
    rank=mapped_column(SmallInteger)
    wins=mapped_column(SmallInteger)
    losses=mapped_column(SmallInteger)
"""class Decklists(Base): 
    __tablename__=set_abbr+'Decks'
    draft_id=mapped_column(Integer, )
    main_colors=mapped_column(String)
    CARDNAME=mapped_column(Integer) #one of these for every card in given set 
"""

class ArchGameStats(Base):
    #For each archetype and rank and length of game (by turns), there is a row for wins and a row for losses
    #In these rows the recorded data is the number of games and the total number of cards of each mana value
    #played in decks in those games.
    #For example: in the row for arid(UB,silver), turns=8, won=True, 
    #n2_drops/game_count would correspond to the average number of 2 drops in UB decks at rank silver that won a game in 8 turns.
    #To get the average number of 2 drops in all UB decks, sum n2_drops and game_count over all rows where the arid corresponds to a UB deck
    #and divide the totals.
    #To get the win rate of arid 40 (currently WU diamond) in 6-turn games: total wins=game count from row arid=40, turns=6 won=True
    #total losses=game_count from row arid=40, turns=6, won=False
    __tablename__=set_abbr+"ArchGameStats"
    arid=mapped_column(SmallInteger, ForeignKey(set_abbr+'ArchRank.id',ondelete='CASCADE',onupdate='CASCADE'),primary_key=True) 
    won=mapped_column(Boolean, primary_key=True)
    turns=mapped_column(SmallInteger, primary_key=True) #how long the game lasted
    game_count=mapped_column(Integer) #number of games that fit the preceding criteria
    lands=mapped_column(Integer) #total number of lands in decks that played those games
    n0_drops=mapped_column(Integer) #total number of 0 mana cards in all of those games
    n1_drops=mapped_column(Integer)
    n2_drops=mapped_column(Integer) 
    n3_drops=mapped_column(Integer)
    n4_drops=mapped_column(Integer)
    n5_drops=mapped_column(Integer)
    n6_drops=mapped_column(Integer)
    n7_drops=mapped_column(Integer)
    n8p_drops=mapped_column(Integer)
    #arid: int that encodes both archetype and rank via the ArchRank table
    #won: whether this row is wins or losses
    #turns: how many turns the game lasted
    #game_count: number of games that match the previous 3 variables (e.g. number of times a WR diamond deck lost in 9 turns)
    #lands: total number of lands in deck in those games
    #nx_drops: total number of x drops in deck in those games (with n8p_drops meaning number of 8+ drops)



class CardGameStats(Base):
    #better structure id/arid/copies : wins/losses
    #id: card id from CardInfo table
    #arid: archetype and rank encoded as one int from ArchRank table
    #copies: number of copies in deck
    #win count: number of wins in deck
    #game count: number of games in deck
    #game played win rate=sum over copies>=1 of win_count/sum over copies>=1 of game_count
    #more stats to come
    __tablename__=set_abbr+"CardGameStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id',ondelete='CASCADE',onupdate='CASCADE'), primary_key=True)
    arid=mapped_column(SmallInteger, ForeignKey(set_abbr+'ArchRank.id',ondelete='CASCADE',onupdate='CASCADE'), primary_key=True)
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
def createDecklists(conn): 
    tableName=set_abbr+'Decklists'
    Base.metadata.reflect(bind=conn)
    if tableName in Base.metadata.tables.keys():
        oldtable=Base.metadata.tables[tableName]
        oldtable.drop(bind=conn)
        print("Dropped previous decklist table")
        conn.commit()
    carddf=cardInfo(set_abbr=set_abbr)
    Base.metadata.reflect(bind=conn)
    cols=[Column('deck_id',Integer, primary_key=True),
          Column('draft_id',String,ForeignKey(set_abbr+'DraftInfo.draft_id')),
          Column('main_colors',String),
          Column('archetype', Integer)]
    for name in carddf['name'].tolist():
        cols.append(Column(name,SmallInteger))
    decktable=Table(set_abbr+'Decklists', Base.metadata, *cols)
    Base.metadata.create_all(bind=conn)
    conn.commit()


def populateDecklists(conn):
    Base.metadata.reflect(bind=conn)
    deck_table=Base.metadata.tables[set_abbr+"Decklists"]
    t0=time.time()
    d=deck_table.delete()
    conn.execute(d)
    conn.commit()
    metadata.reflect(bind=conn1)
    game_data_table=metadata.tables[set_abbr+'GameData']
    s0=select(func.max(game_data_table.c.index))
    size=pd.read_sql_query(s0,conn1).iloc[0,0]
    print("Total rows:",size)
    card_info=cardInfo(set_abbr=set_abbr)
    cards=card_info['name'].to_list()
    cols=[game_data_table.c.draft_id,game_data_table.c.main_colors]
    cols.extend([getattr(game_data_table.c,'deck_'+card).label(card) for card in cards])
    prevIndex=0
    for k in range(size//200000+1):
        s=select(*cols).distinct(game_data_table.c.draft_id)
        t1=time.time()
        s=s.where(game_data_table.c.index>=k*200000, game_data_table.c.index<min((k+1)*200000,size))
        deckDF=pd.read_sql_query(s,conn1)
        oldColOrder=deckDF.columns.to_list()
        deckDF['deck_id']=deckDF.index+prevIndex
        prevIndex=deckDF['deck_id'].max()+1
        deckDF['archetype']=[-1]*deckDF.shape[0]
        newColOrder=['deck_id','draft_id','main_colors','archetype']+oldColOrder[2:]
        deckDF=deckDF[newColOrder]
        print(deckDF.tail())
        deckDF.to_sql(set_abbr+'Decklists',con=conn,index=False,if_exists='append')
        print("Retrieved deck batch in",time.time()-t1)
        conn.commit()
    print("Built decklist table in ",time.time()-t0)



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
    df.sort_index(inplace=True)
    df.to_sql(set_abbr+'CardInfo',conn, if_exists='replace',index=False)
    conn.commit()

def populateDraftInfo(conn):
    draft_table=Base.metadata.tables[set_abbr+'DraftInfo']
    d=draft_table.delete()
    conn.execute(d)
    conn.commit()
    chunksize=45*42*5
    draftdf=pd.DataFrame({'draft_id':[],'draft_time':[], 'rank':[], 'event_match_wins':[],'event_match_losses':[]})
    address=r".\draft_data_public."+set_abbr.upper()+".PremierDraft.csv"
    print("Started reading draft csv")
    progresscount=0
    t0=time.time()
    for chunk in pd.read_csv(address,chunksize=chunksize):
        df = pd.DataFrame(chunk)
        progresscount+=1
        dfp1p1=df[df['pack_number']+df['pick_number']==0]
        draftdf=pd.concat([draftdf,dfp1p1[['draft_id','draft_time','rank','event_match_wins','event_match_losses']]],axis=0)
        if progresscount%50==0:
             print("Processed {} lines in {} total seconds".format((progresscount*chunksize),round(time.time()-t0,3)))
    shorter_names={'event_match_wins':'wins','event_match_losses':'losses'}
    draftdf['rank']=draftdf['rank'].apply(lambda x: rankToNum(x))
    draftdf.rename(columns=shorter_names,inplace=True)
    draftdf.to_sql(set_abbr+'DraftInfo',con=conn,if_exists='append',index_label='draft_id',index=False)
    conn.commit()
    t2=time.time()
    print("Built draft table in ",round(t2-t0,3))

def populateArchGameTable(conn): 
    arID=Base.metadata.tables[set_abbr+'ArchRank']
    """ag=Base.metadata.tables[set_abbr+'ArchGameStats']
    d=delete(ag)
    conn.execute(d)
    conn.commit()"""
    ag_name=set_abbr+'ArchGameStats'
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic'] 
    q1=select(arID)
    aridDF=pd.read_sql_query(q1,conn)
    for id in aridDF.index:
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

def populateCardGameTable(conn): #this takes a while (~20 mins on 1M game LTR data). worth looking for optimizations. 
    cg_table_name=set_abbr+'CardGameStats'
    cg_table=Base.metadata.tables[cg_table_name]
    d=delete(cg_table)
    conn.execute(d)
    conn.commit()
    arID_table=Base.metadata.tables[set_abbr+'ArchRank']
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic'] 
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn,index_col='id')
    cardDF=cardInfo(set_abbr)
    t0=time.time()
    for arID in aridDF.index:
        archLabel=aridDF.at[arID,'name']
        rank=aridDF.at[arID,'rank']
        rankName=ranks[rank]
        df=getGameDataFrame(archLabel, minRank=rank, maxRank=rank,set_abbr=set_abbr)
        print("Counting cards in {} decks at rank {}".format(archLabel, rankName))
        insertdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
        for n in cardDF.index:
            card=cardDF.at[n,'name']
            card_id=n
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
        print("Total time:",time.time()-t0)
        insertdf.to_sql(cg_table_name,conn,if_exists='append', index=False)    
        conn.commit()
    conn.commit()

def populateCardGameTableV2(conn): #slightly faster. Small enough difference to be uncertain.
    #is there a good way to process all cards simultaneously so that each game only gets looked at once?
    #might read faster, but jumping around the write df could be costly.
    #maybe check pandas methods for something helpful
    cg_table_name=set_abbr+'CardGameStats'
    cg_table=Base.metadata.tables[cg_table_name]
    d=delete(cg_table)
    conn.execute(d)
    conn.commit()
    arID_table=Base.metadata.tables[set_abbr+'ArchRank']
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic'] 
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn,index_col='id')
    cardDF=cardInfo(set_abbr)
    t0=time.time()
    arcs=list(set(aridDF['name'].tolist()))
    for archLabel in arcs:
        t2=time.time()
        df=getGameDataFrame(archLabel,set_abbr=set_abbr)
        print("Getting games took",time.time()-t2)
        print("Counting cards in {} decks".format(archLabel))
        insertdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
        arids={}
        for r in range(7):
            arids[ranks[r]]=aridDF[(aridDF['name']==archLabel) & (aridDF['rank']==r)].index[0]
        for n in cardDF.index:
            card=cardDF.at[n,'name']
            card_id=n
            col='deck_'+card 
            partialdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
            valdf=df[[col,'rank','won']].value_counts()
            for copies in valdf.index.get_level_values(0).unique():
                for rank in valdf.index.get_level_values(1).unique():
                    row=partialdf.shape[0]
                    partialdf.loc[row]=[card_id,arids[rank],copies,0,0]
                    for won in valdf.index.get_level_values(2).unique():
                        if (copies,rank,won) in valdf.index:partialdf.loc[row,['win_count','game_count']]+=[valdf[copies,rank,won]*won,valdf[copies,rank,won]]
            insertdf=pd.concat([insertdf,partialdf],axis=0,ignore_index=True)
        print("Color time:",time.time()-t2)
        insertdf.to_sql(cg_table_name,conn,if_exists='append', index=False)    
        conn.commit()
    print("Total time:",time.time()-t0)
    conn.commit()

def populateCardGameTableV3(conn): #Was worth a shot, but ended up slower. 
    #Possible time save: Query multiple columns at a time (can't take too many for memory reasons I think)
    #As is, took 10 cards in 1 minute->27 minutes for whole set (about 35% slower than others)
    cg_table_name=set_abbr+'CardGameStats'
    cg_table=Base.metadata.tables[cg_table_name]
    arID_table=Base.metadata.tables[set_abbr+'ArchRank']
    metadata.reflect(bind=conn1)
    game_data_table=metadata.tables[set_abbr+'GameData']
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic']
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn,index_col='id')
    cardDF=cardInfo(set_abbr)
    t0=time.time()
    #for n in cardDF.index:
    for n in range(10):
        t1=time.time()
        card=cardDF.at[n,'name']
        card_id=n
        col='deck_'+card 
        q=select(game_data_table.c.main_colors,game_data_table.c.rank,getattr(game_data_table.c,col),game_data_table.c.won)
        df=pd.read_sql_query(q,conn)
        print("Query took", time.time()-t1) 
        print("Counting copies of {}".format(card))
        max_copies=int(df[col].max())
        insertdf=pd.DataFrame({'id':[],'arid':[],'copies':[],'win_count':[], 'game_count':[]})
        valdf=df.value_counts()
        for arid in aridDF.index:
            colors=aridDF.loc[arid,'name']
            rank=aridDF.loc[arid,'rank']
            rankName=ranks[rank]
            for copies in range(max_copies+1):
                row=insertdf.shape[0]
                insertdf.loc[row]=[card_id,arid,copies,0,0]
                if (colors,rankName,copies,1) in valdf.index:insertdf.loc[row,['win_count','game_count']]+=[valdf[colors,rankName,copies,1],valdf[colors,rankName,copies,1]]
                if (colors,rankName,copies,0) in valdf.index:insertdf.loc[row,['win_count','game_count']]+=[0,valdf[colors,rankName,copies,0]]

        print("Card {} took {}".format(n,time.time()-t1))
        print(insertdf.iloc[10:20,:])
    print("Total time",time.time()-t0)
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

def buildDB(conn): #Only run this if you are building/rebuilding from the ground up 
    Base.metadata.reflect(bind=conn)
    Base.metadata.drop_all(bind=conn)
    Base.metadata.create_all(bind=conn)
    conn.commit()
    populateARID(conn)
    print("Built Archetype-Rank Table")
    populateCardTable(conn)
    print("Built Card Info Table")
    makeDraftInfo(conn,set_abbr=set_abbr)
    processPacks(conn,set_abbr=set_abbr)
    populateArchGameTable(conn)
    print("Built Archetype Game Stats Table")
    populateCardGameTable(conn)
    print("Built Card Game Stats Table")
    createDecklists(conn)
    populateDecklists(conn)
    print("Built Decklists")
    print("Done")
    conn.commit()
createDecklists(conn2)
populateDecklists(conn2)
Base.metadata.reflect(bind=conn2)
decktable=Base.metadata.tables[set_abbr+'Decklists']
s=select(decktable).limit(10)
print(pd.read_sql_query(s,conn2))

conn1.close()
conn2.close()