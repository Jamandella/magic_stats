import pandas as pd
from sqlalchemy import MetaData, ForeignKey, Integer, SmallInteger, String, Boolean, DateTime, func
from sqlalchemy import Column, Table, select, create_engine, delete, update,insert
from sqlalchemy.orm import mapped_column, DeclarativeBase
from statfunctions import *
from processdraftdata import *
from setinfo import scrape_scryfall
from dbpgstrings import host, database, user, password
import time

set_abbr='ltr'#This determines which set we are working with. Current options: ltr, dmu, bro
engine1 = create_engine("sqlite:///23spells.db", echo=False) #used to build tables locally. use only for GameData.
conn1 = engine1.connect()
port='5432'
engine2=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database))
conn2=engine2.connect()
metadata = MetaData()

# Table Definitions
class Base(DeclarativeBase):
    pass


class Archetypes(Base):
    __tablename__=set_abbr+"Archetypes"
    id=mapped_column(SmallInteger, primary_key=True)
    archLabel=mapped_column(String)
    num_drafts=mapped_column(Integer)
    num_wins=mapped_column(Integer)
    num_losses=mapped_column(Integer)
    #id: For id<32, id encodes main colors by W=1, U=2, B=4, R=8, G=16, e.g. 25=11001=WRG
    #archLabel=main colors currently. When more refined archetypes get made this will change.
    #num_drafts=number of drafts recorded for this archetype
    #num_wins / losses = total games won/lost recorded for this archetype
    

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
    draft_id=mapped_column(String, )
    main_colors=mapped_column(String)
    CARDNAME=mapped_column(Integer) #one of these for every card in given set 
"""

class ArchGameStats(Base):
    #For each archetype and length of game (by turns), there is a row for wins and a row for losses
    #In these rows the recorded data is the number of games and the total number of cards of each mana value
    #played in decks in those games.
    #For example: in the row for arid(UB,silver), turns=8, won=True, 
    #n2_drops/game_count would correspond to the average number of 2 drops in UB decks at rank silver that won a game in 8 turns.
    #To get the average number of 2 drops in all UB decks, sum n2_drops and game_count over all rows where the arid corresponds to a UB deck
    #and divide the totals.
    #To get the win rate of arid 40 (currently WU diamond) in 6-turn games: total wins=game count from row arid=40, turns=6 won=True
    #total losses=game_count from row arid=40, turns=6, won=False
    __tablename__=set_abbr+"ArchGameStats"
    arch_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'Archetypes.id',ondelete='CASCADE'),primary_key=True) 
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
    #arch_id: int labeling the archetype from the Archetypes table
    #won: whether this row is wins or losses
    #turns: how many turns the game lasted
    #game_count: number of games that match the previous 3 variables (e.g. number of times a WR diamond deck lost in 9 turns)
    #lands: total number of lands in deck in those games
    #nx_drops: total number of x drops in deck in those games (with n8p_drops meaning number of 8+ drops)



class CardGameStats(Base):
    #better structure id/arid/copies : wins/losses
    #id: card id from CardInfo table
    #arid: archetype as an int from Archetypes table
    #copies: number of copies in deck
    #win count: number of wins in deck
    #game count: number of games in deck
    #game played win rate=sum over copies>=1 of win_count/sum over copies>=1 of game_count
    #more stats to come
    __tablename__=set_abbr+"CardGameStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id',ondelete='CASCADE'), primary_key=True)
    arch_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'Archetypes.id',ondelete='CASCADE'), primary_key=True)
    copies=mapped_column(SmallInteger, primary_key=True)
    win_count=mapped_column(Integer)
    game_count=mapped_column(Integer)



"""class CardDerivedStats(Base):
    __tablename__=set_abbr+"CardDerivedStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id',ondelete='CASCADE'), primary_key=True)
    arch_id=
    #more stuff here"""



#Table Building
def createDecklists(conn): 
    tableName=set_abbr+'Decklists'
    Base.metadata.reflect(bind=conn)
    if tableName in Base.metadata.tables.keys():
        oldtable=Base.metadata.tables[tableName]
        oldtable.drop(bind=conn)
        print("Dropped previous decklist table")
        Base.metadata.clear()
        conn.commit()
    carddf=cardInfo(conn=conn,set_abbr=set_abbr)
    cols=[Column('deck_id',Integer, primary_key=True),
          #Column('draft_id',String,ForeignKey(set_abbr+'DraftInfo.draft_id',ondelete='CASCADE')),
          Column('draft_id',String),
          Column('main_colors',String),
          Column('arch_id', SmallInteger, ForeignKey(set_abbr+'Archetypes.id',ondelete='CASCADE'))]
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
    metadata2=MetaData()
    metadata2.reflect(bind=conn1)
    game_data_table=metadata2.tables[set_abbr+'GameData']
    s0=select(func.max(game_data_table.c.index))
    size=pd.read_sql_query(s0,conn1).iloc[0,0]
    card_info=cardInfo(conn=conn,set_abbr=set_abbr)
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
        deckDF['arch_id']=deckDF['main_colors'].map(lambda x: colorInt(x))
        newColOrder=['deck_id','draft_id','main_colors','arch_id']+oldColOrder[2:]
        deckDF=deckDF[newColOrder]
        print(deckDF.tail())
        deckDF.to_sql(set_abbr+'Decklists',con=conn,index=False,if_exists='append')
        print("Retrieved deck batch in",time.time()-t1)
        conn.commit()
    print("Built decklist table in ",time.time()-t0)



def populateArchetypes(conn):
    arcs=['C','W','U','WU','B','WB','UB','WUB','R','WR','UR','WUR','BR','WBR','UBR','WUBR','G',
          'WG','UG','WUG','BG','WBG','UBG','WUBG','RG','WRG','URG','WURG','BRG','WBRG','UBRG','WUBRG'] 
    df=pd.DataFrame({'id':[],'archLabel':[],'num_drafts':[],'num_wins':[],'num_losses':[]})
    for i in range(len(arcs)):
        df.loc[i]=(i, arcs[i], 0, 0, 0)  #todo: After filling in draft info/decklists, use that to count drafts and records
        
    #df.to_sql(set_abbr+'Archetypes',conn, index=False, if_exists='replace')
    df.to_sql(set_abbr+'Archetypes',conn, index=False, if_exists='append')
    conn.commit()

def completeArchetypes(conn): #Untested
    Base.metadata.reflect(bind=conn)
    arch_table=Base.metadata.tables[set_abbr+'Archetypes']
    decklist_table=Base.metadata.tables[set_abbr+'Decklists']
    arch_game_table=Base.metadata.tables[set_abbr+'ArchGameStats']
    s=select(arch_table)
    archdf=pd.read_sql_query(s,conn,index_col='id')
    for i in archdf.index:
        count_draft_q=select(func.count(1).label('drafts')).where(decklist_table.c.arch_id==i)
        drafts=pd.read_sql_query(count_draft_q,conn).iat[0,0]
        print("drafts:",drafts)
        count_game_q=select(func.sum(arch_game_table.c.game_count).label('games')).where(arch_game_table.c.arch_id==i).group_by(arch_game_table.c.won)
        gamedf=pd.read_sql_query(count_game_q,conn)
        print(gamedf)
        if 1 in gamedf.index: wins=gamedf.at[1,'games']
        else: wins=0
        if 0 in gamedf.index: losses=gamedf.at[0,'games']
        else: losses=0
        u=update(arch_table).where(arch_table.c.id==int(i)).values(num_drafts=int(drafts),num_wins=int(wins),num_losses=int(losses))
        conn.execute(u)
    conn.commit()

def populateCardTable(conn):
    df=pd.DataFrame.from_dict(scrape_scryfall(set_abbr=set_abbr),orient='index')
    df.columns=['name','mana_value','color','card_type','rarity']
    df['id']=df.index
    df.sort_index(inplace=True)
    #df.to_sql(set_abbr+'CardInfo',conn, if_exists='replace',index=False)
    df.to_sql(set_abbr+'CardInfo',conn, if_exists='append',index=False)
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
    arch_table=Base.metadata.tables[set_abbr+'Archetypes']
    ag_name=set_abbr+'ArchGameStats'
    q1=select(arch_table.c.id,arch_table.c.archLabel)
    archDF=pd.read_sql_query(q1,conn)
    archDF.index=archDF['id']
    carddf=cardInfo(conn,set_abbr=set_abbr)
    #first=True
    for i in archDF.index:
        main_colors=archDF.at[i,'archLabel'] #Uses that archetype=colors at the moment. Will need to adapt when more archs exist.
        print("Filling in {} data".format(main_colors))
        df=getGameDataFrame(main_colors=main_colors,set_abbr=set_abbr)
        insertdf=pd.DataFrame({ 'arch_id': [],'won': [],'turns': [],'game_count': [],
                               'lands': [],'n0_drops': [], 'n1_drops': [],
                                'n2_drops': [], 'n3_drops': [], 'n4_drops': [],
                                'n5_drops': [], 'n6_drops': [], 'n7_drops': [],
                                'n8p_drops': []})
        for won in {True, False}:
            wondf=df[df["won"]==won]
            maxTurns=max(0,wondf.loc[:,'num_turns'].max())
            for turns in range(1,int(maxTurns)+1):
                turndf=wondf[wondf["num_turns"]==turns]
                curve=countCurve(turndf,carddf)
                games=len(turndf.index)
                insertdf.loc[len(insertdf.index)]=[i, won, turns, games,
                                                curve[9], curve[0], curve[1],
                                                curve[2], curve[3], curve[4],
                                                curve[5], curve[6], curve[7],
                                                curve[8]]
        """if first: #The first time through the loop, replace the old table if there is one. 
            insertdf.to_sql(ag_name,conn,if_exists='replace',index=False,)
            first=False
        else: insertdf.to_sql(ag_name,conn,if_exists='append',index=False)"""
        insertdf.to_sql(ag_name,conn,if_exists='append',index=False)
    print("Done")
    conn.commit()

def populateCardGameTable(conn):
    cg_table_name=set_abbr+'CardGameStats'
    cg_table=Base.metadata.tables[cg_table_name]
    arch_table=Base.metadata.tables[set_abbr+'Archetypes']
    q1=select(arch_table.c.id,arch_table.c.archLabel)
    archDF=pd.read_sql_query(q1,conn,index_col='id')
    cardDF=cardInfo(conn=conn,set_abbr=set_abbr)
    t0=time.time()
    #first=True
    for arch_id in archDF.index:
        main_colors=archDF.at[arch_id,'archLabel'] #Will need to be adapted when more archetypes exist
        df=getGameDataFrame(main_colors=main_colors,set_abbr=set_abbr)
        print("Counting cards in {} decks".format(main_colors))
        insertdf=pd.DataFrame({'id':[],'arch_id':[],'copies':[],'win_count':[], 'game_count':[]})
        for n in cardDF.index:
            card=cardDF.at[n,'name']
            card_id=n
            col='deck_'+card 
            partialdf=pd.DataFrame({'id':[],'arch_id':[],'copies':[],'win_count':[], 'game_count':[]})
            partialdf.set_index('copies')
            valdf=df[[col,'won']].value_counts() 
            #^This makes valdf a series which is indexed by number of copies of the given card and a bit for win/loss 
            #and the values are the number of games won/lost with that number of copies of [card] in the deck.
            indices=valdf.index.difference({(0,0),(0,1)})
            card_counts={i[0] for i in indices}
            for c in card_counts:
                partialdf.loc[c]=[card_id,arch_id,c,0,0]
            for (copies, won) in indices:
                partialdf.loc[copies,['win_count','game_count']]+=[valdf[copies,won]*won,valdf[copies,won]]
            insertdf=pd.concat([insertdf,partialdf],axis=0)
        print("Total time:",time.time()-t0)
        """"if first: 
            insertdf.to_sql(cg_table_name,conn,if_exists='replace', index=False)
            first=False
        else: insertdf.to_sql(cg_table_name,conn,if_exists='append', index=False) """
        insertdf.to_sql(cg_table_name,conn,if_exists='append', index=False) 
        conn.commit()
    conn.commit()

def populateCardStats(conn): #todo: make the actual table, fill with all stats that would be indexed by arc/card/rankrange
    #incomplete
    arID_table=Base.metadata.tables[set_abbr+'ArchRank']
    card_table=Base.metadata.tables[set_abbr+'CardInfo']
    q1=select(arID_table)
    aridDF=pd.read_sql_query(q1,conn)
    arcs=aridDF['name'].unique()
    q2=select(card_table.c.name.label('card_name'),card_table.c.id.label('card_id'))
    cardDF=pd.read_sql_query(q2,conn)
    for arcLabel in arcs:
        df=getGameDataFrame(main_colors=arcLabel,set_abbr=set_abbr)
        minRank=0
        while minRank<=6:
            0

def populateImpacts(conn):
    #todo: test reasonable samples, maybe other impacts, do I want to handle >1 separately from =1?
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
        arcdf=getGameDataFrame(main_colors=arcs[arc_num],set_abbr=set_abbr)
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
def tableCensus(conn): #For testing purposes. Go through each table and sample the contents.
    Base.metadata.reflect(bind=conn)
    for table_name in Base.metadata.tables.keys():
        table=Base.metadata.tables[table_name]
        s1=select(table).limit(3)
        df=pd.read_sql_query(s1,conn)
        print("Table: ",table_name)
        print(df)
        s2=select(func.count(1)).select_from(table)
        print("Size:", conn.execute(s2).fetchall())

def dropSet(conn):
    Base.metadata.reflect(bind=conn)
    for i in range(len(Base.metadata.tables.keys()-1,-1,-1)):
        table_name=Base.metadata.tables.keys(i)
        if table_name.startswith(set_abbr) and table_name[-8:]!='GameData':
            print("Dropping ",table_name)
            tbl=Base.metadata.tables[table_name]
            tbl.drop(bind=conn)
    Base.metadata.clear()
    conn.commit()
def clearSet(conn):
    Base.metadata.reflect(bind=conn)
    for i in range(len(Base.metadata.tables.keys()-1,-1,-1)):
        table_name=Base.metadata.tables.keys(i)
        if table_name.startswith(set_abbr) and table_name[-8:]!='GameData':
            print("Deleting contents of ",table_name)
            tbl=Base.metadata.tables[table_name]
            d=delete(tbl)
            conn.execute(d)
            conn.commit()
    Base.metadata.clear()
    conn.commit()
def buildDBLoc(conn): #Only run this if you are building/rebuilding from the ground up 
    Base.metadata.create_all(bind=conn)
    conn.commit()
    populateArchetypes(conn)
    print("Built Archetype Table")
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
    completeArchetypes(conn)
    print("Filled in archetype summaries")
    print("Done")
    conn.commit()
def buildDBServer(conn): #Only run this if you are building/rebuilding from the ground up 
    clearSet(conn)
    Base.metadata.reflect(bind=conn) 
    Base.metadata.create_all(bind=conn)
    populateArchetypes(conn)
    print("Built Archetype Table")
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
    completeArchetypes(conn)
    print("Filled in archetype summaries")
    print("Done")
    conn.commit()

conn1.close()
conn2.close()