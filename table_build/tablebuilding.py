import pandas as pd
from sqlalchemy import MetaData, ForeignKey, Integer, SmallInteger, String, Boolean, DateTime, Float, func
from sqlalchemy import Column, Table, select, create_engine, delete, update,insert
from sqlalchemy.orm import mapped_column, DeclarativeBase
from statfunctions import *
from processdraftdata import *
from setinfo import scrape_scryfall
import os
from dotenv import load_dotenv
from clustermaking import *
load_dotenv()
db_url=os.getenv("DB_URL")
set_abbr='one' #This determines which set we are working with. Current options: ltr, dmu, bro, mkm, one
port='5432'
engine=create_engine(url=db_url) 
conn=engine.connect()
#metadata = MetaData()

# Table Definitions
class Base(DeclarativeBase):
    pass


class Archetypes(Base):
    __tablename__=set_abbr+"Archetypes"
    id=mapped_column(SmallInteger, primary_key=True)
    arch_label=mapped_column(String)
    num_drafts=mapped_column(Integer)
    num_wins=mapped_column(Integer)
    num_losses=mapped_column(Integer)
    #id: For id<32, id encodes main colors by W=1, U=2, B=4, R=8, G=16, e.g. 17=10001=WG.
    #For id>=32, id%32 encodes color as above and int(id/32) is subarchetype number, e.g. 81=32*2+17=WG2
    #arch_label=main colors for id<32, and main colors with a number appended for subarchetypes.
        #e.g. 'WG' refers to all decks that have white and green as their main colors. 
        #'WG1', 'WG2', and 'WG3' would be the three archetypes within those colors.
    #num_drafts=number of drafts recorded for this archetype
    #num_wins / losses = total games won/lost recorded for this archetype
    #id=-1 has arch_label='ALL' and has cumulative data for the entire set.
    
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


"""class Decklists(Base): #The exact structure of this table varies between sets so it is constructed separately
    __tablename__=set_abbr+'Decks'
    deck_id: Integer, primary key for this table
    draft_id: String, corresponds to DraftData to identify which draft yielded this deck
    draft_time: DateTime, when this deck was drafted
    rank: SmallInteger, rank of the drafter
    wins: SmallInteger, number of games this deck won
    games: SmallInteger, number of games this deck played
    main_colors: String, uses WUBRG notation
    arch_id: SmallInteger, 
    CARDNAME: SmallInteger, one of these for every card in given set, number of copies of CARDNAME in this deck
"""
class ArchGameStats(Base):
    #For each archetype and length of game (by turns), there is a row for wins and a row for losses
    #In these rows the recorded data is the number of games and the total number of cards of each mana value
    #played in decks in those games.
    #For example: in the row for arch_id=6 (UB), turns=8, won=True, 
    #n2_drops/game_count would correspond to the average number of 2 drops in UB decks that won a game in 8 turns.
    __tablename__=set_abbr+"ArchGameStats"
    arch_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'Archetypes.id'),primary_key=True) 
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
    #game_count: number of games that match the previous 3 variables (e.g. number of times a WR deck lost in 9 turns)
    #lands: total number of lands in deck in those games
    #nx_drops: total number of x drops in deck in those games (with n8p_drops meaning number of 8+ drops)

class CardGameStats(Base):
    #id: card id from CardInfo table
    #arch_id: archetype as an int from Archetypes table
    #copies: number of copies in deck
    #win count: number of wins for decks with given number of copies of this card
    #game count: number of games played for those decks
    #game played win rate=sum over copies>=1 of win_count/sum over copies>=1 of game_count

    __tablename__=set_abbr+"CardGameStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id',ondelete='CASCADE'), primary_key=True)
    arch_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'Archetypes.id',ondelete='CASCADE'), primary_key=True)
    #arch_id=mapped_column(SmallInteger, primary_key=True)
    copies=mapped_column(SmallInteger, primary_key=True)
    win_count=mapped_column(Integer)
    game_count=mapped_column(Integer)



class CardDerivedStats(Base):
    __tablename__=set_abbr+"CardDerivedStats"
    arch_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'Archetypes.id'), primary_key=True)
    #arch_id=mapped_column(SmallInteger, primary_key=True)
    card_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id',ondelete='CASCADE'), primary_key=True)
    games_in_hand=mapped_column(Integer,nullable=True)
    wins_in_hand=mapped_column(Integer,nullable=True)
    avg_win_shares=mapped_column(Float,nullable=True)
    adjusted_iwd=mapped_column(Float,nullable=True)
    inclusion_impact=mapped_column(Float,nullable=True)
    #For each pairing of card and archetype (with 'ALL' counting as an archetype), stores various card evaluation stats
    #card_id, arch_id: integers defining card and archetype
    #games_in_hand: Number of games in which that card was ever in hand for the given archetype
    #wins_in_hand: Number of those games that were wins. (wins_in_hand/games_in_hand=game in hand win rate)
    #avg_win_shares: average win shares per appearance. 
    #adjusted_iwd: not yet implemented. planned to be impact when drawn, rescaled to control for game length bias
    #inclusion_impact: not yet implemented. some version of difference between gpwr for decks running and not running this card 


class ArchStartStats(Base):
    __tablename__=set_abbr+"ArchStartStats"
    arch_id=mapped_column(SmallInteger, ForeignKey(set_abbr+'Archetypes.id'), primary_key=True)
    #arch_id=mapped_column(SmallInteger, primary_key=True)
    num_mulligans=mapped_column(SmallInteger, primary_key=True)
    on_play=mapped_column(Boolean, primary_key=True)
    win_count=mapped_column(Integer)
    game_count=mapped_column(Integer)
    #For each archetype, stores play/draw records partitioned by number of mulligans
    #arch_id: integer labelling archetype from Archetypes table
    #num_mulligans: how many mulligans the player took to start the game
    #on_play: Whether the player went first. True for going first, False for going second.
    #win_count: total number of games won matching the preceding variables
    #game_count: total number of games played matching those variables
    #e.g. a row with arch_id=3, num_mulligans=0, on_play=True, win_count=1000, game_count=1800 
    #would mean that WU decks that didn't mulligan and went first won 1000 of the 1800 games they played.

#Table Building
def createDecklists(): 
    tableName=set_abbr+'Decklists'
    Base.metadata.reflect(bind=conn)
    if tableName in Base.metadata.tables.keys():
        oldtable=Base.metadata.tables[tableName]
        oldtable.drop(bind=conn)
        print("Dropped previous decklist table")
        Base.metadata.clear()
        conn.commit()
        Base.metadata.reflect(bind=conn)
    carddf=cardInfo(conn=conn,set_abbr=set_abbr)
    cols=[Column('deck_id',Integer, primary_key=True),
          Column('draft_id',String, ForeignKey(set_abbr+'DraftInfo.draft_id', ondelete='CASCADE')),
          #Column('draft_id',String),
          Column('draft_time', DateTime),
          Column('rank', SmallInteger),
          Column('wins',SmallInteger),
          Column('games',SmallInteger),
          Column('main_colors',String),
          #Column('arch_id', SmallInteger)] #ForeignKey causes an error I can't explain and is a negligible optimization
          Column('arch_id', SmallInteger, ForeignKey(set_abbr+'Archetypes.id'))]
    for name in carddf['name'].tolist():
        cols.append(Column(name,SmallInteger))
    decktable=Table(set_abbr+'Decklists', Base.metadata, *cols)
    Base.metadata.create_all(bind=conn)
    conn.commit()
   


def makeDecklistSection(draftGameDF:pd.DataFrame,start_index:int,main_colors:str,arch_id:int):
    deck_count=draftGameDF.shape[0]
    extensionDF=pd.DataFrame(data={'deck_id':list(range(start_index,start_index+deck_count)),
                                   'main_colors':[main_colors]*deck_count,
                                   'arch_id':[arch_id]*deck_count},index=draftGameDF.index)
    sectionDF=pd.concat([draftGameDF,extensionDF],axis=1)
    starting_columns=['deck_id','draft_id','draft_time','rank','wins','games','main_colors','arch_id']
    other_columns=list(sectionDF.columns)
    for c in starting_columns: other_columns.remove(c)
    new_col_order=starting_columns+other_columns
    sectionDF=sectionDF[new_col_order]
    return sectionDF

def populateArchetypes():
    arcs=['C','W','U','WU','B','WB','UB','WUB','R','WR','UR','WUR','BR','WBR','UBR','WUBR','G',
          'WG','UG','WUG','BG','WBG','UBG','WUBG','RG','WRG','URG','WURG','BRG','WBRG','UBRG','WUBRG'] 
    df=pd.DataFrame({'id':[],'arch_label':[],'num_drafts':[],'num_wins':[],'num_losses':[]})
    for i in range(len(arcs)):
        df.loc[i]=(i, arcs[i], 0, 0, 0) 
    df.loc[df.shape[0]]=(-1, 'ALL', 0, 0, 0) 
    df.to_sql(set_abbr+'Archetypes',conn, index=False, if_exists='append')
    conn.commit()

def populateCardTable():
    df=pd.DataFrame.from_dict(scrape_scryfall(set_abbr=set_abbr),orient='index')
    df.columns=['name','mana_value','color','card_type','rarity']
    df['id']=df.index
    df.sort_index(inplace=True)
    df.to_sql(set_abbr+'CardInfo',conn, if_exists='append',index=False)
    conn.commit()
        

def insertColorToArchGames(colorDF:pd.DataFrame,cardDF:pd.DataFrame,color_id:int,ag_name:str):
    insertdf=pd.DataFrame({ 'arch_id': [],'won': [],'turns': [],'game_count': [],
                               'lands': [],'n0_drops': [], 'n1_drops': [],
                                'n2_drops': [], 'n3_drops': [], 'n4_drops': [],
                                'n5_drops': [], 'n6_drops': [], 'n7_drops': [],
                                'n8p_drops': []})
    for won in {True, False}:
        wondf=colorDF[colorDF["won"]==won]
        maxTurns=max(0,wondf.loc[:,'num_turns'].max())
        for turns in range(1,int(maxTurns)+1):
            turndf=wondf[wondf["num_turns"]==turns]
            curve=countCurve(turndf,cardDF)
            games=len(turndf.index)
            insertdf.loc[len(insertdf.index)]=[color_id, won, turns, games,
                                            curve[9], curve[0], curve[1],
                                            curve[2], curve[3], curve[4],
                                            curve[5], curve[6], curve[7],
                                            curve[8]]
    insertdf.to_sql(ag_name,conn,if_exists='append',index=False)

def insertColorToArchStarts(colorGamesDF:pd.DataFrame,color_id:int, table_name:str):
    recordDF=gameStartCounts(colorGamesDF)
    recordDF['arch_id']=pd.Series([color_id]*recordDF.shape[0])
    column_order=['arch_id','num_mulligans','on_play','win_count','game_count']
    recordDF=recordDF[column_order]
    recordDF.to_sql(table_name,con=conn,index=False,if_exists='append')
    return recordDF


def insertColorToCardTables(colorGamesDF:pd.DataFrame,cardDF:pd.DataFrame,arch_id:int,cg_table_name,derived_table_name):
    cgInsertDF=pd.DataFrame({'id':[],'arch_id':[],'copies':[],'win_count':[], 'game_count':[]})
    derivedInsertDF=pd.DataFrame({'arch_id':[],'card_id':[],'games_in_hand':[],'wins_in_hand':[], 'avg_win_shares':[],'adjusted_iwd':[],'inclusion_impact':[]})
    gamesInHandDF=gameInHandTotals(colorGamesDF)
    winShares,appearances=winSharesTotals(colorGamesDF)
    ws_per_appearance={}
    for card_id in cardDF.index:
        card_name=cardDF.at[card_id,'name']
        col='deck_'+card_name 
        partialdf=pd.DataFrame({'id':[],'arch_id':[],'copies':[],'win_count':[], 'game_count':[]})
        partialdf.set_index('copies')
        valdf=colorGamesDF[[col,'won']].value_counts() 
        indices=valdf.index.difference({(0,0),(0,1)})
        card_counts={i[0] for i in indices}
        for c in card_counts:
            partialdf.loc[c]=[card_id,arch_id,c,0,0]
        for (copies, won) in indices:
            partialdf.loc[copies,['win_count','game_count']]+=[valdf[copies,won]*won,valdf[copies,won]]
        cgInsertDF=pd.concat([cgInsertDF,partialdf],axis=0)
        if appearances[card_name]==0:
                ws_per_appearance[card_name]=0
        else:
            ws_per_appearance[card_name]=winShares[card_name]/appearances[card_name]
        derivedInsertDF.loc[len(derivedInsertDF.index)]=[arch_id,card_id,int(gamesInHandDF.loc[card_name,'games']),
                                                     int(gamesInHandDF.loc[card_name,'wins']),ws_per_appearance[card_name],0,0]
    cgInsertDF.to_sql(cg_table_name,conn,if_exists='append', index=False)
    derivedInsertDF.to_sql(derived_table_name,conn,if_exists='append',index=False)
    conn.commit()
    return (gamesInHandDF, winShares, appearances)
def populateAllColorData(): #Find and write all data that is derived from color partitioning GameData
    Base.metadata.reflect(bind=conn)
    arch_table=Base.metadata.tables[set_abbr+'Archetypes']
    ag_name=set_abbr+'ArchGameStats'
    arch_start_name=set_abbr+'ArchStartStats'
    totalArchStartsDF=pd.DataFrame({'arch_id':[-1]*8,'num_mulligans':[0,0,1,1,2,2,3,3],'on_play':[False,True]*4,'win_count':[0]*8,'game_count':[0]*8}) #Holds cumulative start data for archetype 'ALL'
    derived_table_name=set_abbr+'CardDerivedStats'
    #derived_table=Base.metadata.tables[derived_table_name]
    cg_name=set_abbr+'CardGameStats'
    cardDF=cardInfo(conn=conn,set_abbr=set_abbr)
    cardNameToID={cardDF.loc[idx,'name']:idx for idx in cardDF.index}
    num_decks=0 #used to count how many decks have been added to Decklists for indexing purposes
    archTableByColorDF=pd.DataFrame({'id':[],'arch_label':[],'num_drafts':[],'num_wins':[],'num_losses':[]})
    for color_id in range(32):
        colors=colorString(color_id)
        print("Getting all stats for",colors)
        colors=colorString(color_id)
        colorGamesDF=getGameDataFrame(main_colors=colors,set_abbr=set_abbr)
        colorDraftDF=organizeGameInfoByDraft(colorGamesDF,include_decklists=True)
        num_arch_drafts=colorDraftDF.shape[0]
        num_arch_wins=colorDraftDF['wins'].sum()
        num_arch_losses=colorGamesDF.shape[0]-num_arch_wins
        archTableByColorDF.loc[archTableByColorDF.shape[0]]=(color_id,colors,num_arch_drafts,num_arch_wins,num_arch_losses)
        insertColorToArchGames(colorGamesDF,cardDF,color_id,ag_name)
        startRecordDF=insertColorToArchStarts(colorGamesDF,color_id,arch_start_name)
        totalArchStartsDF[['win_count','game_count']]+=startRecordDF[['win_count','game_count']]
        print("Finished",colors,"deck stats")
        gamesInHandDF, winShares, appearances= insertColorToCardTables(colorGamesDF,cardDF,color_id,cg_name,derived_table_name)
        if color_id==0:
            handTotalsDF=gamesInHandDF.copy()
            cumulativeWinShares=winShares.copy()
            cumulativeAppearances=appearances.copy()
        else:
            handTotalsDF=handTotalsDF+gamesInHandDF
            cumulativeWinShares=cumulativeWinShares+winShares
            cumulativeAppearances=cumulativeAppearances+appearances
        print("Finished",colors,"card stats")
        colorGamesDF=assignClusterLabels(gamesDF=colorGamesDF)
        num_archetypes=colorGamesDF['label'].max()+1
        print("Categorized into ",num_archetypes, " archetypes")
        if num_archetypes>1:
            archTableUpdate=pd.DataFrame({'id':[],'arch_label':[],'num_drafts':[],'num_wins':[],'num_losses':[]})
            deckTableUpdate=pd.DataFrame({})
            archetypes={}
            for label_number in range(num_archetypes):
                archGamesDF=colorGamesDF[colorGamesDF['label']==label_number]
                archetypes[label_number]=archGamesDF
                arch_id=label_number*32+32+color_id
                archDraftDF=organizeGameInfoByDraft(archGamesDF)
                num_arch_drafts=archDraftDF.shape[0]
                num_arch_wins=archDraftDF['wins'].sum()
                num_arch_losses=archGamesDF.shape[0]-num_arch_wins
                arch_label=colors+str(label_number+1) #WU archetypes go in as 'WU1', 'WU2', etc.
                archTableUpdate.loc[archTableUpdate.shape[0]]=(arch_id,arch_label,num_arch_drafts,num_arch_wins,num_arch_losses)
                deckSection=makeDecklistSection(draftGameDF=archDraftDF,start_index=num_decks,main_colors=colors,arch_id=arch_id)
                deckTableUpdate=pd.concat([deckTableUpdate,deckSection],axis=0)
                num_decks+=archDraftDF.shape[0]
            archTableUpdate.to_sql(name=set_abbr+'Archetypes',con=conn,index=False,if_exists='append')
            deckTableUpdate.to_sql(name=set_abbr+'Decklists',con=conn,index=False,if_exists='append')
            conn.commit()
            for label_number in range(num_archetypes):
                archGamesDF=archetypes[label_number]
                arch_id=label_number*32+32+color_id
                insertColorToArchGames(colorDF=archGamesDF,cardDF=cardDF,color_id=arch_id,ag_name=ag_name)
                insertColorToArchStarts(archGamesDF,arch_id,arch_start_name)
                insertColorToCardTables(archGamesDF,cardDF,arch_id,cg_name,derived_table_name)      
            print("Finished archetype stats")
        else:
            deckTableUpdate=makeDecklistSection(draftGameDF=colorDraftDF,start_index=num_decks,main_colors=colors,arch_id=color_id) 
            num_decks+=colorDraftDF.shape[0]
            deckTableUpdate.to_sql(name=set_abbr+'Decklists',con=conn,index=False,if_exists='append')
            conn.commit()
    overall_ws_per_appearance={}
    for card_name in handTotalsDF.index: 
        if cumulativeAppearances[card_name]==0:
                overall_ws_per_appearance[card_name]=0
        else:
            overall_ws_per_appearance[card_name]=cumulativeWinShares[card_name]/cumulativeAppearances[card_name]
        derived_table=Base.metadata.tables[derived_table_name]
        u=update(derived_table).where(derived_table.c.card_id==cardNameToID[card_name],derived_table.c.arch_id==-1).values(
            games_in_hand=int(handTotalsDF.loc[card_name,'games']),wins_in_hand=int(handTotalsDF.loc[card_name,'wins']),
            avg_win_shares=overall_ws_per_appearance[card_name]
        )
        conn.execute(u)
    conn.commit()
    for i in range(32):
        drafts=archTableByColorDF.at[i,'num_drafts']
        wins=archTableByColorDF.at[i,'num_wins']
        losses=archTableByColorDF.at[i,'num_losses']
        u=update(arch_table).where(arch_table.c.id==int(i)).values(num_drafts=int(drafts),num_wins=int(wins),num_losses=int(losses))
        conn.execute(u)
    totalArchStartsDF.to_sql(arch_start_name,con=conn,index=False,if_exists='append')
    conn.commit()
def populateImpacts():
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
def tableCensus(prefix=''): #For testing purposes. Go through each table and sample the contents.
    md=MetaData()
    md.reflect(bind=conn)
    print(md.tables.keys())
    for table_name in md.tables.keys():
        if table_name.startswith(prefix):
            table=md.tables[table_name]
            print(table.columns)
            """table=md.tables[table_name]
            s1=select(table).limit(3)
            df=pd.read_sql_query(s1,conn)
            print("Table: ",table_name)
            print(df)
            s2=select(func.count(1)).select_from(table)
            print("Size:", conn.execute(s2).fetchall())"""
def tableCensus2(prefix=''): #For testing purposes. Go through each table and sample the contents.
    md=Base.metadata()
    md.reflect(bind=conn)
    print(md.tables.keys())
    for table_name in md.tables.keys():
        if table_name.startswith(prefix):
            table=md.tables[table_name]
            print(table.columns)
def dropSet(drop_draft=True,drop_cards=True):
    Base.metadata.clear()
    Base.metadata.reflect(bind=conn)
    table_order=['CardDerivedStats','CardGameStats','ArchStartStats','ArchGameStats','Decklists','Archetypes']
    if drop_draft:
        table_order.extend(['DraftInfo','DraftPacks'])
    if drop_cards:
        table_order.append('CardInfo')
    for name in table_order:
        table_name=set_abbr+name
        if table_name in Base.metadata.tables.keys():
            table=Base.metadata.tables[set_abbr+name]
            print("Dropping ",set_abbr+name)
            table.drop(bind=conn)
    Base.metadata.clear()
    conn.commit()
def clearSet():
    Base.metadata.clear()
    Base.metadata.reflect(bind=conn)
    table_order=['CardDerivedStats','CardGameStats','ArchStartStats','ArchGameStats','Decklists','Archetypes']
    for name in table_order:
        table_name=set_abbr+name
        if table_name in Base.metadata.tables.keys():
            print("Deleting contents of ",table_name)
            tbl=Base.metadata.tables[table_name]
            d=delete(tbl)
            conn.execute(d)
            conn.commit()
    Base.metadata.clear()
    conn.commit()

def buildDBSimul():
    Base.metadata.reflect(bind=conn) 
    Base.metadata.create_all(bind=conn)
    populateArchetypes()
    print("Built Archetype Table")
    populateCardTable()
    print("Built Card Info Table")
    makeDraftInfo(conn,set_abbr=set_abbr) ##<
    processPacks(conn,set_abbr=set_abbr)  ##<These both iterate through draft data and could be merged
    createDecklists()
    populateAllColorData()
    print("Done")
    conn.commit()
    conn.close()
def refreshGameData():
    #For sets that already exist, keep CardInfo and draftInfo. Replace all stats based on GameData.
    clearSet()
    Base.metadata.reflect(bind=conn) 
    Base.metadata.create_all(bind=conn)
    populateArchetypes()
    print("Built Archetype Table")
    createDecklists()
    populateAllColorData()
    print("Done")
    conn.commit()
    conn.close()

refreshGameData()
