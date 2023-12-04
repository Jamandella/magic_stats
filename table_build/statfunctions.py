import pandas as pd
import numpy as np
from sqlalchemy import MetaData, select, create_engine, func
from sqlalchemy.orm import sessionmaker
from math import sqrt

engine = create_engine("sqlite:///23spells.db", echo=False) #this will need to be something else for the web version
conn = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)
MINTURNS=5
MAXTURNS=14
MAXMV=8

def cardsSeenInDF(gamesDF: pd.DataFrame): #not currently in use
    #given a dataframe of rows from game_data, returns list of number of games with each total number of cards drawn
    cols=[]
    for key in gamesDF.keys():
        if key[:5]=='drawn' or key[:7]=='opening':
            cols.append(key)
    drawnDF=gamesDF.loc[:,cols]
    totals=drawnDF.sum(axis=1)
    distributionDF=totals.value_counts()
    return distributionDF

def cardsSeenPercentiles(distributionDF): #not yet in use, may still be useful later
    #used to determine reasonable bucket ranges for total cards seen
    #decide buckets based on large data sets (i.e. whole archetype). fine to compute aIWD on somewhat smaller data sets (i.e. rank within arch)
    minBucket=.12
    total=0
    counts=[]
    total_cards=0
    square_sum=0
    for key in distributionDF.keys():
        counts.append(int(key))
        total+=int(distributionDF[key])
        total_cards+=int(distributionDF[key]*key)
        square_sum+=int(distributionDF[key]*key*key)
    counts.sort()
    sum=0
    buckets=[-1] #bucket bounds
    bucket_levels=[] #proportion of games in each bucket, parallel lists should just be a dict for return purposes
    bucket_level=0
    last_bucket=False
    for count in counts:
        sum+=distributionDF[count]
        print("{} cards: {}".format(count,round(sum*100/total, 3)))
        bucket_level+=distributionDF[count]/total
        if bucket_level>minBucket and not last_bucket:
            buckets.append(int(count))
            bucket_levels.append(bucket_level)
            bucket_level=0
            if sum/total>=1-minBucket:
                last_bucket=True
                buckets.append(int(max(counts)))
                bucket_levels.append(1-sum/total)
    #print("Buckets:")
    #for n in range(0,len(buckets)-1):
        #print("{}-{}: {}".format(buckets[n]+1,buckets[n+1], bucket_levels[n]))
    mean =total_cards/total
    print("Sample size: {}".format(total))
    print("{} average cards drawn".format(mean))
    var=square_sum/total-mean*mean
    stdev=sqrt(var)
    print("Standard deviation: {}".format(stdev))



def cardInfo(set_abbr='ltr'):
    card_table=metadata.tables[set_abbr+'CardInfo']
    s=select(card_table)
    df=pd.read_sql_query(s,conn)
    return df

def colorString(color:int):
    if color==0:
        return 'C'
    else:
        s=""
        if color%2==1: s+='W'
        if (color//2)%2==1: s+='U'
        if (color//4)%2==1: s+='B'    
        if (color//8)%2==1: s+='R'
        if (color//16)%2==1: s+='G'
        return s
def getCardsWithMV(mv, set_abbr="ltr"): #returns a list of all card names from a given set with a given mana value (mv)
    #all cards with 8+ mv get sorted into the same bucket
    card_table=metadata.tables[set_abbr+'CardInfo']
    s=select(card_table.c.name,card_table.c.mana_value)
    carddf=pd.read_sql_query(s,conn)
    if mv!=MAXMV:
        cards=carddf[carddf['mana_value']==mv]['name'].to_list()
    else:
        cards=carddf[carddf['mana_value']>=MAXMV]['name'].to_list()
    return cards



def getGameDataFrame(archLabel, minRank=0, maxRank=6, set_abbr='ltr'): 
    #returns the data gamedata rows of all games fitting the given criteria as a dataframe
    #trade off of using too much memory vs reading the raw data too many times which is slow
    game_data_table=metadata.tables[set_abbr+'GameData']
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic']
    ranks=ranks[minRank:maxRank+1]
    q=select(game_data_table).where(game_data_table.c.main_colors==archLabel,
                                        game_data_table.c.rank.in_(ranks))
    df=pd.read_sql_query(q,conn)

    return df



def countCurve(gamesDF,set_abbr='ltr'):
    #given a dataframe of games, returns total number of cards of each MV in those games
    #returns in the form [0 drops, ..., 8+ drops, lands]
    curve=[0]*10
    for m in range(-1,MAXMV+1):
        cards=getCardsWithMV(m,set_abbr=set_abbr)
        mv=m%10
        for card in cards:
            c='deck_'+card
            curve[mv]+=int(gamesDF[c].sum())
    return curve

def getArchAvgCurve(archLabel, minRank=0, maxRank=6, set_abbr='ltr'): #Not in use currently
    #returns mean values of lands and each n drop for given archetype
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    q=select(archStats_table).join(arid_table, archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel, 
                                         arid_table.c.rank>=minRank, arid_table.c.rank<=maxRank)
    df=pd.read_sql_query(q,conn)
    dfTotal=df.iloc[0:,3:].sum() 
    if dfTotal['game_count']!=0:
        n=dfTotal['game_count']
        avgs=dfTotal.iloc[1:]/n
        return avgs
    else:
        return dfTotal.iloc[1:] #should be all 0s as this is the 'no games meet these conditions' case

def getArchWinRate(archLabel, minRank=0, maxRank=6, set_abbr='ltr'):
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    q1=select(func.sum(archStats_table.c.game_count).label('games')).join(arid_table,archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel,
                                                                                    arid_table.c.rank>=minRank,
                                                                                    arid_table.c.rank<=maxRank)                                                                                 
    games_played=pd.read_sql_query(q1,conn).at[0,'games']
    q1=q1.where(archStats_table.c.won==True)
    wins=pd.read_sql_query(q1,conn).at[0,'games']
    if games_played==0: return 0
    else: return wins/games_played

def getCardInDeckWinRate(cardID, archLabel='ALL', minCopies=1, maxCopies=40, minRank=0, maxRank=6, set_abbr='ltr'): #Not in use
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    q1=select(func.sum(cg_table.c.game_count).label("wins")).join(arid_table, cg_table.c.arid==arid_table.c.id).where(
                                                                            cg_table.c.id==cardID, cg_table.c.won==True, 
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies,
                                                                            arid_table.c.rank>=minRank,arid_table.c.rank<=maxRank)
    q2=select(func.sum(cg_table.c.game_count).label("games")).join(arid_table, cg_table.c.arid==arid_table.c.id).where(
                                                                            cg_table.c.id==cardID, arid_table.c.rank>=minRank,
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies,
                                                                            arid_table.c.rank<=maxRank)
    if archLabel!='ALL':
        q1=q1.where(arid_table.c.name==archLabel)
        q2=q2.where(arid_table.c.name==archLabel)

    wins=pd.read_sql_query(q1,conn).at[0,'wins']
    games=pd.read_sql_query(q2,conn).at[0,'games']
    if games==0: return 0
    else: return wins/games
def winRateFromCounts(df): #returns win rate of data frame with a game count and a win/loss column, i.e. subset of CGStats or ArcStats
    games=df['game_count'].sum()
    wins=df[[df['won']==True]]['game_count'].sum()
    if games==0: return 0
    else: return wins/games


def meanGameLength(archLabel, minRank=0, maxRank=6, won=-1, set_abbr='ltr'):  #Not in use
    ag_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    #use won=0 to only count losses, won=1 to only count wins, archLabel='any' to include all archetypes
    q1=select(func.sum(ag_table.c.game_count).label('games'),
        func.sum(ag_table.c.game_count * ag_table.c.turns).label('turns')).join(
                                        arid_table, ag_table.c.arid==arid_table.c.id).where(
                                        arid_table.c.rank>=minRank,
                                        arid_table.c.rank<=maxRank)
    if won==0: q1=q1.where(ag_table.c.won==False)
    elif won==1: q1=q1.where(ag_table.c.won==True)
    if archLabel!='ALL': q1=q1.where(arid_table.c.name==archLabel)
    df=pd.read_sql_query(q1,conn)
    total_games=df.at[0,'games']
    total_turns=df.at[0,'turns']
    if total_games==0: return 0
    else: return total_turns/total_games

def gameLengthDistDB(archLabel, minRank=0, maxRank=6,set_abbr='ltr'): #not in use
    #given archetype and range of ranks, returns series with game lengths as indices and proportion of games of that length as values
    #game lengths <=5 turns and >=14 turns are grouped together 
    ag_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    q=select(ag_table.c.turns,ag_table.c.game_count).join(arid_table, ag_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel, 
                                         arid_table.c.rank>=minRank, arid_table.c.rank<=maxRank)
    df=pd.read_sql_query(q,conn)
    counts=df[['turns','game_count']].groupby('turns').sum()
    total=df['game_count'].sum()
    if total==0:
        print("Insufficient data")
        return pd.Series([0]*(MAXTURNS-MINTURNS+1), index=range(MINTURNS,MAXTURNS+1))        
    counts.loc[MINTURNS]=counts.loc[:MINTURNS].sum()
    counts.loc[MAXTURNS]=counts.loc[MAXTURNS:].sum()
    counts=counts.loc[MINTURNS:MAXTURNS]/total
    return counts

def winRatesByTurnDF(df):
    games=df['num_turns'].value_counts().sort_index()
    games.loc[MINTURNS]=games.loc[:MINTURNS].sum()
    games.loc[MAXTURNS]=games.loc[MAXTURNS:].sum()
    games=games.loc[MINTURNS:MAXTURNS]
    wins=df[df['won']==True]['num_turns'].value_counts().sort_index()
    wins.loc[MINTURNS]=wins.loc[:MINTURNS].sum()
    wins.loc[MAXTURNS]=wins.loc[MAXTURNS:].sum()
    wins=wins.loc[MINTURNS:MAXTURNS]
    #if there are no games in one of the turn 5 to 14 buckets, then there isn't enough data
    if 0 in games.values: 
        print("Insufficient data")
        return pd.Series([0]*(MAXTURNS-MINTURNS+1), index=range(MINTURNS,MAXTURNS+1))
    else: return wins/games 

def gameLengthDistDF(df):
    #given a game dataframe, returns series with game lengths as indices and proportion of games of that length as values
    #game lengths <=5 turns and >=14 turns are grouped together 
    total=df.shape[0]
    lens=df['num_turns'].value_counts().sort_index()
    lens.loc[MINTURNS]=lens.loc[:MINTURNS].sum()
    lens.loc[MAXTURNS]=lens.loc[MAXTURNS:].sum()
    lens=lens.loc[MINTURNS:MAXTURNS]
    if total==0: 
        print("Insufficient data")
        return pd.Series([0]*(MAXTURNS-MINTURNS+1), index=range(MINTURNS,MAXTURNS+1))
    else: return lens/total
    #could do this faster extracting from archstats for archetype game lengths


def getRecordByLength(df):
    df=pd.DataFrame(df)
    gameCount=df.shape[0]
    zeros=pd.Series([0]*60, index=range(0,60))
    records=df[['num_turns','won']].value_counts().sort_index()
    games2=records.values.sum()
    wins=(zeros+records[:,1]).replace({np.nan:0}).astype('int')
    winCount=wins.sum()
    losses=(zeros+records[:,0]).replace({np.nan:0}).astype('int')
    lossCount=losses.sum()
    wins.loc[MINTURNS]=wins.loc[:MINTURNS].sum()
    wins.loc[MAXTURNS]=wins.loc[MAXTURNS:].sum()
    wins=wins.loc[MINTURNS:MAXTURNS]
    losses.loc[MINTURNS]=losses.loc[:MINTURNS].sum()
    losses.loc[MAXTURNS]=losses.loc[MAXTURNS:].sum()
    losses=losses.loc[MINTURNS:MAXTURNS]
    record=pd.DataFrame({'wins':wins, 'losses':losses})
    return record

def getCardsWithEnoughGames(df, min_sample, prefix="deck_"):
    #df should be a game dataframe. 
    #returns list of names of all cards such that there are at least min_sample games played with that card in deck in df
    cards=[]
    for col in df.columns:
        if col.startswith(prefix):
            if df[df[col]>0].shape[0]>min_sample:
                cards.append(col[len(prefix):])
    return cards
    
    
def winRate(df):
    #df should be a game dataframe
    if df.shape[0]==0: return 0
    else: return df[df['won']==True].shape[0]/df.shape[0]




          