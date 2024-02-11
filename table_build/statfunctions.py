import pandas as pd
import numpy as np
from sqlalchemy import MetaData, select, create_engine, func
from sqlalchemy.orm import sessionmaker
from math import sqrt
from dbpgstrings import host, database, user, password 
port='5432'


engine_loc = create_engine("sqlite:///23spells.db", echo=False) #Local db. Contains GameData table for getGameDataFrame.
#Use first line to read stats from online db. Switch to second to run all locally
"""engine=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database))  
engine=engine_loc
conn = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)"""
MINTURNS=5
MAXTURNS=15
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



def cardInfo(conn, set_abbr='ltr'):
    metadata=MetaData()
    metadata.reflect(bind=conn)
    card_table=metadata.tables[set_abbr+'CardInfo']
    s=select(card_table)
    df=pd.read_sql_query(s,conn,index_col='id')
    return df

def listOfColors(order='binary'):
    if order=='binary':
        return ['W','U','WU','B','WB','UB','WUB','R','WR','UR','WUR','BR','WBR','UBR','WUBR','G',
          'WG','UG','WUG','BG','WBG','UBG','WUBG','RG','WRG','URG','WURG','BRG','WBRG','UBRG','WUBRG']
    else: return ['W','U','B','R','G','WU','WB','WR','WG','UB','UR','UG','BR','BG','RG','WUB','WUR','WUG','WBR','WBG','WRG',
          'UBR','UBG','URG','BRG','WUBR','WUBG','WURG','WBRG','UBRG','WUBRG']
    

def colorString(color:int):
    #Color in card info is stored as a 5 bit int where each bit is the presence of a color. 
    #This function turns that int into the corresponding WUBRG string.
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
def colorInt(colorString):
    #Inverse of colorString on appropriate WUBRG strings. Requires letters be written in WUBRG order.
    #Returns 0 on any string that doesn't start with a WUBRG letter (including 'C')
    #Extracts color info from strings that start with colors (e.g. colorInt('UR2')=10=colorInt('UR'))
    color_int=0
    if colorString=="" or type(colorString)!=str:
        return 0
    if colorString.startswith('W'):
        color_int=1
        colorString=colorString[1:]
    if colorString.startswith('U'):
        color_int+=2
        colorString=colorString[1:]
    if colorString.startswith('B'):
        color_int+=4
        colorString=colorString[1:]
    if colorString.startswith('R'):
        color_int+=8
        colorString=colorString[1:]
    if colorString.startswith('G'):
        color_int+=16
    return color_int
    #If speed is an issue, could just make the 32 case match statement

def rankToNum(name:str):
    match name:
        case 'bronze':
            return 1
        case 'silver':
            return 2
        case 'gold':
            return 3
        case 'platinum':
            return 4
        case 'diamond':
            return 5
        case 'mythic':
            return 6
        case _:
            return 0
def getCardsWithMV(conn, mv, set_abbr="ltr"): #returns a list of all card names from a given set with a given mana value (mv)
    #all cards with 8+ mv get sorted into the same bucket
    metadata=MetaData()
    metadata.reflect(bind=conn)
    card_table=metadata.tables[set_abbr+'CardInfo']
    s=select(card_table.c.name,card_table.c.mana_value)
    carddf=pd.read_sql_query(s,conn)
    if mv!=MAXMV:
        cards=carddf[carddf['mana_value']==mv]['name'].to_list()
    else:
        cards=carddf[carddf['mana_value']>=MAXMV]['name'].to_list()
    return cards


def getCardsWithColor(conn, color, set_abbr='ltr',include_multicolor=True, include_lands=False, as_string=False):
    #Returns list of all cards with matching the given color
    #If as_string=True gives their names, otherwise gives their index in cardInfo
    #Color is determined (in setinfo.py) by mana cost. Could be misleading on some cards like DFCs, adventures, alternate costs, etc.
    carddf=cardInfo(conn, set_abbr)
    colors=['W','U','R','B','G','C']
    if color in colors:
        c=colors.index(color)
    else:
        print("WARNING: Invalid color requested")
        return
    if c==5: #colorless case
       colorfilter=carddf['color']==0
       if not include_lands:
           landfilter=pd.Series(['L' not in card_type for card_type in carddf['card_type']])
           colorfilter=colorfilter * landfilter
    else:
        cnum=2**c
        if include_multicolor:
            colorfilter=(carddf['color']//cnum)%2==1
        else:
            colorfilter=carddf['color']==cnum
    if as_string:
        cards=(carddf.loc[colorfilter])['name'].tolist()
    else:
        cards=carddf.loc[colorfilter].index.to_list()
    return cards


def getGameDataFrame(main_colors, set_abbr='ltr'): 
    #returns the data gamedata rows of all games fitting the given criteria as a dataframe
    #trade off of using too much memory vs reading the raw data too many times which is slow
    conn = engine_loc.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine_loc)
    game_data_table=metadata.tables[set_abbr+'GameData']
    q=select(game_data_table).where(game_data_table.c.main_colors==main_colors)
    df=pd.read_sql_query(q,conn)
    conn.close()
    return df



def countCurve(gamesdf,carddf):
    #given a dataframe of games, returns total number of cards of each MV in those games
    #carddf should be the output of cardInfo for the relevant set
    #returns in the form [0 drops, ..., 8+ drops, lands]
    curve=[0]*10
    for m in range(-1,8):
        cards=carddf[carddf['mana_value']==m]['name'].to_list()
        mv=m%10 
        for card in cards:
            c='deck_'+card
            curve[mv]+=int(gamesdf[c].sum())
    cards=carddf[carddf['mana_value']>=8]['name'].to_list()
    for card in cards:
            c='deck_'+card
            curve[8]+=int(gamesdf[c].sum())
    return curve


def countDecklistColors(conn, decks,set_abbr='ltr'):
    #given an array/dataframe where columns are card counts in set id order, returns number of cards of each color in each row
    #counts multicolor cards as 1 of each color
    colordf=pd.DataFrame({'W':[],'U':[],'B':[],'R':[],'G':[],'C':[]})
    for color in colordf.columns:
        cards=getCardsWithColor(conn, color, set_abbr=set_abbr)
        colordf[color]=decks.iloc[:,cards].sum(axis=1)
    return colordf



    

def getArchAvgCurve(conn, archLabel, set_abbr='ltr'): #Not in use currently
    #returns mean values of lands and each n drop for given archetype
    metadata=MetaData()
    metadata.reflect(bind=conn)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    q=select(archStats_table).where(archStats_table.c.archLabel==archLabel)
    df=pd.read_sql_query(q,conn)
    dfTotal=df.iloc[0:,3:].sum() 
    if dfTotal['game_count']!=0:
        n=dfTotal['game_count']
        avgs=dfTotal.iloc[1:]/n
        return avgs
    else:
        return dfTotal.iloc[1:] #should be all 0s as this is the 'no games meet these conditions' case

def getArchWinRate(conn, archLabel,set_abbr='ltr'): #Will just get baked into Archetype table
    metadata=MetaData()
    metadata.reflect(bind=conn)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q1=select(func.sum(archStats_table.c.game_count).label('games')).join(arch_table,archStats_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==archLabel)                                                                                 
    games_played=pd.read_sql_query(q1,conn).at[0,'games']
    q1=q1.where(archStats_table.c.won==True)
    wins=pd.read_sql_query(q1,conn).at[0,'games']
    if games_played==0: return 0
    else: return wins/games_played

def getCardInDeckWinRates(conn,archLabel='ALL', minCopies=1, maxCopies=40,set_abbr='ltr'): #Not in use
    metadata=MetaData()
    metadata.reflect(bind=conn)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q=select(cg_table.c.id,func.sum(cg_table.c.win_count).label("wins"),func.sum(cg_table.c.game_count).label("games_played")).join(arch_table, cg_table.c.arch_id==arch_table.c.id).where(
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies).group_by(cg_table.c.id)
    if archLabel!='ALL':
        q=q.where(arch_table.c.name==archLabel)
    df=pd.read_sql_query(q,conn)
    tempgames=df['games_played'].mask(df['games_played']==0,1) #Used so that 0wins/0games->0%
    df['win_rate']=df['wins']/tempgames
    return df


def winRateFromCounts(df): #returns win rate of data frame with a game count and a win/loss column, i.e. subset of CGStats or ArcStats
    games=df['game_count'].sum()
    wins=df[[df['won']==True]]['game_count'].sum()
    if games==0: return 0
    else: return wins/games


def meanGameLength(conn,archLabel, minRank=0, maxRank=6, won=-1, set_abbr='ltr'):  #Not in use
    metadata=MetaData()
    metadata.reflect(bind=conn)
    ag_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    #use won=0 to only count losses, won=1 to only count wins, archLabel='any' to include all archetypes
    q1=select(func.sum(ag_table.c.game_count).label('games'),
        func.sum(ag_table.c.g * ag_table.c.turns).label('turns')).join(
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

def recordByLengthDB(conn,archLabel, set_abbr='ltr'):
    #given archetype and range of ranks, returns df with wins and total games at each game length
    #game lengths <=5 turns and >=14 turns are grouped together 
    metadata=MetaData()
    metadata.reflect(bind=conn)
    ag_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q=select(ag_table.c.turns,ag_table.c.won,ag_table.c.game_count).join(arch_table, ag_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==archLabel)
    df=pd.read_sql_query(q,conn)
    counts=df[['turns','game_count']].groupby('turns').sum()
    winsdf=df[df['won']==1]
    win_counts=winsdf[['turns','game_count']].groupby('turns').sum()
    total=df['game_count'].sum()
    if total==0:
        #print("Insufficient data")
        return pd.Series([0]*(MAXTURNS-MINTURNS+1), index=range(MINTURNS,MAXTURNS+1))        
    counts.loc[MINTURNS]=counts.loc[:MINTURNS].sum()
    counts.loc[MAXTURNS]=counts.loc[MAXTURNS:].sum()
    counts=counts.loc[MINTURNS:MAXTURNS]
    win_counts.loc[MINTURNS]=win_counts.loc[:MINTURNS].sum()
    win_counts.loc[MAXTURNS]=win_counts.loc[MAXTURNS:].sum()
    win_counts=win_counts.loc[MINTURNS:MAXTURNS]
    recorddf=pd.concat([win_counts,counts],axis=1)
    recorddf.columns=['wins','games']
    tempgames=recorddf['games'].mask(recorddf['games']==0,1)
    recorddf['win_rate']=recorddf['wins']/tempgames
    return recorddf

def winRatesByTurnDF(df):
    games=df['num_turns'].value_counts().sort_index()
    games.loc[MINTURNS]=games.loc[:MINTURNS].sum()
    games.loc[MAXTURNS]=games.loc[MAXTURNS:].sum()
    games=games.loc[MINTURNS:MAXTURNS]
    wins=df[df['won']==True]['num_turns'].value_counts().sort_index()
    wins.loc[MINTURNS]=wins.loc[:MINTURNS].sum()
    wins.loc[MAXTURNS]=wins.loc[MAXTURNS:].sum()
    wins=wins.loc[MINTURNS:MAXTURNS]
    games=games.mask(games==0,1)
    return wins/games 

def medianPick(pickdf: pd.Series):
    #pickdf should be a series where the index is the pick number (0-13) 
    #and the values are the # of appearances of a given card at that pick number
    #Returns the pick number by which 50% of the copies of that card have been taken
    #e.g. medpick=5 means that after 4 picks less than half have been taken, but after 5 picks more than half hae been taken
    half=pickdf[0]/2
    postmedian=pickdf[pickdf<half]
    medpick=postmedian.index.min()
    return medpick
    
def meanPick(pickdf: pd.Series):
   return pickdf.sum()/pickdf[0]

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
    zeros=pd.Series([0]*60, index=range(0,60))
    records=df[['num_turns','won']].value_counts().sort_index()
    wins=(zeros+records[:,1]).replace({np.nan:0}).astype('int')
    losses=(zeros+records[:,0]).replace({np.nan:0}).astype('int')
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
