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
        if key[:5]=='drawn' or key[:7]=='opening': #should tutored be here too?
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
def deckSizeInfo(conn,set_abbr):
    #Shows how many decks for a given set have 40, 41, etc. cards in them. Shows win rate for each deck size.
    #Data is imperfect because deck lists are being determined by game 1 build and not accounting for changes made later.
    #Not in use. I just made this because I was curious.
    metadata=MetaData()
    metadata.reflect(bind=conn)
    deck_table=metadata.tables[set_abbr+'Decklists']
    draft_table=metadata.tables[set_abbr+'DraftInfo']
    s=select(deck_table,draft_table.c.wins,draft_table.c.losses).join(draft_table,deck_table.c.draft_id==draft_table.c.draft_id)
    deckdf=pd.read_sql_query(s,conn)
    decksizes=deckdf.iloc[:,4:-2].sum(axis=1)
    populations=decksizes.value_counts()
    print(populations)
    pcts=populations/populations.sum()
    print(pcts)
    recordsdf=deckdf.iloc[:,-2:]
    recordsdf['size']=decksizes
    wins_and_losses=recordsdf.groupby('size').sum()
    wins_and_losses['win_rate']=wins_and_losses['wins']/(wins_and_losses['wins']+wins_and_losses['losses'])
    print(wins_and_losses)    
    
def winRate(df):
    #df should be a game dataframe
    if df.shape[0]==0: return 0
    else: return df[df['won']==True].shape[0]/df.shape[0]

def cardsInHand(gameDF: pd.DataFrame):
    #Given a game dataframe, return a dataframe containing the number of copies of each card that are ever in hand each game
    #The returned dataframe has each row representing a game, an each column is a card.
    hand_info={}
    for key in gameDF.keys():
         if key[:5]=='drawn': 
            card_name=key[6:]
            hand_info[card_name]=gameDF[key]+gameDF['opening_hand_'+card_name] #+gameDF['tutored_'+card_name] ltrGameData doesn't have tutored currently
    handDF=pd.DataFrame(hand_info)
    return handDF


def winSharesTotals(gameDF: pd.DataFrame):
    #Idea: each card present in a win gets an equal portion of credit for the win (1/total cards seen). Same for losses but negative.
    #Average this over multiple games by dividing each card's win shares by the number of times it appears.
    #This should be pretty strongly correlated to GIHWR, without the long game bias.
    #If every game had exactly N total cards drawn this AvgWS=1/N(2*GIH-1).
    #The trade off is that this has a bias against card draw spells
    #as the games where they are cast have more cards seen, and thus lower weight per card, than the ones where they aren't
    #Given a section of GameData, return total win shares and number of appearances for each card
    win_loss=np.array(2*gameDF[['won']]-1)
    handDF=cardsInHand(gameDF)
    total_cards_seen=handDF.sum(axis=1) #Total number of cards ever in hand for each game
    total_cards_seen=np.array(total_cards_seen.mask(total_cards_seen==0,1)).reshape(win_loss.shape) #Replacing 0->1 to avoid divide by 0
    game_weights=np.divide(win_loss,total_cards_seen)
    hand_totals=handDF.sum(axis=0)
    hand_matrix=handDF.values
    ws_matrix=np.matmul(hand_matrix.T,game_weights)
    ws_totals=pd.Series(data=np.reshape(ws_matrix,-1),index=handDF.keys())
    return ws_totals, hand_totals
def winSharesByColors(main_colors, set_abbr='ltr'):
    #Find win share stats for a specific set of main colors
    gameDF=getGameDataFrame(main_colors=main_colors,set_abbr=set_abbr)
    ws_totals, hand_totals=winSharesTotals(gameDF)
    ws_per_appearance={}
    for card_name in hand_totals.keys():
        if hand_totals[card_name]==0:
            ws_per_appearance[card_name]=0
        else:
            ws_per_appearance[card_name]=ws_totals[card_name]/hand_totals[card_name]
    ws_per_appearance=pd.Series(ws_per_appearance)
    ws_per_appearance.sort_values(inplace=True)
    for key in ws_per_appearance.keys():
        significant=hand_totals[key]>100
        if significant: print(key,':',ws_per_appearance[key])
def winSharesOverall(set_abbr='ltr',chunk_size=50000):
    #Find win share stats for all games played.
    conn = engine_loc.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine_loc)
    game_data_table=metadata.tables[set_abbr+'GameData']
    q_size=select(func.count(1)).select_from(game_data_table)
    num_games_res=pd.read_sql_query(q_size,conn)
    num_games=num_games_res.iloc[0,0]
    q_first_games=select(game_data_table).where(game_data_table.c.index<chunk_size)
    gameDF1=pd.read_sql_query(q_first_games,conn)
    ws_totals, hand_totals=winSharesTotals(gameDF1)
    for i in range(1,num_games//chunk_size+1):
        start_index=i*chunk_size
        q_games=select(game_data_table).where(game_data_table.c.index>=start_index,game_data_table.c.index<start_index+chunk_size)
        gameDF=pd.read_sql_query(q_games,conn)
        ws_totals_temp, hand_totals_temp=winSharesTotals(gameDF)
        ws_totals=ws_totals+ws_totals_temp
        hand_totals=hand_totals+hand_totals_temp
        print("Counted win shares from", (i+1)*chunk_size, "games")
    ws_per_appearance={}
    for card_name in hand_totals.keys():
        if hand_totals[card_name]==0:
            ws_per_appearance[card_name]=0
        else:
            ws_per_appearance[card_name]=ws_totals[card_name]/hand_totals[card_name]
    ws_per_appearance=pd.Series(ws_per_appearance)
    ws_per_appearance.sort_values(inplace=True,ascending=False)
    for key in ws_per_appearance.keys():
        significant=hand_totals[key]>100
        if significant: print(key,':',ws_per_appearance[key])

def gameInHandTotals(gameDF:pd.DataFrame):
    #gameDF should be a game dataframe
    #returns total number of games in which each card shows up and how many of those are wins
    handDF=cardsInHand(gameDF)
    card_names=handDF.keys()
    boolHandDF=handDF.gt(0)
    boolHandDF['won']=gameDF['won']
    games=(boolHandDF.iloc[:,:-1]).sum(axis=0) #number of games in hand for each card
    boolHandDF=boolHandDF[boolHandDF['won']==1] #filtering to only look at wins
    wins=(boolHandDF.iloc[:,:-1]).sum(axis=0) #number of wins where each card appeared
    totals=pd.DataFrame({'games':games.to_list(),'wins':wins.to_list()},index=card_names)
    return totals

def gameInHandOverall(set_abbr='ltr', chunk_size=100000):
    #For each card, gets total number of games and number of wins where that card is ever in hand. 
    #Returns a dataframe indexed by card name with columns 'games' and 'wins'. 
    #'games'/'wins' should match 17lands 'GIHWR' stat.
    #This is unnecessary if I'm going to find all colors separately anyway. May as well just add those together.
    conn = engine_loc.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine_loc)
    game_data_table=metadata.tables[set_abbr+'GameData']
    q_size=select(func.count(1)).select_from(game_data_table)
    num_games_res=pd.read_sql_query(q_size,conn)
    num_games=num_games_res.iloc[0,0]
    q_first_games=select(game_data_table).where(game_data_table.c.index<chunk_size)
    gameDF1=pd.read_sql_query(q_first_games,conn)
    totals=gameInHandTotals(gameDF1)
    for i in range(1,num_games//chunk_size+1):
        start_index=i*chunk_size
        q_games=select(game_data_table).where(game_data_table.c.index>=start_index,game_data_table.c.index<start_index+chunk_size)
        gameDF=pd.read_sql_query(q_games,conn)
        temp_totals=gameInHandTotals(gameDF)
        totals=totals+temp_totals
        print("Counted win shares from", (i+1)*chunk_size, "games")
    return totals
def gameInHandByColors(main_colors, set_abbr='ltr')->pd.DataFrame:
    gameDF=getGameDataFrame(main_colors=main_colors,set_abbr=set_abbr)
    totals=gameInHandTotals(gameDF)
    return totals

def gameStartCounts(gameDF: pd.DataFrame):
    #gameDF should be a game dataframe containing the num_mulligans, on_play, and won columns from GameData
    #Returns a dataframe of records for each pairing of num_mulligans and on_play values
    #Games with 3 or more mulligans are grouped together.
    counts=gameDF[['num_mulligans','on_play','won']].value_counts()
    recordDF=pd.DataFrame({'num_mulligans':[],'on_play':[],'win_count':[],'game_count':[]})
    for m in range(3):
        for p in range(2):
            wins=0
            games=0
            if (m,p,0) in counts.index:
                games+=counts[m,p,0]
            if (m,p,1) in counts.index:
                wins+=counts[m,p,1]
                games+=counts[m,p,1]
            recordDF.loc[recordDF.shape[0]]=[m,bool(p),wins,games]
    for p in range(2):
        wins3=0
        games3=0
        for m in range(3,8):
                if (m,p,0) in counts.index:
                    games3+=counts[m,p,0]
                if (m,p,1) in counts.index:
                    wins3+=counts[m,p,1]
                    games3+=counts[m,p,1]
        recordDF.loc[recordDF.shape[0]]=[3,bool(p),wins3,games3]
    return recordDF
