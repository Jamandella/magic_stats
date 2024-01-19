import pandas as pd
from sqlalchemy import MetaData, select, create_engine, func
from dbpgstrings import host, database, user, password 
engine=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
    user, password, host, 5432, database))  


def cardInfo(set_abbr='ltr',as_json=False):
    
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    #Returns the full card info table for the given set. Defaults to returning a pandas dataframe, with an option for json instead.
    table_name=set_abbr+'CardInfo'
    s="SELECT * FROM {}".format(table_name)
    df=pd.read_sql_query(s,conn,index_col='id')
    conn.close()
    if as_json:return df.to_json()
    else: return df

def getCardsWithColor(color, set_abbr='ltr',include_multicolor=True, include_lands=False, as_string=True):
    #Returns list of all cards matching the given color
    #If as_string=True gives their names, otherwise gives their integer index in cardInfo
    #Color is determined (in setinfo.py) by mana cost. Could be misleading on some cards like DFCs, adventures, alternate costs, etc.
    #If include_multicolor, get all cards containing that color. Otherwise get cards that are exactly that color.
    #Lands are all marked as colorless. If color='C' and include_lands=True, lands will be included with the colorless cards.
    carddf=cardInfo(set_abbr)
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

def getArchAvgCurve(archLabel, minRank=0, maxRank=6, set_abbr='ltr'):
    #returns mean values of lands and each n drop for given archetype
    #Currently, archLabel is just colors in string form, e.g. 'G', or 'UB'. All colors are listed in WUBRG order.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    #Query all rows of the ArchGameStats table corresponding to given deck and ranks
    q=select(archStats_table).join(arid_table, archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel, 
                                         arid_table.c.rank>=minRank, arid_table.c.rank<=maxRank)
    df=pd.read_sql_query(q,conn) 
    #Remove first 3 columns from that table, leaving only game count and number of cards per mana value. Add up all the columns.
    dfTotal=df.iloc[0:,3:].sum() 
    conn.close()
    if dfTotal['game_count']!=0:
        n=dfTotal['game_count']
        avgs=dfTotal.iloc[1:]/n
        return avgs.to_json()
    else: #Should be all 0's in this case
        return dfTotal.iloc[1:].to_json()
def getArchWinRate(main_colors, minRank=0, maxRank=6, set_abbr='ltr'):
    #returns a a given deck's win rate.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    q1=select(func.sum(archStats_table.c.game_count).label('games')).join(arid_table,archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==main_colors,
                                                                                    arid_table.c.rank>=minRank,
                                                                                    arid_table.c.rank<=maxRank)                                                                                 
    games_played=pd.read_sql_query(q1,conn).at[0,'games']
    q1=q1.where(archStats_table.c.won==True)
    wins=pd.read_sql_query(q1,conn).at[0,'games']
    conn.close()
    if games_played==0: return 0
    else: return wins/games_played
def getCardInDeckWinRates(archLabel='ALL', minCopies=1, maxCopies=40, minRank=0, maxRank=6, set_abbr='ltr', as_json=True): 
#Returns game played win rates for all cards, indexed by their numerical id from CardInfo table. Can be restricted to specific decks and ranks.
#Can also require a specific range of copies of each card.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    arid_table=metadata.tables[set_abbr +'ArchRank']
    q=select(cg_table.c.id,func.sum(cg_table.c.win_count).label("wins"),func.sum(cg_table.c.game_count).label("games_played")).join(arid_table, cg_table.c.arid==arid_table.c.id).where(
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies,
                                                                            arid_table.c.rank>=minRank,arid_table.c.rank<=maxRank).group_by(cg_table.c.id)
    if archLabel!='ALL':
        q=q.where(arid_table.c.name==archLabel)
    df=pd.read_sql_query(q,conn)
    conn.close()
    tempgames=df['games_played'].mask(df['games_played']==0,1) #Used so that 0wins/0games->0%
    df['win_rate']=df['wins']/tempgames
    if as_json: return df.to_json()
    else: return df

def getRecordByLength(archLabel:str,set_abbr='ltr', minRank=0,maxRank=6):
    #Returns number of wins and losses at each game length for given deck
    #Shows how long the games tend to be and how the deck does as the game gets longer
    #"'(8,False)':9303" means that the deck lost 9303 games that lasted exactly 8 turns
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    q1=select(archStats_table.c.turns,archStats_table.c.won,func.sum(archStats_table.c.game_count).label('games')).group_by(archStats_table.c.turns,archStats_table.c.won).join(arid_table,archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel,
                                                                                    arid_table.c.rank>=minRank,
                                                                                    arid_table.c.rank<=maxRank).order_by(archStats_table.c.turns,archStats_table.c.won)   
    df=pd.read_sql_query(q1,conn) 
    df.set_index(['turns','won'],inplace=True)
    conn.close()
    return df.to_json()

def getMetaDistribution(set_abbr='ltr', minRank=0,maxRank=6):
    #Gets number of drafts for each set of main colors
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    draft_table=metadata.tables[set_abbr+"DraftInfo"]
    deck_table=metadata.tables[set_abbr+"Decklists"]
    s=select(deck_table.c.main_colors,func.count(1)).group_by(deck_table.c.main_colors)
    if minRank!=0 or maxRank!=6: s=s.join(draft_table,deck_table.c.draft_id==draft_table.c.draft_id).where(
        draft_table.c.rank>=minRank,draft_table.c.rank<=maxRank)
    df=pd.read_sql_query(s,conn)
    conn.close()
    return df.to_json()

def getCardRecordByCopies(card_name:str, main_colors='ALL', set_abbr='ltr',minRank=0,maxRank=6):
    #Gets number of wins, games played, and win rate for a given card split up by number of copies of that card in the deck
    #For example "4":{"wins":859.0,"games":1414.0,"win_rate":0.6074964639}
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    card_table=metadata.tables[set_abbr+'CardInfo']
    arid_table=metadata.tables[set_abbr+'ArchRank']
    s=select(cg_table.c.copies,func.sum(cg_table.c.win_count),func.sum(cg_table.c.game_count)).group_by(cg_table.c.copies).join(
        card_table,cg_table.c.id==card_table.c.id).where(card_table.c.name==card_name).join(
            arid_table,cg_table.c.arid==arid_table.c.id).where(arid_table.c.rank>=minRank,arid_table.c.rank<=maxRank)
    if main_colors!='ALL': s=s.where(arid_table.c.main_colors==main_colors)
    df=pd.read_sql_query(s,conn)
    df.set_index(['copies'],inplace=True)
    df.columns=['wins','games']
    tempgames=df['games'].mask(df['games']==0,1) #replaces 0 with 1 to avoid dividing by 0
    df['win_rate']=df['wins']/tempgames
    df=df.T
    conn.close()
    return df.to_json()
