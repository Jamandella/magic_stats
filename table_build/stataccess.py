#Functions for reading and deriving statistics from the database

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
    card_table=metadata.tables[set_abbr+'CardInfo']
    s=select(card_table)
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

def getArchAvgCurve(archLabel, set_abbr='ltr'):
    #returns mean values of lands and each n drop for given archetype
    #Currently, archLabel is just colors in string form, e.g. 'G', or 'UB'. All colors are listed in WUBRG order.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    #Query all rows of the ArchGameStats table corresponding to given deck
    q=select(archStats_table).join(arch_table, archStats_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==archLabel)
    df=pd.read_sql_query(q,conn) 
    #Remove first 3 columns from that table, leaving only game count and number of cards per mana value. Add up all the columns.
    dfTotal=df.iloc[0:,3:].sum() 
    conn.close()
    if dfTotal['game_count']!=0: #Avoiding divide by 0.
        n=dfTotal['game_count']
        avgs=dfTotal.iloc[1:]/n
        return avgs.to_json()
    else: #Should be all 0's in this case.
        return dfTotal.iloc[1:].to_json()
def getArchBasics(archLabel,set_abbr='ltr'):
    #returns a a given deck's total wins, losses, and drafts. wins/(wins+losses) for deck's overall win rate. 
    #wins/drafts and losses/drafts for deck's average record.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q1=select(arch_table.c.num_drafts,arch_table.c.num_wins,arch_table.c.numlosses).where(arch_table.c.archLabel==archLabel)                                                                             
    df=pd.read_sql_query(q1,conn)
    return df.to_json()
def getCardInDeckWinRates(archLabel='ALL', minCopies=1, maxCopies=40, set_abbr='ltr', as_json=True): 
#Returns game played win rates for all cards, indexed by their numerical id from CardInfo table. Can be restricted to specific decks and ranks.
#Can also require a specific range of copies of each card.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    arch_table=metadata.tables[set_abbr +'Archetypes']
    q=select(cg_table.c.id,func.sum(cg_table.c.win_count).label("wins"),func.sum(cg_table.c.game_count).label("games_played")).join(arch_table, cg_table.c.arch_id==arch_table.c.id).where(
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies).group_by(cg_table.c.id)
    if archLabel!='ALL':
        q=q.where(arch_table.c.archLabel==archLabel)
    df=pd.read_sql_query(q,conn)
    conn.close()
    tempgames=df['games_played'].mask(df['games_played']==0,1) #Used so that 0wins/0games->0%
    df['win_rate']=df['wins']/tempgames
    if as_json: return df.to_json()
    else: return df

def getRecordByLength(archLabel:str,set_abbr='ltr'):
    #Returns number of wins and losses at each game length for given deck
    #Shows how long the games tend to be and how the deck does as the game gets longer
    #"'(8,False)':9303" means that the deck lost 9303 games that lasted exactly 8 turns
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q1=select(archStats_table.c.turns,archStats_table.c.won,func.sum(archStats_table.c.game_count).label('games')).group_by(
        archStats_table.c.turns,archStats_table.c.won).join(
        arch_table,archStats_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==archLabel
    ).order_by(archStats_table.c.turns,archStats_table.c.won)   
    df=pd.read_sql_query(q1,conn) 
    df.set_index(['turns','won'],inplace=True)
    conn.close()
    return df.to_json()

def getMetaDistribution(set_abbr='ltr', minRank=0,maxRank=6):
    #Gets number of drafts for each set of main colors. Can be filtered by rank to show the metagame at user's level.
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

def getCardRecordByCopies(card_name:str, main_colors='ALL', set_abbr='ltr'):
    #Gets number of wins, games played, and win rate for a given card split up by number of copies of that card in the deck
    #For example "4":{"wins":859.0,"games":1414.0,"win_rate":0.6074964639}
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    card_table=metadata.tables[set_abbr+'CardInfo']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    s=select(cg_table.c.copies,func.sum(cg_table.c.win_count),func.sum(cg_table.c.game_count)).group_by(cg_table.c.copies).join(
        card_table,cg_table.c.id==card_table.c.id).where(card_table.c.name==card_name)
    if main_colors!='ALL': s=s.join(arch_table,cg_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==main_colors)
    df=pd.read_sql_query(s,conn)
    df.set_index(['copies'],inplace=True)
    df.columns=['wins','games']
    tempgames=df['games'].mask(df['games']==0,1) #replaces 0 with 1 to avoid dividing by 0
    df['win_rate']=df['wins']/tempgames
    df=df.T
    conn.close()
    return df.to_json()
def getGameInHandWR(main_colors='ALL',set_abbr='ltr', as_json=True,index_by_name=False):
    #Returns game in hand win rate for all cards in the given set. May be filtered by archetype, or 'ALL' to count all games.
    #Includes both win rate and number of games in hand, which is the sample size.
    #Cards are labeled by their id in the CardInfo table, unless index_by_name=True, then they use their card names
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cds_table=metadata.tables[set_abbr+'CardDerivedStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    if index_by_name:
        card_table=metadata.tables[set_abbr+'CardInfo']
        s=select(cds_table.c.games_in_hand,cds_table.c.wins_in_hand,card_table.c.name).join(
            arch_table,cds_table.c.arch_id==arch_table.c.id).join(card_table,cds_table.c.card_id==card_table.c.id).where(
            arch_table.c.archLabel==main_colors    
            )
        resultDF=pd.read_sql(s,conn,index_col='name')
    else:
        s=select(cds_table.c.games_in_hand,cds_table.c.wins_in_hand,cds_table.c.card_id).join(
            arch_table,cds_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==main_colors)
        resultDF=pd.read_sql(s,conn,index_col='card_id')
    tempgames=resultDF['games_in_hand'].mask(resultDF['games_in_hand']==0,1)
    resultDF['win_rate']=resultDF['wins_in_hand']/tempgames
    resultDF.drop('wins_in_hand',axis=1,inplace=True)
    if as_json: return resultDF.to_json()
    else: return resultDF
def getAverageWinShares(main_colors='ALL',set_abbr='ltr',as_json=True,index_by_name=False):
    #Returns average win shares per appearance for all cards in the given set. May be filtered by archetype, or 'ALL' to count all games.
    #Includes both win shares and number of games in hand, which is the sample size.
    #Cards are labeled by their id in the CardInfo table
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cds_table=metadata.tables[set_abbr+'CardDerivedStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    if index_by_name:
        card_table=metadata.tables[set_abbr+'CardInfo']
        s=select(cds_table.c.games_in_hand,cds_table.c.avg_win_shares,card_table.c.name).join(
            arch_table,cds_table.c.arch_id==arch_table.c.id).join(card_table,cds_table.c.card_id==card_table.c.id).where(
            arch_table.c.archLabel==main_colors)
        resultDF=pd.read_sql(s,conn,index_col='name')    

    else:    
        s=select(cds_table.c.games_in_hand,cds_table.c.avg_win_shares,cds_table.c.card_id).join(
            arch_table,cds_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==main_colors)
        resultDF=pd.read_sql(s,conn,index_col='card_id')
    if as_json: return resultDF.to_json()
    else: return resultDF
#print(getGameInHandWR(main_colors='UR'))
#print(getAverageWinShares(main_colors='UR'))

