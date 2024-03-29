#Functions for reading and deriving statistics from the database

import pandas as pd
from sqlalchemy import MetaData, select, create_engine, func
from dbpgstrings import host, database, user, password 
engine=create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
    user, password, host, 5432, database))  


def cardInfo(set_abbr='ltr',as_json=True):
    #Returns the full card info table for the given set. Defaults to returning a pandas dataframe, with an option for json instead.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
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
    carddf=cardInfo(set_abbr,as_json=False)
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
def getArchRecord(archLabel,set_abbr='ltr'):
    #returns a a given deck's total wins, losses, drafts, win percentage, and average record per draft. wins/(wins+losses) for deck's overall win rate. 
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q1=select(arch_table.c.num_drafts,arch_table.c.num_wins,arch_table.c.num_losses).where(arch_table.c.archLabel==archLabel)                                                                             
    df=pd.read_sql_query(q1,conn)
    conn.close()
    df['num_games']=df['num_wins']+df['num_losses']
    df['win_rate']=df['num_wins']/df['num_games']
    #df['significant_sample']=df['num_games']>500
    result=pd.Series(data=df.loc[0])
    return result.to_json()
def getCardInDeckWinRates(archLabel='ALL', minCopies=1, maxCopies=40, set_abbr='ltr', index_by_name=False,as_json=True): 
#Returns game played win rates for all cards, indexed by their numerical id from CardInfo table. Can be restricted to specific decks.
#Can also require a specific range of copies of each card.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    arch_table=metadata.tables[set_abbr +'Archetypes']
    if index_by_name:
        card_table=metadata.tables[set_abbr+'CardInfo']
        q=select(card_table.c.name,func.sum(cg_table.c.win_count).label("wins"),
                 func.sum(cg_table.c.game_count).label("games_played")).join(
                    arch_table, cg_table.c.arch_id==arch_table.c.id).join(
                    card_table, cg_table.c.id==card_table.c.id).where(
                     cg_table.c.copies>=minCopies,
                     cg_table.c.copies<=maxCopies).group_by(card_table.c.name)
        if archLabel!='ALL':
            q=q.where(arch_table.c.archLabel==archLabel)
        df=pd.read_sql(q,conn,index_col='name')
    else:
        q=select(cg_table.c.id,func.sum(cg_table.c.win_count).label("wins"),func.sum(cg_table.c.game_count).label("games_played")).join(arch_table, cg_table.c.arch_id==arch_table.c.id).where(
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies).group_by(cg_table.c.id)
        if archLabel!='ALL':
            q=q.where(arch_table.c.archLabel==archLabel)
        df=pd.read_sql_query(q,conn,index_col='id')
    conn.close()
    #df['significant_sample']=df['games_played']>500
    tempgames=df['games_played'].mask(df['games_played']==0,1) #Used so that 0wins/0games->0%
    df['win_rate']=df['wins']/tempgames
    df.sort_index(inplace=True)
    if as_json: return df.to_json()
    else: return df

def getRecordByLength(archLabel:str,set_abbr='ltr'):
    #For each game length (by number of turns), returns given archetype's record, win rate, and how frequently games last that long.
    #Games of length <=4 and >=16 are grouped together
    MINTURNS=4
    MAXTURNS=16
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    archStats_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q1=select(archStats_table.c.turns,archStats_table.c.won,func.sum(archStats_table.c.game_count).label('games')).group_by(
        archStats_table.c.turns,archStats_table.c.won).join(
        arch_table,archStats_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==archLabel
    ).order_by(archStats_table.c.turns,archStats_table.c.won)   
    resDF=pd.read_sql_query(q1,conn) 
    conn.close()
    outputDF=pd.DataFrame({'turns':[],'wins':[],'games':[]})
    outputDF.set_index('turns',inplace=True)
    df2=resDF.set_index(['turns','won'],inplace=False)
    games_min=0 
    wins_min=0
    for num_turns in range(1,MINTURNS+1): #Grouping together all games that last 5 or less turns
        if (num_turns, True) in df2.index:
            wins_min+=df2.at[(num_turns,True),'games']
            games_min+=df2.at[(num_turns,True),'games']
        if (num_turns, False) in df2.index:
            games_min+=df2.at[(num_turns,False),'games']
    outputDF.loc[MINTURNS]=[wins_min,games_min]
    for num_turns in range(MINTURNS+1,MAXTURNS):
        games=0
        wins=0
        if (num_turns, True) in df2.index:
            wins=df2.at[(num_turns,True),'games']
            games+=wins
        if (num_turns, False) in df2.index:
            games+=df2.at[(num_turns,False),'games']
        outputDF.loc[num_turns]=[wins,games]   
    games_max=0
    wins_max=0 
    for num_turns in range(MAXTURNS, resDF['turns'].max()+1):
        if (num_turns, True) in df2.index:
            wins_max+=df2.at[(num_turns,True),'games']
            games_max+=df2.at[(num_turns,True),'games']
        if (num_turns, False) in df2.index:
            games_max+=df2.at[(num_turns,False),'games']
    outputDF.loc[MAXTURNS]=[wins_max,games_max]
    tempgames=outputDF['games'].mask(outputDF['games']==0,1) #replaces 0 with 1 to avoid dividing by 0
    outputDF['win_rate']=outputDF['wins']/tempgames
    total_games=outputDF['games'].sum()
    outputDF['game_length_rate']=outputDF['games']/total_games
    #outputDF['significant_sample']=outputDF['games']>500
    return outputDF.to_json()

def getMetaDistribution(set_abbr='ltr', minRank=0,maxRank=6):
    #Gets number of drafts for each set of main colors. Can be filtered by rank to show the metagame at user's level.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    draft_table=metadata.tables[set_abbr+"DraftInfo"]
    deck_table=metadata.tables[set_abbr+"Decklists"]
    s=select(deck_table.c.main_colors,func.count(1).label('drafts')).group_by(deck_table.c.main_colors)
    if minRank!=0 or maxRank!=6: 
        rank_names=[None,'bronze','silver','gold','platinum','diamond','mythic']
        valid_ranks=rank_names[minRank:maxRank+1]
        s=s.join(draft_table,deck_table.c.draft_id==draft_table.c.draft_id).where(
        draft_table.c.rank.in_(valid_ranks))
    df=pd.read_sql_query(s,conn)
    total_drafts=df['drafts'].sum()
    df.set_index('main_colors',inplace=True)
    df['meta_share']=df['drafts']/total_drafts
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
    conn.close()
    df.set_index(['copies'],inplace=True)
    df.sort_index(inplace=True)
    df.columns=['wins','games']
    tempgames=df['games'].mask(df['games']==0,1) #replaces 0 with 1 to avoid dividing by 0
    df.loc[4]=df.loc[4:].sum()
    df=df.loc[:4]
    df['win_rate']=df['wins']/tempgames
    #df['significant_sample']=df['games']>500
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
    conn.close()
    tempgames=resultDF['games_in_hand'].mask(resultDF['games_in_hand']==0,1)
    resultDF['win_rate']=resultDF['wins_in_hand']/tempgames
    #resultDF['significant_sample']=resultDF['games_in_hand']>500
    resultDF.drop('wins_in_hand',axis=1,inplace=True)
    resultDF.sort_index(inplace=True)
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
    #resultDF['signficant_sample']=resultDF['games_in_hand']>500
    conn.close()
    if as_json: return resultDF.to_json()
    else: return resultDF

def getArchWinRatesByMulls(main_colors='ALL',set_abbr='ltr', as_json=True):
    #Returns win rates and number of games played on play, draw, and overall by number of mulligans taken.
    #Any game with 3 or more mulligans is grouped into num_mulligans=3.
    #If main_colors='ALL', returns cumulative records where games for all archetypes are included
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    start_table=metadata.tables[set_abbr+'ArchStartStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    s=select(start_table.c.num_mulligans,start_table.c.on_play,start_table.c.win_count,start_table.c.game_count).join(
            arch_table,start_table.c.arch_id==arch_table.c.id).where(arch_table.c.archLabel==main_colors)
    resultDF=pd.read_sql_query(s,conn)
    conn.close()
    resultDF.sort_values(['num_mulligans','on_play'],inplace=True)
    outputDF=pd.DataFrame({'games_on_play':[],'wr_on_play':[],'games_on_draw':[],
                           'wr_on_draw':[],'games_total':[],'wr_total':[]})
    for mulls in range(4):
        games_on_draw=resultDF.loc[2*mulls,'game_count']
        games_on_play=resultDF.loc[2*mulls+1,'game_count']
        games_total=games_on_draw+games_on_play
        wins_on_draw=resultDF.loc[2*mulls,'win_count']
        wins_on_play=resultDF.loc[2*mulls+1,'win_count']
        wr_on_draw=round(wins_on_draw/max(games_on_draw,1),4)
        wr_on_play=round(wins_on_play/max(games_on_play,1),4)
        wr_total=round((wins_on_play+wins_on_draw)/max(games_total,1),4)
        outputDF.loc[mulls]=[int(games_on_play),wr_on_play,int(games_on_draw),wr_on_draw,int(games_total),wr_total]
    if as_json: return outputDF.to_json()
    else: return outputDF
def getPlayDrawSplits(set_abbr='ltr', as_json=True):
    #Returns number of games played and win rate on the play and on the draw for each archetype
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    start_table=metadata.tables[set_abbr+'ArchStartStats']
    arch_table=metadata.tables[set_abbr+'Archetypes']
    s=select(arch_table.c.archLabel,func.max(start_table.c.arch_id).label("arch_id"),start_table.c.on_play,func.sum(start_table.c.win_count).label('wins'),
             func.sum(start_table.c.game_count).label('games')).join(
            arch_table,start_table.c.arch_id==arch_table.c.id).group_by(arch_table.c.archLabel,start_table.c.on_play)
    resultDF=pd.read_sql_query(s,conn,index_col=['arch_id','on_play'])
    conn.close()
    resultDF['win_rate']=resultDF['wins']/(resultDF['games'].mask(resultDF['games']==0,1))
    resultDF.sort_index(inplace=True)
    outputDF=pd.DataFrame({'games_on_play':[],'wr_on_play':[],'games_on_draw':[],'wr_on_draw':[]})
    for arch_num in range(-1,32):
        archLabel=resultDF.at[(arch_num,False),'archLabel']
        games_on_play=resultDF.at[(arch_num,True),'games']
        wr_on_play=resultDF.at[(arch_num,True),'win_rate']
        games_on_draw=resultDF.at[(arch_num,False),'games']
        wr_on_draw=resultDF.at[(arch_num,False),'win_rate']
        outputDF.loc[archLabel]=[games_on_play,wr_on_play,games_on_draw,wr_on_draw]
    if as_json: return outputDF.to_json()
    else: return resultDF

print(getPlayDrawSplits(set_abbr='ltr'))