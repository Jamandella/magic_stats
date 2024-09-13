#Functions for reading and deriving statistics from the database

import pandas as pd
from sqlalchemy import MetaData, select, create_engine, func
from backend.statfunctions import colorInt,archLabelToID, archIDtoLabel
import os
from dotenv import load_dotenv
load_dotenv()
db_url=os.getenv("DB_URL")
engine=create_engine(url=db_url)  
def cardInfo(set_abbr:str,as_json=True):
    #Returns the full card info table for the given set. Defaults to returning a pandas dataframe, with an option for json instead.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    card_table=metadata.tables[set_abbr+'CardInfo']
    s=select(card_table)
    df=pd.read_sql_query(s,conn,index_col='id')
    conn.close()
    if as_json:return df.to_json()
    else: return df


def getCardsWithColor(set_abbr:str,color:str,include_multicolor=True, include_lands=False, as_string=True):
    #Returns list of all cards matching the given color
    #If as_string=True gives their names, otherwise gives their integer index in cardInfo
    #Color is determined (in setinfo.py) by mana cost. Could be misleading on some cards like DFCs, adventures, alternate costs, etc.
    #If include_multicolor, get all cards containing that color. Otherwise get cards that are exactly that color.
    #Lands are all marked as colorless. If color='C' and include_lands=True, lands will be included with the colorless cards.
    set_abbr=set_abbr.lower()
    color=color.upper()
    carddf=cardInfo(set_abbr,as_json=False)
    colors=['W','U','R','B','G','C']
    set_abbr=set_abbr.lower()
    color=color.upper()
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
    resp={'response':cards}
    return resp

def getArchAvgCurve(set_abbr:str, arch_label:str):
    #returns mean values of lands and each n drop for given archetype
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_stats_table=metadata.tables[set_abbr+'ArchGameStats']
    #Query all rows of the ArchGameStats table corresponding to given deck
    arch_id=archLabelToID(arch_label)
    q=select(arch_stats_table).where(arch_stats_table.c.arch_id==arch_id)
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
def getArchRecord(set_abbr:str, arch_label:str):
    #returns a a given deck's total wins, losses, drafts, win percentage, and average record per draft. wins/(wins+losses) for deck's overall win rate. 
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_table=metadata.tables[set_abbr+'Archetypes']
    q1=select(arch_table.c.num_drafts,arch_table.c.num_wins,arch_table.c.num_losses).where(arch_table.c.arch_label==arch_label)                                                                             
    df=pd.read_sql_query(q1,conn)
    conn.close()
    df['num_games']=df['num_wins']+df['num_losses']
    df['win_rate']=df['num_wins']/df['num_games']
    result=pd.Series(data=df.loc[0])
    return result.to_json()
def getCardInDeckWinRates(set_abbr:str,arch_label='ALL', minCopies=1, maxCopies=40,index_by_name=False,as_json=True): 
#Returns game played win rates for all cards, indexed by their numerical id from CardInfo table. Can be restricted to specific decks.
#Can also require a specific range of copies of each card.
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    arch_id=archLabelToID(arch_label)
    if index_by_name:
        card_table=metadata.tables[set_abbr+'CardInfo']
        q=select(card_table.c.name,func.sum(cg_table.c.win_count).label("wins"),
                 func.sum(cg_table.c.game_count).label("games_played")).join(
                    card_table, cg_table.c.id==card_table.c.id).where(
                     cg_table.c.copies>=minCopies,
                     cg_table.c.copies<=maxCopies).group_by(card_table.c.name)
        if arch_label!='ALL':
            q=q.where(cg_table.c.arch_id==arch_id)
        df=pd.read_sql(q,conn,index_col='name')
    else:
        q=select(cg_table.c.id,func.sum(cg_table.c.win_count).label("wins"),func.sum(cg_table.c.game_count).label("games_played")).where(
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies).group_by(cg_table.c.id)
        if arch_label!='ALL':
            q=q.where(cg_table.c.arch_id==arch_id)
        df=pd.read_sql_query(q,conn,index_col='id')
    conn.close()
    tempgames=df['games_played'].mask(df['games_played']==0,1) #Used so that 0wins/0games->0%
    df['win_rate']=df['wins']/tempgames
    df.sort_index(inplace=True)
    if as_json: return df.to_json()
    else: return df

def getRecordByLength(set_abbr:str, arch_label:str,):
    #For each game length (by number of turns), returns given archetype's record, win rate, and how frequently games last that long.
    #Games of length <=4 and >=16 are grouped together
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    MINTURNS=4
    MAXTURNS=16
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_stats_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_id=archLabelToID(arch_label)
    q1=select(arch_stats_table.c.turns,arch_stats_table.c.won,func.sum(arch_stats_table.c.game_count).label('games')).group_by(
        arch_stats_table.c.turns,arch_stats_table.c.won).where(arch_stats_table.c.arch_id==arch_id
    ).order_by(arch_stats_table.c.turns,arch_stats_table.c.won)   
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

def getMetaDistribution(set_abbr:str, minRank=0,maxRank=6):
    #Gets number of drafts for each set of main colors. Can be filtered by rank to show the metagame at user's level.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    #draft_table=metadata.tables[set_abbr+"DraftInfo"]
    deck_table=metadata.tables[set_abbr+"Decklists"]
    s=select(deck_table.c.main_colors,func.count(1).label('drafts')).group_by(deck_table.c.main_colors)
    if minRank!=0 or maxRank!=6: 
        rank_names=[None,'bronze','silver','gold','platinum','diamond','mythic']
        valid_ranks=rank_names[minRank:maxRank+1]
        s=s.where(
        deck_table.c.rank.in_(valid_ranks))
    df=pd.read_sql_query(s,conn)
    total_drafts=df['drafts'].sum()
    df.set_index('main_colors',inplace=True)
    df['meta_share']=df['drafts']/total_drafts
    return df.to_json()

def getCardRecordByCopies(set_abbr:str, card_name:str, arch_label='ALL', ):
    #Gets number of wins, games played, and win rate for a given card split up by number of copies of that card in the deck
    #For example "4":{"wins":859.0,"games":1414.0,"win_rate":0.6074964639}
    #card_name is case sensitive
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cg_table=metadata.tables[set_abbr+'CardGameStats']
    card_table=metadata.tables[set_abbr+'CardInfo']
    arch_id=archLabelToID(arch_label)
    s=select(cg_table.c.copies,func.sum(cg_table.c.win_count),func.sum(cg_table.c.game_count)).group_by(cg_table.c.copies).join(
        card_table,cg_table.c.id==card_table.c.id).where(card_table.c.name==card_name,card_table.c.arch_id==arch_id)
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
def getGameInHandWR(set_abbr:str, arch_label='ALL', as_json=True,index_by_name=False):
    #Returns game in hand win rate for all cards in the given set. May be filtered by archetype, or 'ALL' to count all games.
    #Includes both win rate and number of games in hand, which is the sample size.
    #Cards are labeled by their id in the CardInfo table, unless index_by_name=True, then they use their card names
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cds_table=metadata.tables[set_abbr+'CardDerivedStats']
    arch_id=archLabelToID(arch_label)
    if index_by_name:
        card_table=metadata.tables[set_abbr+'CardInfo']
        s=select(cds_table.c.games_in_hand,cds_table.c.wins_in_hand,card_table.c.name).join(
            card_table,cds_table.c.card_id==card_table.c.id).where(
            cds_table.c.arch_id==arch_id    
            )
        resultDF=pd.read_sql(s,conn,index_col='name')
    else:
        s=select(cds_table.c.games_in_hand,cds_table.c.wins_in_hand,cds_table.c.card_id).where(cds_table.c.arch_id==arch_id)
        resultDF=pd.read_sql(s,conn,index_col='card_id')
    conn.close()
    tempgames=resultDF['games_in_hand'].mask(resultDF['games_in_hand']==0,1)
    resultDF['win_rate']=resultDF['wins_in_hand']/tempgames
    #resultDF['significant_sample']=resultDF['games_in_hand']>500
    resultDF.drop('wins_in_hand',axis=1,inplace=True)
    resultDF.sort_index(inplace=True)
    if as_json: return resultDF.to_json()
    else: return resultDF
def getAverageWinShares(set_abbr:str,arch_label='ALL',as_json=True,index_by_name=False):
    #Returns average win shares per appearance for all cards in the given set. May be filtered by archetype, or 'ALL' to count all games.
    #Includes both win shares and number of games in hand, which is the sample size.
    #Cards are labeled by their id in the CardInfo table
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    cds_table=metadata.tables[set_abbr+'CardDerivedStats']
    arch_id=archLabelToID(arch_label)
    if index_by_name:
        card_table=metadata.tables[set_abbr+'CardInfo']
        s=select(cds_table.c.games_in_hand,cds_table.c.avg_win_shares,card_table.c.name).join(
            card_table,cds_table.c.card_id==card_table.c.id).where(
            cds_table.c.arch_id==arch_id)
        resultDF=pd.read_sql(s,conn,index_col='name')    

    else:    
        s=select(cds_table.c.games_in_hand,cds_table.c.avg_win_shares,cds_table.c.card_id).where(cds_table.c.arch_id==arch_id)
        resultDF=pd.read_sql(s,conn,index_col='card_id')
    #resultDF['signficant_sample']=resultDF['games_in_hand']>500
    conn.close()
    if as_json: return resultDF.to_json()
    else: return resultDF

def getArchWinRatesByMulls(set_abbr:str,arch_label='ALL', as_json=True):
    #Returns win rates and number of games played on play, draw, and overall by number of mulligans taken.
    #Any game with 3 or more mulligans is grouped into num_mulligans=3.
    #If arch_label='ALL', returns cumulative records where games for all archetypes are included
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    start_table=metadata.tables[set_abbr+'ArchStartStats']
    arch_id=archLabelToID(arch_label)
    s=select(start_table.c.num_mulligans,start_table.c.on_play,start_table.c.win_count,start_table.c.game_count).where(
        start_table.c.arch_id==arch_id)
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
def getPlayDrawSplits(set_abbr:str, as_json=True):
    #Returns number of games played and win rate on the play and on the draw for each archetype
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    start_table=metadata.tables[set_abbr+'ArchStartStats']
    s=select(start_table.c.arch_id,start_table.c.on_play,func.sum(start_table.c.win_count).label('wins'),
             func.sum(start_table.c.game_count).label('games')).group_by(start_table.c.arch_id,start_table.c.on_play)
    resultDF=pd.read_sql_query(s,conn,index_col=['arch_id','on_play'])
    conn.close()
    resultDF['win_rate']=resultDF['wins']/(resultDF['games'].mask(resultDF['games']==0,1))
    resultDF.sort_index(inplace=True)
    outputDF=pd.DataFrame({'games_on_play':[],'wr_on_play':[],'games_on_draw':[],'wr_on_draw':[]})
    ids=[x[0] for x in resultDF.index]
    for arch_num in ids:
        arch_label=archIDtoLabel(arch_num)
        games_on_play=resultDF.at[(arch_num,True),'games']
        wr_on_play=resultDF.at[(arch_num,True),'win_rate']
        games_on_draw=resultDF.at[(arch_num,False),'games']
        wr_on_draw=resultDF.at[(arch_num,False),'win_rate']
        outputDF.loc[arch_label]=[games_on_play,wr_on_play,games_on_draw,wr_on_draw]
    if as_json: return outputDF.to_json()
    else: return outputDF

def getMeanDecklist(set_abbr:str, arch_label:str, min_wins=0, max_wins=7, min_rank=0, max_rank=6,as_json=True):
    #Get's average decklist for all decks of a given set in specified colors or archetype. Can be filtered by rank and record.
    #(Infrastructure exists to filter by date drafted too if we want)
    #arch_label can be colors as a WUBRG string, getting all decks with those main colors, e.g. "WB" 
    #or colors plus a number, e.g.' "WB2", getting a subarchetype of those colors
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    deck_table=metadata.tables[set_abbr+'Decklists']
    card_table=metadata.tables[set_abbr+'CardInfo']
    s0=select(card_table.c.name)
    cardSeries=pd.read_sql_query(s0,conn)
    select_args=[func.count(1).label('num_decks')]
    for card_name in cardSeries['name']:
        select_args.append(func.avg(getattr(deck_table.c,card_name)).label(card_name))
    s=select(*select_args)
    if min_wins>0:
        s=s.where(deck_table.c.wins>=min_wins)
    if max_wins<7:
        s=s.where(deck_table.c.wins<=max_wins)
    if min_rank>0:
        s=s.where(deck_table.c.rank>=min_rank)
    if max_rank<6:
        s=s.where(deck_table.c.rank<=max_rank)
    if arch_label[-1:].isnumeric():
        arch_id=archLabelToID(arch_label)
        s=s.where(deck_table.c.arch_id==arch_id)
    else:
        s=s.where(deck_table.c.main_colors==arch_label)
    resultDF=pd.read_sql_query(s,conn)
    conn.close()
    if as_json: return resultDF.T.to_json()
    else: return resultDF
def getArchetypeLabels(set_abbr:str,main_colors='ALL'):
    #Returns the archetypes that exist for the given set
    #If WU was split into 3 groups, the list will contain 'WU' and 'WU1', 'WU2', and 'WU3'.
    #If BRG was not divided, the list will contain only 'BRG'
    #Can be restricted to only one set of main colors by setting main_colors to be the corresponding WUBRG string.
    set_abbr=set_abbr.lower()
    main_colors=main_colors.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    arch_table=metadata.tables[set_abbr+'Archetypes']
    s=select(arch_table.c.id,arch_table.c.arch_label)
    if main_colors!='ALL':
        color_number=colorInt(main_colors)
        s=s.where(arch_table.c.id%32==color_number)
    resultDF=pd.read_sql_query(s,conn,index_col='id')
    conn.close()
    return resultDF.to_json()
