#Functions for reading and deriving statistics from the database


#TODO: 
import pandas as pd
from sqlalchemy import MetaData, select, create_engine, func
from backend.statfunctions import colorInt,archLabelToID, archIDtoLabel, meanDecklist, cardInfo
import os
from dotenv import load_dotenv
load_dotenv()
db_url=os.getenv("DATABASE_URL")
engine=create_engine(url=db_url)  

#Currently Useful Functions:
def getActiveSets():
    #Returns a list of all sets that are currently active in the database.
    #Includes the set abbreviation, full title, release date, and time of last update.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    sets_table=metadata.tables['ActiveSets']
    output=pd.read_sql_table(sets_table.name,conn).to_json()
    conn.close()
    return output
def getMostRecentSet():
    #Returns the abbreviation and name for the most recent set in the database by release date.
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    sets_table=metadata.tables['ActiveSets']
    s=select(sets_table.c.set_abbr,sets_table.c.set_name).order_by(sets_table.c.set_release_date.desc()).limit(1)
    output=pd.read_sql_query(s,conn).to_json()
    conn.close()
    return output
def getCardInfo(set_abbr:str,as_json=True):
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
def getMetaDistribution(set_abbr:str, min_rank=0,max_rank=6):
    #Gets number of drafts for each set of main colors. Can be filtered by rank to show the metagame at user's level.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    #draft_table=metadata.tables[set_abbr+"DraftInfo"]
    deck_table=metadata.tables[set_abbr+"Decklists"]
    s=select(deck_table.c.main_colors,func.count(1).label('drafts')).group_by(deck_table.c.main_colors)
    if min_rank!=0 or max_rank!=6: 
        s=s.where(
        deck_table.c.rank>=min_rank,deck_table.c.rank<=max_rank)
    df=pd.read_sql_query(s,conn)
    total_drafts=df['drafts'].sum()
    df.set_index('main_colors',inplace=True)
    df['meta_share']=df['drafts']/total_drafts
    return df.to_json()

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
    
def makeCardTable(set_abbr:str, arch_label='ALL', as_json=True):
    #Returns a table of all cards in the given set, with descriptive attributes and stats for each card.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    card_table=metadata.tables[set_abbr+'CardInfo']
    #First make all relevant queries before any calculations/manipulations.
    s1=select(card_table.c.id,card_table.c.name,card_table.c.color,
             card_table.c.rarity)
    df=pd.read_sql_query(s1,conn,index_col='id')
    card_stats_table=metadata.tables[set_abbr+'CardGameStats']
    s2=select(card_stats_table.c.id,func.sum(card_stats_table.c.win_count).label('wins'),func.sum(card_stats_table.c.game_count).label('games_played'),).group_by(card_stats_table.c.id)
    arch_id=archLabelToID(arch_label)
    if arch_label!='ALL':
        s2=s2.where(card_stats_table.c.arch_id==arch_id)
    df2=pd.read_sql_query(s2,conn,index_col='id')
    pack_table=metadata.tables[set_abbr+'DraftPacks']
    s3=select(pack_table)
    pack_df=pd.read_sql_query(s3,conn)
    derived_stats_table=metadata.tables[set_abbr+'CardDerivedStats']
    s4=select(derived_stats_table.c.card_id,derived_stats_table.c.games_in_hand,derived_stats_table.c.wins_in_hand,
              derived_stats_table.c.adj_gihwr,derived_stats_table.c.adjusted_iwd,
              derived_stats_table.c.inclusion_impact).where(derived_stats_table.c.arch_id==arch_id)
    derived_stats_df=pd.read_sql_query(s4,conn,index_col='card_id')
    conn.close()
    #Compute GPWR and attach to output
    df['GPWR']=df2['wins']/(df2['games_played'].mask(df2['games_played']==0,1))
    df['games_played']=df2['games_played']
    df['GPWR']=df['GPWR'].mask(df['GPWR'].isna(),0) #Replace NaN with 0
    #Compute average pick and attach to output
    pack_df=pack_df.groupby('pick_number').sum()
    pack_df.drop('pack_number',axis=1,inplace=True) 
    new_col_names=[]
    for col in pack_df.columns:
        if col[:10]=='pack_card_':
            new_col_names.append(col[10:])
        else:
            new_col_names.append(col)
    pack_df.columns=new_col_names
    pack_df=pack_df.T
    mean_pick_df=pack_df.sum(axis=1)/pack_df.max(axis=1)
    mean_pick_df.index.name='name'
    mean_pick_df.sort_index(inplace=True)
    df.sort_values('name',inplace=True) #Sorting by name so that the mean pick order matches the card order.)
    df['mean_pick']=mean_pick_df.values
    #Compute games in hand win rate and attach to output
    derived_stats_df['GIHWR']=derived_stats_df['wins_in_hand']/(derived_stats_df['games_in_hand'].mask(derived_stats_df['games_in_hand']==0,1))
    df['games_in_hand']=derived_stats_df['games_in_hand']
    df['GIHWR']=derived_stats_df['GIHWR']
    df['adjusted_IWD']=derived_stats_df['adjusted_iwd']
    df['inclusion_impact']=derived_stats_df['inclusion_impact']
    df['adjusted_GIHWR']=derived_stats_df['adj_gihwr']
    if as_json: return df.to_json()
    else: return df
def makeFormatOverviewTable(set_abbr:str, as_json=True):
    #Returns a table of all archetypes in the given set, with their win rates, number of games played, and number of drafts.
    #Also has a couple stats to indicate the speed of the deck.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_table=metadata.tables[set_abbr+'Archetypes']
    s1=select(arch_table.c.arch_label,arch_table.c.num_wins,arch_table.c.num_losses,arch_table.c.num_drafts).where(arch_table.c.id<32)
    df=pd.read_sql_query(s1,conn,index_col='arch_label')
    arch_stats_table=metadata.tables[set_abbr+'ArchGameStats']
    s2=select(arch_stats_table.c.arch_id,arch_stats_table.c.turns,arch_stats_table.c.won,arch_stats_table.c.game_count.label('games')).where(arch_stats_table.c.arch_id<32)
    arch_stats_df=pd.read_sql_query(s2,conn) 
    conn.close()
    df['average_win_length']=[0]*33
    df['average_loss_length']=[0]*33
    df['average_game_length']=[0]*33
    df['aggression']=[0]*33
    for id in range(32):
        label=archIDtoLabel(id)
        temp_df= arch_stats_df[arch_stats_df['arch_id']==id]
        if temp_df.shape[0]==0: continue
        df.at[label,'average_win_length']=(temp_df[temp_df['won']==True]['turns']*temp_df[temp_df['won']==True]['games']).sum()/(max(temp_df[temp_df['won']==True]['games'].sum(),1))
        df.at[label,'average_game_length']=(temp_df['turns']*temp_df['games']).sum()/(max(temp_df['games'].sum(),1))
        df.at[label,'average_loss_length']=(temp_df[temp_df['won']==False]['turns']*temp_df[temp_df['won']==False]['games']).sum()/(max(temp_df[temp_df['won']==False]['games'].sum(),1))
        df.at[label,'aggression']=df.at[label,'average_loss_length']-df.at[label,'average_win_length']
    df.at['ALL','average_win_length']=(df['average_win_length']*df['num_wins']).sum()/(max(df['num_wins'].sum(),1))
    df.at['ALL','average_game_length']=(df['average_game_length']*df['num_wins']).sum()/(max(df['num_wins'].sum(),1))
    df.at['ALL','average_loss_length']=(df['average_loss_length']*df['num_losses']).sum()/(max(df['num_losses'].sum(),1))
    df.at['ALL','aggression']=df.at['ALL','average_loss_length']-df.at['ALL','average_win_length']
    df.at['ALL','num_drafts']=df['num_drafts'].sum()
    df.at['ALL','num_wins']=df['num_wins'].sum()
    df.at['ALL','num_losses']=df['num_losses'].sum()
    df['num_games']=df['num_wins']+df['num_losses']
    df['win_rate']=df['num_wins']/(df['num_wins']+df['num_losses']).mask(df['num_wins']+df['num_losses']==0,1)
    output_df=df[['num_drafts','num_games','win_rate','average_win_length','average_game_length','aggression']]
    reorder=['ALL','C','W','U','B','R','G','WU','WB','WR','WG','UB','UR','UG','BR','BG','RG',
             'WUB','WUR','WUG','WBR','WBG','WRG','UBR','UBG','URG','BRG','WUBR','WUBG','WURG','WBRG','UBRG','WUBRG']
    output_df=output_df.loc[reorder]
    if as_json: return output_df.to_json()
    else: return df
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


#May be useful later, but not currently used:

def getArchRecord(set_abbr:str, arch_label:str):
    #returns a a given deck's total wins, losses, drafts, win percentage, and average record per draft. wins/(wins+losses) for deck's overall win rate. 
    #Could be used the page for a single archetype
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
def getCardInDeckWinRates(set_abbr:str,arch_label='ALL', min_copies=1, max_copies=40,index_by_name=False,as_json=True): 
#Returns game played win rates for all cards, indexed by their numerical id from CardInfo table. Can be restricted to specific decks.
#Can also require a specific range of copies of each card.
#This is one column of the full card table, but may be useful on its own.
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
                     cg_table.c.copies>=min_copies,
                     cg_table.c.copies<=max_copies).group_by(card_table.c.name)
        if arch_label!='ALL':
            q=q.where(cg_table.c.arch_id==arch_id)
        df=pd.read_sql(q,conn,index_col='name')
    else:
        q=select(cg_table.c.id,func.sum(cg_table.c.win_count).label("wins"),func.sum(cg_table.c.game_count).label("games_played")).where(
                                                                            cg_table.c.copies>=min_copies,
                                                                            cg_table.c.copies<=max_copies).group_by(cg_table.c.id)
        if arch_label!='ALL':
            q=q.where(cg_table.c.arch_id==arch_id)
        df=pd.read_sql_query(q,conn,index_col='id')
    conn.close()
    tempgames=df['games_played'].mask(df['games_played']==0,1) #Used so that 0wins/0games->0%
    df['win_rate']=df['wins']/tempgames
    df.sort_index(inplace=True)
    if as_json: return df.to_json()
    else: return df
def getGameInHandWR(set_abbr:str, arch_label='ALL', as_json=True,index_by_name=False):
    #Returns game in hand win rate for all cards in the given set. May be filtered by archetype, or 'ALL' to count all games.
    #Includes both win rate and number of games in hand, which is the sample size.
    #Cards are labeled by their id in the CardInfo table, unless index_by_name=True, then they use their card names
    #This is one column of the full card table, but may be useful on its own.
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
def getRecordByLength(set_abbr:str, arch_label:str,):
    #For each game length (by number of turns), returns given archetype's record, win rate, and how frequently games last that long.
    #Games of length <=4 and >=16 are grouped together
    #Intended for use on the archetype page to show how quickly/slowly the deck tends to win or lose.
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
    res_df=pd.read_sql_query(q1,conn) 
    conn.close()
    output_df=pd.DataFrame({'turns':[],'wins':[],'games':[]})
    output_df.set_index('turns',inplace=True)
    df2=res_df.set_index(['turns','won'],inplace=False)
    games_min=0 
    wins_min=0
    for num_turns in range(1,MINTURNS+1): #Grouping together all games that last 5 or less turns
        if (num_turns, True) in df2.index:
            wins_min+=df2.at[(num_turns,True),'games']
            games_min+=df2.at[(num_turns,True),'games']
        if (num_turns, False) in df2.index:
            games_min+=df2.at[(num_turns,False),'games']
    output_df.loc[MINTURNS]=[wins_min,games_min]
    for num_turns in range(MINTURNS+1,MAXTURNS):
        games=0
        wins=0
        if (num_turns, True) in df2.index:
            wins=df2.at[(num_turns,True),'games']
            games+=wins
        if (num_turns, False) in df2.index:
            games+=df2.at[(num_turns,False),'games']
        output_df.loc[num_turns]=[wins,games]   
    games_max=0
    wins_max=0 
    for num_turns in range(MAXTURNS, res_df['turns'].max()+1):
        if (num_turns, True) in df2.index:
            wins_max+=df2.at[(num_turns,True),'games']
            games_max+=df2.at[(num_turns,True),'games']
        if (num_turns, False) in df2.index:
            games_max+=df2.at[(num_turns,False),'games']
    output_df.loc[MAXTURNS]=[wins_max,games_max]
    tempgames=output_df['games'].mask(output_df['games']==0,1) #replaces 0 with 1 to avoid dividing by 0
    output_df['win_rate']=output_df['wins']/tempgames
    total_games=output_df['games'].sum()
    output_df['game_length_rate']=output_df['games']/total_games
    return output_df.to_json()


def getCardRecordByCopies(set_abbr:str, card_name:str, arch_label='ALL', ):
    #Gets number of wins, games played, and win rate for a given card split up by number of copies of that card in the deck
    #For example "4":{"wins":859.0,"games":1414.0,"win_rate":0.6074964639}
    #card_name is case sensitive
    #intended for use on individual card pages
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


def getArchWinRatesByMulls(set_abbr:str,arch_label='ALL', as_json=True):
    #Returns win rates and number of games played on play, draw, and overall by number of mulligans taken.
    #Any game with 3 or more mulligans is grouped into num_mulligans=3.
    #If arch_label='ALL', returns cumulative records where games for all archetypes are included
    #Could be used on the archetype page, but kind of niche information
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
    
def getArchAvgSpeed(set_abbr:str, arch_label:str,):
    #Returns average speed for a given archetype. Speed is defined as the difference between average win and loss length.
    #Also includes average win/loss/game length and number of games for the purpose of sample size cutoffs
    #May be redundant with makeFormatOverviewTable and getRecordByLength depending on what information we present and where
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_stats_table=metadata.tables[set_abbr+'ArchGameStats']
    arch_id=archLabelToID(arch_label)
    q1=select(arch_stats_table.c.turns,arch_stats_table.c.won,func.sum(arch_stats_table.c.game_count).label('games')).group_by(
        arch_stats_table.c.turns,arch_stats_table.c.won).where(arch_stats_table.c.arch_id==arch_id
    ).order_by(arch_stats_table.c.turns,arch_stats_table.c.won)   
    res_df=pd.read_sql_query(q1,conn) 
    conn.close()
    if res_df.shape[0]==0: return {'average_win_length':0,'average_loss_length':0,'average_game_length':0,'wins':0,'losses':0,'games':0}
    record_df=pd.DataFrame({'wins':[],'losses':[],'games':[]})
    df2=res_df.set_index(['turns','won'],inplace=False)
    for num_turns in range(0,res_df['turns'].max()+1):
        games=0
        wins=0
        if (num_turns, True) in df2.index:
            wins=df2.at[(num_turns,True),'games']
            games+=wins
        if (num_turns, False) in df2.index:
            games+=df2.at[(num_turns,False),'games']
        losses=games-wins
        record_df.loc[num_turns]=[wins,losses,games]   
    output={}
    output['average_win_length']=(record_df['wins']*record_df.index).sum()/(max(record_df['wins'].sum(),1))
    output['average_loss_length']=(record_df['losses']*record_df.index).sum()/(max(record_df['losses'].sum(),1))
    output['average_game_length']=(record_df['games']*record_df.index).sum()/(max(record_df['games'].sum(),1))
    output['wins']=record_df['wins'].sum()
    output['losses']=record_df['losses'].sum()
    output['games']=record_df['games'].sum()
    output['speed']=output['average_win_length']-output['average_loss_length']
    return output


def getSubarchetypeDistinguishingCards(set_abbr:str, arch_label:str, n_top=10, n_bottom=10):
    #arch_label is a WUBRG string with a number at the end, e.g. "WU2" for the second subarchetype of WU.
    #Returns the top cards that distinguish this subarchetype from the overall main colors by number of copies above and below average.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    main_colors=arch_label[:-1].upper()
    num_sub_decks,sub_mean_deck=meanDecklist(conn,set_abbr,arch_label) #may be interesting to have the option to filter by num_wins here
    num_main_decks,overall_mean_deck=meanDecklist(conn,set_abbr,main_colors)
    conn.close()
    sub_mean_deck=pd.DataFrame(sub_mean_deck)
    overall_mean_deck=pd.DataFrame(overall_mean_deck)
    deck_delta=sub_mean_deck-overall_mean_deck
    deck_delta.columns=['delta']
    deck_delta.sort_values(ascending=False,by='delta',inplace=True)
    top_distinguishing_cards=deck_delta.head(n_top)
    bottom_distinguishing_cards=deck_delta.tail(n_bottom)
    return {'top_distinguishing_cards':top_distinguishing_cards.to_dict(),
            'bottom_distinguishing_cards':bottom_distinguishing_cards.to_dict()}
def getRandomSampleDecklist(set_abbr:str, arch_label:str, min_wins=0, max_wins=7, min_rank=0, max_rank=6):
    #Returns a random decklist from the database matching the given criteria.
    #May want to format the decklist for readability here or on the frontend.
    set_abbr=set_abbr.lower()
    arch_label=arch_label.upper()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)    
    deck_table=metadata.tables[set_abbr+'Decklists']
    card_table=metadata.tables[set_abbr+'CardInfo']
    s1=select(card_table.c.name)
    cardSeries=pd.read_sql_query(s1,conn)
    select_args=[]
    for card_name in cardSeries['name']:
        select_args.append(getattr(deck_table.c,card_name).label(card_name))
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
    s=s.order_by(func.random()).limit(1)
    resultDF=pd.read_sql_query(s,conn)
    conn.close()
    return resultDF.T.to_json()

def getOverperformingCards(set_abbr:str, arch_label:str, n_top=10,exclude_rares=True,as_json=True):
    #Returns the top overperforming cards for a given archetype in a set.
    set_abbr=set_abbr.lower()
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    arch_id=archLabelToID(arch_label)
    is_subarchetype=arch_label[-1:].isnumeric()
    if is_subarchetype:
        main_colors=arch_label[:-1].upper()
    else:
        main_colors=arch_label.upper()
    color_id=colorInt(main_colors)
    derived_stats_table=metadata.tables[set_abbr+'CardDerivedStats']
    s_arch=select(derived_stats_table.c.card_id,derived_stats_table.c.arch_id,
             derived_stats_table.c.games_in_hand,derived_stats_table.c.adjusted_iwd).where(
                 derived_stats_table.c.arch_id==arch_id)
    arch_derived_stats_df=pd.read_sql_query(s_arch,conn,index_col='card_id')
    s_color=select(derived_stats_table.c.card_id,derived_stats_table.c.arch_id,
             derived_stats_table.c.games_in_hand,derived_stats_table.c.adjusted_iwd).where(
                 derived_stats_table.c.arch_id==color_id)
    color_derived_stats_df=pd.read_sql_query(s_color,conn,index_col='card_id')
    s_all=select(derived_stats_table.c.card_id,derived_stats_table.c.arch_id,
             derived_stats_table.c.games_in_hand,derived_stats_table.c.adjusted_iwd).where(
                 derived_stats_table.c.arch_id==-1)
    all_derived_stats_df=pd.read_sql_query(s_all,conn,index_col='card_id')
    card_df=cardInfo(conn,set_abbr)
    conn.close()
    comparison_df=pd.DataFrame({'arch_iwd':arch_derived_stats_df['adjusted_iwd'],
                                'color_iwd':color_derived_stats_df['adjusted_iwd'],
                                'overall_iwd':all_derived_stats_df['adjusted_iwd'],
                                'arch_games_in_hand':arch_derived_stats_df['games_in_hand'],
                                'color_games_in_hand':color_derived_stats_df['games_in_hand'],
                                'overall_games_in_hand':all_derived_stats_df['games_in_hand'],
                                'card_name':card_df['name']})
    if exclude_rares:
        rares=card_df[card_df['rarity'].isin(['M','R'])].index
        comparison_df=comparison_df[~comparison_df.index.isin(rares)]
    basics=['Plains','Island','Swamp','Mountain','Forest']
    basic_ids=card_df[card_df['name'].isin(basics)].index
    comparison_df=comparison_df[~comparison_df.index.isin(basic_ids)]
    SAMPLE_SIZE_CUTOFF=500
    comparison_df=comparison_df[comparison_df['arch_games_in_hand']>=SAMPLE_SIZE_CUTOFF]
    comparison_df=comparison_df[comparison_df['color_games_in_hand']>=SAMPLE_SIZE_CUTOFF*2] 
    comparison_df=comparison_df[comparison_df['overall_games_in_hand']>=SAMPLE_SIZE_CUTOFF*3]
    comparison_df['color_delta']=comparison_df['arch_iwd']-comparison_df['color_iwd']
    comparison_df['overall_delta']=comparison_df['arch_iwd']-comparison_df['overall_iwd']
    if is_subarchetype:
        comparison_df.sort_values(by='color_delta',ascending=False,inplace=True)
    else:
        comparison_df.sort_values(by='overall_delta',ascending=False,inplace=True)
    top_overperformers=comparison_df.iloc[:,[0,3,4,5,6,7,8]].head(n_top)
    if as_json:
        return top_overperformers.to_json()
    else: return top_overperformers