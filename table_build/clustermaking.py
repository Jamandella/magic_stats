import pandas as pd
from sklearn.cluster import SpectralCoclustering
from statfunctions import cardsInHand
from stataccess import getCardInfo

MIN_CLUSTER_SIZE=300
def findLands(set_abbr):
    card_df=getCardInfo(set_abbr=set_abbr,as_json=False)
    filter=['L' in card_df['card_type'].iloc[n] for n in range(card_df.shape[0])]
    land_names=card_df.loc[filter]['name'].to_list()
    return land_names

def getDeckColumnsFromGameDF(set_abbr, gamesDF, include_names=False,ignore_lands=False):
    card_names=[]
    deck_cols=[]
    if ignore_lands:
        land_names=findLands(set_abbr)
        land_cols=['deck_'+name for name in land_names]
        for col in gamesDF.columns:
            if col[:5]=='deck_' and (col not in land_cols):
                deck_cols.append(col)
                card_names.append(col[5:])
    else:
        for col in gamesDF.columns:
            if col[:5]=='deck_':
                deck_cols.append(col)
                card_names.append(col[5:])
    if include_names:
        return card_names,deck_cols
    else: return deck_cols

def makeMultipleCoclusterings(set_abbr,game_df:pd.DataFrame, num_runs_per:int,max_clusters:int):
    """Returns a dict of coclustering objects where the keys are the run numbers."""
    deck_cols_without_lands=getDeckColumnsFromGameDF(set_abbr=set_abbr,gamesDF=game_df,ignore_lands=True)
    deck_data=game_df.loc[:,deck_cols_without_lands]
    deck_data=deck_data.loc[deck_data.sum(axis=1)>0]
    deck_data=deck_data.loc[:,deck_data.sum(axis=0)>0]
    labels={}
    for n_clusters in range(2,max_clusters):
        for n in range(num_runs_per):
            index= (n_clusters-2)*num_runs_per+n
            coclustering=SpectralCoclustering(n_clusters=n_clusters)
            coclustering.fit(deck_data.values)
            labels[index]=coclustering.row_labels_
    max_true_clusters=0
    for n in range(num_runs_per):
        index=(max_clusters-2)*num_runs_per+n
        coclustering=SpectralCoclustering(n_clusters=max_clusters)
        coclustering.fit(deck_data.values)
        labels[index]=coclustering.row_labels_
        cluster_sizes=pd.Series(coclustering.row_labels_).value_counts()
        n_true_clusters=(cluster_sizes>=MIN_CLUSTER_SIZE).sum()
        if n_true_clusters>max_true_clusters:
            max_true_clusters=n_true_clusters
    extra_clusters=1
    index=(max_clusters-1)*num_runs_per
    while max_true_clusters<max_clusters and extra_clusters<=3:
    #If the maximum number of "true" clusters (clusters with that have at least 300 members) is less than max_clusters,
    #try running coclustering with an increased number of clusters.
        for n in range(num_runs_per):
            coclustering=SpectralCoclustering(n_clusters=max_clusters+extra_clusters)
            coclustering.fit(deck_data.values)
            cluster_sizes=pd.Series(coclustering.row_labels_).value_counts()
            n_true_clusters=(cluster_sizes>=MIN_CLUSTER_SIZE).sum()
            if n_true_clusters>max_true_clusters:
                max_true_clusters=n_true_clusters
            if n_true_clusters<=max_clusters: #Only keep runs with true clusters less than or equal to max_clusters.
                labels[index]=coclustering.row_labels_
                index+=1
        extra_clusters+=1
    return labels

def chi2WinsInHand2(wins_in_hand:pd.DataFrame,game_df:pd.DataFrame,games_in_hand:pd.DataFrame,labels:pd.Series,min_games=300):
    #compares number of wins in hand against expected wins in hand for each card in each label.
    #expected wins in hand is based on total number of wins per label and number of games in hand per card in each label.
    basics=['Plains','Island','Swamp','Mountain','Forest'] #Exclude basic lands from the analysis. May want to consider excluding all lands.
    wih=wins_in_hand.copy()
    gih=games_in_hand.copy()
    for b in basics:
        if b in wih.columns: wih.drop(columns=b,inplace=True)
        if b in gih.columns: gih.drop(columns=b,inplace=True)
    total_gih=gih.sum(axis=0)
    card_filter=total_gih.index[total_gih>min_games]
    gih=gih[card_filter]
    wih=wih[card_filter]
    temp_game_df=game_df.copy()
    temp_game_df['label']=labels
    win_rates=temp_game_df.groupby('label')['won'].mean()
    weights=gih.T*win_rates
    weights=weights.T/(weights.sum(axis=1).T)
    total_wih=wih.sum(axis=0)
    expected_wih=total_wih*weights
    chi2_components=(((wih-expected_wih)**2)/(expected_wih.mask(expected_wih==0,1))).sum(axis=0)
    chi2=chi2_components.sum()
    n_clusters=win_rates.shape[0]
    score=chi2/n_clusters
    return score
def makeHandStatsByLabel(hand_df:pd.DataFrame,games_df:pd.DataFrame,labels):
    #Finds the number of games in hand and wins in hand of each card for each label.
    hand_df=hand_df.gt(0)
    hand_df['label']=labels
    games_df['label']=labels
    hand_win_df=hand_df[games_df['won']==1]
    games_in_hand=hand_df.groupby('label').sum()
    wins_in_hand=hand_win_df.groupby('label').sum()
    return games_in_hand,wins_in_hand
def findBestRun(game_df:pd.DataFrame,labels:dict,num_runs_per:int):
    best_run=-1
    best_score=-1
    hand_df=cardsInHand(game_df)
    for i in labels.keys():
        gih,wih=makeHandStatsByLabel(hand_df=hand_df,games_df=game_df,labels=labels[i])
        score=chi2WinsInHand2(wins_in_hand=wih,games_in_hand=gih,game_df=game_df,labels=labels[i])
        if score>best_score:
            best_score=score
            best_run=i
    return best_run
def assignClusterLabels(set_abbr,gamesDF:pd.DataFrame):
    #Starting with a dataframe of games from game_data, group them by archetype.
    #Appends a column of labels to the dataframe that indicates each game's archetype.
    #Runs coclustering multiple times, varying the number of clusters.
    #Identifies the best run based on chi2 of wins in hand, which is intended to measure
    #how significantly the relative value of cards differs between archetypes.
    n_games=gamesDF.shape[0]
    max_n_clusters=min(n_games//1000,5)
    if max_n_clusters>1: 
        gamesDFTemp=gamesDF.copy()
        labels=makeMultipleCoclusterings(set_abbr=set_abbr,game_df=gamesDFTemp,num_runs_per=8,max_clusters=max_n_clusters)
        best_run=findBestRun(game_df=gamesDFTemp,labels=labels,num_runs_per=8)
        best_labels=labels[best_run]
        cluster_sizes=pd.Series(best_labels).value_counts().sort_index()
        for n in cluster_sizes.index:
            if cluster_sizes[n]<MIN_CLUSTER_SIZE:
                best_labels=pd.Series(best_labels).replace(n,-1)
        gamesDF['label']=best_labels
    else: 
        gamesDF['label']=pd.Series(data=[-1]*gamesDF.shape[0])
    return gamesDF
        

            








