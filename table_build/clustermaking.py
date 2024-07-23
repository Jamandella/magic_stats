import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.cluster import  AgglomerativeClustering
from sklearn.decomposition import PCA


def getDeckColumnsFromGameDF(gamesDF:pd.DataFrame):
    deck_cols=[]
    for col in gamesDF.columns:
        if col[:5]=='deck_':
            deck_cols.append(col)
    return deck_cols
def makeDeckTDistanceMatrix(clusterDecks:dict):
    #Returns a matrix where the entry in position i,j is a measure of how different the deck lists in cluster i and cluster j are. 
    n_clusters=len(clusterDecks)
    deck_dist_matrix=np.zeros(shape=(n_clusters,n_clusters))
    deck_means=[]
    deck_vars=[]
    for i in range(n_clusters):
        statDF=clusterDecks[i].describe()
        deck_means.append(statDF.loc['mean'])
        deck_vars.append(statDF.loc['std']**2)
    for i in range(n_clusters):
        for j in range(i+1,n_clusters):
            deck_dist_ij=round((((deck_means[i]-deck_means[j])**2/(deck_vars[i]+deck_vars[j])).sum())**.5,3)
            deck_dist_matrix[i,j]=deck_dist_ij
            deck_dist_matrix[j,i]=deck_dist_ij
    return deck_dist_matrix

def pcaWardByDraft(gamesDF:pd.DataFrame,n_components:int, n_clusters:int):
    #Given a dataframe of games, sorts them into n_clusters groups. 
    #Does so by first grouping the games by draft and reducing decklists to n_components dimensions using PCA algorithm.
    #Then runs a clustering algorithm on the lower dimensional data.
    #Assigns a label to each game in the dataframe based on which group the corresponding draft got assigned.
    pca=PCA(n_components=n_components)
    deck_cols=getDeckColumnsFromGameDF(gamesDF)
    deck_cols.append('draft_id')
    deckDataDF=gamesDF.loc[:,deck_cols].groupby('draft_id').mean() #make one deck list per draft by averaging over the games
    transformed_data=pca.fit_transform(deckDataDF.values)
    agg = AgglomerativeClustering(n_clusters=n_clusters, linkage='ward')
    agg.fit(transformed_data)
    draftLabels=pd.DataFrame(data={'label':list(agg.labels_)},index=deckDataDF.index)
    gamesDF=gamesDF.join(other=draftLabels,on='draft_id')
    return gamesDF

def aggGrouping(distMatrix:np.ndarray,threshold:float,linkage='complete'):
    #Groups the subclusters together to form larger clusters based on the distances in distMatrix 
    #With linkage=complete, threshold can be understood as the maximum allowable distance between two subclusters in the same grouping
    agg=AgglomerativeClustering(metric='precomputed',distance_threshold=threshold,linkage=linkage,compute_full_tree=True,n_clusters=None)
    labels=agg.fit_predict(distMatrix)
    label_series=pd.Series(data=labels)
    label_series.sort_values(inplace=True)
    return label_series
    #Notes: linkage=ward, average, and complete all yield decent results, with different reasonable ranges of threshold
 
def makeClusterDecks(gamesDF:pd.DataFrame):
    #Given a dataframe of labeled games, returns a dict where the keys are the labels
    #and values are dataframes of decklists with that label.
    clusterDecks={}
    n_clusters=gamesDF['label'].max()+1
    deck_cols=getDeckColumnsFromGameDF(gamesDF)
    for i in range(n_clusters):
        clusterDF=gamesDF[gamesDF['label']==i]
        clusterDecks[i]=clusterDF.loc[:,deck_cols]
    return clusterDecks
def reclustering(deck_dist_matrix:np.ndarray,n_clusters:int):
    #Find the best grouping of subclusters into larger clusters
    #Note: This step has the most room for change and improvement. There are several other methods I would like to test out.
    distances=deck_dist_matrix.reshape(-1)
    value_list=distances.tolist()
    value_list.sort()
    value_list=[value_list[i] for i in range(n_clusters,n_clusters**2,2)] #removes 0s and duplicate values.
    median=np.median(value_list)
    #Scan through thresholds for aggGrouping near the median to look for the one that provides the desired number of groups (2-5)
    #that is the most stable, i.e. the same grouping occurs for a wide range of thresholds.
    best_run_length=0
    current_run=0
    previous=aggGrouping(distMatrix=deck_dist_matrix,threshold=median-.5)
    best_grouping=previous
    for i in range(15):
        threshold=median-.4+.1*i
        grouping=aggGrouping(distMatrix=deck_dist_matrix,threshold=threshold)
        if grouping==previous and i<14:
            current_run+=.1
        else:
            previous=grouping
            if current_run>=best_run_length:
                num_groups=grouping.max()+1
                if num_groups>=2 and num_groups<=5:
                    best_grouping=grouping
                    best_run_length=current_run
            current_run=0
    if best_grouping.max()>4: #If the above range didn't narrow things down to 5 or less archetypes, keep going
        best_run_length=0
        current_run=0
        previous=aggGrouping(distMatrix=deck_dist_matrix,threshold=median+1)
        best_grouping=previous
        for i in range(10):
            threshold=median+1+.1*i
            grouping=aggGrouping(distMatrix=deck_dist_matrix,threshold=threshold)
            if grouping==previous and i<9:
                current_run+=.1
            else:
                previous=grouping
                if current_run>=best_run_length:
                    num_groups=grouping.max()+1
                    if num_groups>=2 and num_groups<=5:
                        best_grouping=grouping
                        best_run_length=current_run
                current_run=0
    return best_grouping

def assignClusterLabels(gamesDF:pd.DataFrame):
    #Starting with a dataframe of games from game_data, group them by archetype.
    #Appends a column of labels to the dataframe that indicates each game's archetype.
    n_games=gamesDF.shape[0]
    n_clusters=min(n_games//6000,12)
    if n_clusters>1: 
        gamesDFTemp=gamesDF.copy()
        gamesDFTemp=pcaWardByDraft(gamesDF=gamesDFTemp,n_components=5,n_clusters=n_clusters) 
        #May want to vary n_components in the future. Experimentally, 5 has looked best so far.
        if n_clusters>2:
            clusterDecks=makeClusterDecks(gamesDF)
            deck_dist_matrix=makeDeckTDistanceMatrix(clusterDecks)
            grouping=reclustering(deck_dist_matrix=deck_dist_matrix,n_clusters=n_clusters) 
            final_labels=gamesDFTemp['label'].apply(lambda x: grouping.loc[x])
        else:
            final_labels=gamesDFTemp['label']
        gamesDF['label']=final_labels
    else: 
        gamesDF['label']=pd.Series(data=[0]*gamesDF.shape[0])
    return gamesDF
        

            








