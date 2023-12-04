import pandas as pd
import numpy as np
import random, time
import statfunctions
from sklearn.cluster import MiniBatchKMeans, DBSCAN, Birch, _birch
from sqlalchemy import MetaData, Column, ForeignKey, Integer, String, Boolean, select, Insert, create_engine, func, sql
from sqlalchemy.orm import sessionmaker
from math import sqrt
engine = create_engine("sqlite:///23spells.db", echo=False) #this will need to be something else for the web version
conn = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)

def getNonRandomDecks(set_abbr='ltr',maincolors='ALL',splash='ALL',size=1000, start=0,includeWins=False):
    game_data_table=metadata.tables[set_abbr+'GameData']
    card_info=statfunctions.cardInfo(set_abbr=set_abbr)
    cards=card_info['name'].to_list()
    cols=[getattr(game_data_table.c,'deck_'+card) for card in cards]
    if includeWins:cols.append(game_data_table.c.won)
    s=select(*cols).limit(size).distinct(game_data_table.c.draft_id).where(game_data_table.c.index>=start)
    if maincolors!='ALL':
        s=s.where(game_data_table.c.main_colors==maincolors)
    if splash!='ALL': #splash="" for no splash
        s=s.where(game_data_table.c.splash_colors==splash)
    data=pd.read_sql_query(s,conn)
    return data
def getRandomDecks(set_abbr='ltr',maincolors='ALL',splash='ALL',size=1000,includeWins=False):
    #SQLA crashes when trying to select list of 32768+ indices
    game_data_table=metadata.tables[set_abbr+'GameData']
    s=select(game_data_table.c.index).distinct(game_data_table.c.draft_id)
    if splash!='ALL': #splash="" for no splash
        s=s.where(game_data_table.c.splash_colors==splash)
    if maincolors!='ALL':
        s=s.where(game_data_table.c.main_colors==maincolors)
    res=pd.read_sql_query(s,conn)
    valid_inds=res['index'].to_list()
    if len(valid_inds)<size:
        size=len(valid_inds)
    indices=random.sample(valid_inds,size)
    batches=size//30000+1
    card_info=statfunctions.cardInfo(set_abbr=set_abbr)
    cards=card_info['name'].to_list()
    cols=[getattr(game_data_table.c,'deck_'+card) for card in cards]
    if includeWins: cols.append(game_data_table.c.won)
    s2=select(*cols).where(game_data_table.c.index.in_(indices[:30000]))
    data=pd.read_sql_query(s2,conn)
    for i in range(1,batches):
        s2=select(*cols).where(game_data_table.c.index.in_(indices[30000*i:30000*(i+1)]))
        data=pd.concat([data,pd.read_sql_query(s2,conn)],axis=0,ignore_index=True)
    print("Got {} games".format(data.shape[0]))
    return data
def extendDeckData(decks: pd.DataFrame, cyclers_are_lands=False, statWeight=1, mvsmoothing=True, basics_are_stats=True,set_abbr='ltr'):
    #.5 in each color rather than 1/#colors treats 3+ color cards weirdly, but I think it may be preferrable
    #A UB deck running the UBR Sauron has as much of a red card as one running the BR Balrog.
    #statWeight is to rescale the relative importance of the color split and mana curve vs the exact card list
    #extending DOES seem to have a small improvement on mbkmeans stability! yay!
    #no clear difference between various statWeights so far, but .5~1~2>5>0. 5 is too much. 0 is too little.
    #mvsmoothing is to make nearby mana values count as similar by splitting up an n cost card into .25(n-1)+.5n+.25(n+1).
    #This makes the switching a 1 cost card for a 2 (or 3) cost a smaller change than for a 5 cost.
    #Boundaries are split .75-.25.
    carddf=statfunctions.cardInfo(set_abbr)
    colors=['W','U','B','R','G']
    if cyclers_are_lands and set_abbr=='ltr': #delete the existence of land cyclers and treat them as corresponding basics
        cyclers=['Eagles of the North','Lórien Revealed','Troll of Khazad-dûm','Oliphaunt','Generous Ent'] 
        ids=[41, 82, 133, 161, 249]
        cycle_cols=['deck_'+name for name in cyclers]
        decks['deck_Plains']+=decks[cycle_cols[0]]
        decks['deck_Island']+=decks[cycle_cols[1]]
        decks['deck_Swamp']+=decks[cycle_cols[2]]
        decks['deck_Mountain']+=decks[cycle_cols[3]]
        decks['deck_Forest']+=decks[cycle_cols[4]]
        decks.drop(labels=cycle_cols,axis=1,inplace=True)
        carddf.drop(labels=ids,axis=0,inplace=True)
    for c in range(-1,5):
        if c==-1: #colorless case
            colordf=carddf[carddf['color']==0]
            colordf=colordf[colordf['mana_value']>=0] #remove lands (listed as mana value -1)
            cols=['deck_'+name for name in colordf['name']]
            decks['C']=decks[cols].sum(axis=1)*statWeight
        else:
            cnum=2**c
            colorplus=(carddf['color']//cnum)%2==1
            colordf=carddf.loc[colorplus]
            singlecolor=colordf[colordf['color']==cnum]['name'].to_list()
            cols=['deck_'+name for name in singlecolor]
            decks[colors[c]]=decks[cols].sum(axis=1)*statWeight
            multicolor=colordf[colordf['color']!=cnum]['name'].to_list()
            cols2=['deck_'+name for name in multicolor]
            decks[colors[c]]+=.5*decks[cols2].sum(axis=1)*statWeight
    for mv in range(carddf['mana_value'].max()+1):
        decks[mv]=np.zeros(decks.shape[0])
    for mv in range(carddf['mana_value'].max()+1):
        mvdf=carddf[carddf['mana_value']==mv]
        cols=['deck_'+name for name in mvdf['name']]
        counts=decks[cols].sum(axis=1)*statWeight
        decks[mv]+=(1-0.5*mvsmoothing)*counts
        if mvsmoothing: 
            decks[min(carddf['mana_value'].max(),mv+1)]+=.25*counts
            decks[max(0,mv-1)]+=.25*counts
    if basics_are_stats:
        decks['stat_Plains']=decks['deck_Plains']*statWeight
        decks['stat_Island']=decks['deck_Island']*statWeight
        decks['stat_Swamp']=decks['deck_Swamp']*statWeight
        decks['stat_Mountain']=decks['deck_Mountain']*statWeight
        decks['stat_Forest']=decks['deck_Forest']*statWeight
    return decks

def makeDeckList(zscores, cardCounts, zcutoff=0, qcutoff=0, show=10, incBasics=False, set_abbr='ltr'):
    #cardCounts should be a list where cardCounts[i] is an amount of the card with cardInfo.id=i
    carddf=statfunctions.cardInfo(set_abbr=set_abbr)
    basic_names=['Plains','Island','Swamp','Mountain','Forest']
    basics=[]
    for b in basic_names:
        basics.append(carddf.index[carddf['name']==b].tolist()[0]) 
    basicCounts=[round(cardCounts[i],2) for i in basics]
    deckList=pd.DataFrame({'name':[],'z-scores':[], 'quantity':[], 'color':[]})
    for i in range(carddf.shape[0]):
        if zscores[i]>zcutoff and cardCounts[i]>qcutoff and (incBasics or (i not in basics)):
            name=carddf.at[i,'name']
            color=statfunctions.colorString(carddf.at[i,'color'])
            deckList.loc[len(deckList.index)]=[name,round(zscores[i],2),round(cardCounts[i],2),color]
    deckList=deckList.sort_values('z-scores',axis=0, ascending=False)
    print(deckList.iloc[:show,:])
    print('Basics- W:{} U:{} B:{} R:{} G:{}'.format(*basicCounts))

def mbkmClusters(data, batch_size=4096, clusters=10,reassignment_ratio=.01):
    #data should be a dataframe from makeTestData
    decks=data.drop('won',axis=1)
    wins=data['won']
    mbk = MiniBatchKMeans(
        init="k-means++",
        n_clusters=clusters,
        batch_size=batch_size,
        n_init=10,
        max_no_improvement=10,
        verbose=0,
        reassignment_ratio=reassignment_ratio #higher value means more willing to discard small clusters, increasing accuracy and run time
    )
    t0 = time.time()
    decks.columns=range(decks.shape[1])
    mbk.fit(decks)
    t_mini_batch = time.time() - t0
    print("Took {} to process".format(t_mini_batch))
    return mbk,wins
def testMBKMeans(set_abbr='ltr',clusters=10,maincolors='ALL',size='20000'):
    decks=getRandomDecks(set_abbr=set_abbr,maincolors=maincolors,size=size,includeWins=True)
    print("Total wins:", decks.iloc[:,-1].sum())
    deckstats=decks.iloc[:,:-1].describe()
    mbk,wins=mbkmClusters(decks,clusters=clusters)
    win_counts=[0]*clusters
    game_counts=[0]*clusters
    for i in range(len(mbk.labels_)):
        game_counts[mbk.labels_[i]]+=1
        win_counts[mbk.labels_[i]]+=wins[i]
    print("Distinguishing characteristics:")
    for j in range(clusters):
        center=mbk.cluster_centers_[j]
        #makeDeckList(((clusters+1)*center-total)/clusters, cutoff=.4)
        z_scores=(center-deckstats.loc['mean'])/deckstats.loc['std']
        makeDeckList(z_scores,center, qcutoff=0.1,set_abbr=set_abbr)
        print("Win rate:",win_counts[j]/game_counts[j], "Game count:", game_counts[j])

#testMBKMeans()
def testClusterCounts(data,minimum=10, maximum=40):
    df=pd.DataFrame({'n':[],'mean_squared_error':[],'smallest':[],'largest:':[],'smallestcentergap':[],'mseacc':[]})
    for i in range(minimum,maximum+1):
        mbk,wins=mbkmClusters(data,clusters=i,reassignment_ratio=0.01)
        mse=mbk.inertia_/data.shape[0]
        game_counts=[0]*i
        for j in range(len(mbk.labels_)):
            game_counts[mbk.labels_[j]]+=1
        smallest=min(game_counts)
        largest=max(game_counts)
        centers=mbk.cluster_centers_
        scg=10000
        for j in range(i):
            for k in range(j):
                gap=(abs(centers[j]-centers[k]).sum())
                scg=min(scg,gap)
        df.loc[i]=[i,round(mse,3),smallest,largest,scg,0]
    for n in range(minimum+1,maximum): 
        df.loc[n,'mseacc']=df.loc[n-1,'mean_squared_error']+df.loc[n+1,'mean_squared_error']-2*df.loc[n,'mean_squared_error']
    print(df)


def checkMBKStability(decks1,decks2,clusters=20,verbose=True,nocyclers=False):
    #if only one is extended, make sure it's data2
    #mode='text' to view difference in outputs, 
    #mode=anything else to just silently compute the l2 displacement between centers
    #note- l1 displacement doesn't mean much as they are getting matched based on l2 proximity
    #l2 displacement also isn't perfect as pairings aren't 1-1
    #currently only ltr compatible
    CARDS=266
    if nocyclers: CARDS=261
    deckstats1=decks1.iloc[:,:CARDS].describe()
    mbk1,wins1=mbkmClusters(decks1,clusters=clusters,reassignment_ratio=0.01)
    print("Sample 1 inertia:{}".format(mbk1.inertia_))
    deckstats2=decks2.iloc[:,:CARDS].describe()
    mbk2,wins2=mbkmClusters(decks2,clusters=clusters,reassignment_ratio=0.01)
    cc2=mbk2.cluster_centers_[:,:mbk1.cluster_centers_.shape[1]]
    print("Sample 2 inertia:{}".format(mbk2.inertia_))
    pairing=mbk1.predict(cc2)
    win_counts1=[0]*clusters
    game_counts1=[0]*clusters
    win_counts2=[0]*clusters
    game_counts2=[0]*clusters
    delta_l1=0
    delta_l2=0
    for i in range(len(mbk1.labels_)):
        game_counts1[mbk1.labels_[i]]+=1
        win_counts1[mbk1.labels_[i]]+=wins1[i]
        game_counts2[mbk2.labels_[i]]+=1
        win_counts2[mbk2.labels_[i]]+=wins2[i]
    for j in range(clusters):
        center=mbk1.cluster_centers_[j][:CARDS]
        z_scores=(center-deckstats1.loc['mean'])/deckstats1.loc['std']
        if verbose:
            makeDeckList(z_scores,center, qcutoff=0.1)
            print("Sample 1 center:")
            print("Win rate:",win_counts1[j]/game_counts1[j], "Game count:", game_counts1[j])
            print("Nearest sample 2 center(s):")
        for i in range(clusters):
            if pairing[i]==j:
                center2=cc2[i][:CARDS]
                z_scores=(center2-deckstats2.loc['mean'])/deckstats2.loc['std']
                if verbose:
                    makeDeckList(z_scores,center2, qcutoff=0.1)
                    print("Win rate:",win_counts2[i]/game_counts2[i], "Game count:", game_counts2[i])
                l1=abs(center2-center).sum()
                l2=sqrt(((center2-center)*(center2-center)).sum())
                delta_l1+=l1
                delta_l2+=l2
                if verbose:
                    print("Center l2 distance:",l2)
                    print("Center l1 distance:",l1)
    if verbose:
        print("Total l2 displacement (sortof):",delta_l2)                
    return delta_l2
    
    
    
                
def epsilonScouting(size=10000,samplecount=10): #preliminary info for setting up DBSCAN parameters
    #epsilon =5-6 looks reasonable for undextended data
    #for extended data, varies depending on statweight
    decks=extendDeckData(getRandomDecks(size=size),statWeight=2)
    print(decks.shape)
    inds=list(random.sample(range(size),samplecount))
    sample_decks=decks.iloc[inds]
    print(sample_decks.shape)
    neighborsa=[-1]*samplecount
    neighborsb=[-1]*samplecount
    neighborsc=[-1]*samplecount
    for k in range(samplecount):
        for i in range(size):
            dist=((decks.iloc[i]-sample_decks.iloc[k])**2).sum()
            neighborsa[k]+=(dist<100)
            neighborsb[k]+=(dist<400)
            neighborsc[k]+=(dist<900)
    for k in range(samplecount):
        makeDeckList(sample_decks.iloc[k,:266],sample_decks.iloc[k,:266],qcutoff=.9,show=26)
        print("Neighbors for epsilon=10: {}, =20 {}, =30 {}".format(neighborsa[k],neighborsb[k],neighborsc[k]))

def dbscanClustering(size=10000, epsilon=10,min_samples=5):
    #Doesn't work great for this data. 
    #Any set of parameters either leaves too many points uncategorized or merges a huge clump of points together
    #decks in a subset of UBR are much more popular and thus denser, 
    #so those end up blending together before most Gx and Wx decks have found similar decks
    #Possibly a more custom metric would 
    decks=extendDeckData(getRandomDecks(size=size),statWeight=2)
    decks2=decks.copy()
    decks2.columns=range(decks2.shape[1])
    t0=time.time()
    clustering = DBSCAN(eps=epsilon, min_samples=min_samples, metric='l2').fit(decks2)
    t_dbs = time.time() - t0
    print("Took {} to process".format(t_dbs))
    decks['label']=clustering.labels_
    arc_count=decks['label'].max()+1
    print(decks[['label']].value_counts())
    for i in range(min(arc_count,5)):
        arc=decks[decks['label']==i].iloc[:,:-1]
        arcstats=arc.describe()
        asT=arcstats.T
        asT=asT[asT['mean']>.5]
        asT.sort_values('mean',inplace=True)
        print("Stats for the label {} cluster:".format(i))
        print(asT)
    outliers=decks[decks['label']==-1].shape[0]
    print("Number of outliers:",outliers)
def birchClustering(data):
    #TODO: normalize data
    cleaned=data.copy()
    cleaned.columns=range(cleaned.shape[1])
    brc=Birch(n_clusters=4, branching_factor=5, threshold=8)
    brc.fit_predict(cleaned)
    vcounts=pd.Series(brc.labels_).value_counts()
    print(vcounts)
    count=0
    seen=[]
    if len(vcounts<100):
        for center in brc.subcluster_centers_:
            lbl=brc.subcluster_labels_[count]
            if lbl not in seen:
                print("Subcluster center ",count)
                makeDeckList(center, center,qcutoff=.3)
                lbl=brc.subcluster_labels_[count]
                print("Centroid label:", lbl)
                print("Population: ",vcounts[lbl])
                seen.append(lbl)
            count+=1     
 
def showTree(root,level=0):
    print("Depth: ",level)
    for subc in root.subclusters_:
        centerCards=subc.centroid_[:266]
        print("Subcluster size:",subc.n_samples_)
        makeDeckList(centerCards,centerCards,qcutoff=.3,show=10)
        if subc.child_==None:
            print("^leaf")
        elif level<2:
            showTree(subc.child_,level=level+1)
    
        
def birchByColors(arcmin=1000,max_subclusters=8):
    maincolor_list=['WR']
    for mc in maincolor_list:
        rawdata=getNonRandomDecks(maincolors=mc,size=50000)
        total_decks=rawdata.shape[0]
        if total_decks>arcmin:
            extdata=extendDeckData(rawdata,statWeight=1)
            extdata.columns=range(extdata.shape[1])
            smallest_cluster=0
            number_tries=1
            max_subclusters=min(max_subclusters,total_decks//arcmin)
            subclusters=max_subclusters
            brc=Birch(n_clusters=max_subclusters,branching_factor=5,threshold=6)
            while smallest_cluster<arcmin and number_tries<=max_subclusters:
                brc=Birch(n_clusters=subclusters, branching_factor=5, threshold=6)
                brc.fit_predict(extdata)
                vcounts=pd.Series(brc.labels_).value_counts()
                print("Try number {} for {} with up to {} clusters:".format(number_tries,mc,subclusters))
                print(vcounts)
                smallest_cluster=vcounts.min()
                number_tries+=1
                subclusters-=1
            count=0
            merged_center=np.zeros((len(vcounts),rawdata.shape[1]))
            totals=[0]*len(vcounts)
            for center in brc.subcluster_centers_:
                lbl=brc.subcluster_labels_[count]
                totals[lbl]+=1
                merged_center[lbl,:]+=center[:rawdata.shape[1]]
                count+=1
            for i in range(len(vcounts)):
                if totals[i]>0:
                    merged_center[i,:]=merged_center[i,:]/totals[i]
                    print("Cluster {} for {}:".format(i,mc))
                    print("Population:",vcounts[i])
                    makeDeckList(merged_center[i,:],merged_center[i,:],qcutoff=.3,show=12)
            showTree(brc.root_)
            
            

        else:
            print('{} too small to split'.format(mc))        
testMBKMeans(set_abbr='dmu',clusters=20,size=50000)

conn.close()
#Better diagnostics needed
#Question- what metrics SHOULD determine whether an archetype distinction is meaningful
#1. Patterns of included cards (clustering picks this up generally)
    #a. Normalizing could do a better job of finding distinctive features
        #-Would probably want to scale down mythics (and rares?) as a few copies of a high rarity card could greatly skew normalized data
        #maybe have a cutoff by population for whether a card gets included in normalized data (maybe drop all with (1-mean)/sigma>c)
#2. Significantly different play patterns
    #a. Could look at win rates and game length for easy metrics, but could easily be similar for substantially different archs
#3. Different archetypes should get different value from some cards
    #a. Some card specific metrics beyond just number played should vary between different archetypes
    #b. Look at gpwr of cards with significant presence in a cluster, should vary between clusters by more than random noise
        #more meaningful for clustering within a color pair where almost every played card has some presence in all clusters
        #gpwr has some elements of both card strength and deck strength, but detecting a difference in either is desirable
        #iwd could even be useful. it has card strength, game speed and preferred game speed all as factors
        #and detecting a difference in any of those would be good. takes more total processing to find sample iwd though.
#4. Reproducibility. Archetype distinctions should remain similar on large enough disjoint deck samples.