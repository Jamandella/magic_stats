import pandas as pd
import numpy as np
import sqlalchemy as sqla
from sqlalchemy import MetaData, Column, ForeignKey, Integer, String, Boolean, select, Insert, create_engine, func
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from math import sqrt
class Base(DeclarativeBase):
    pass
#from sqlalchemy import select, func, MetaData,create_engine
engine = create_engine("sqlite:///23spells.db", echo=False) #this will need to be something else for the web version
conn = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)
MINTURNS=5
MAXTURNS=14

def cardsSeenInDF(gamesDF: pd.DataFrame): #given a dataframe of rows from game_data, returns list of number of games with each total number of cards drawn
    cols=[]
    for key in gamesDF.keys():
        if key[:5]=='drawn' or key[:7]=='opening':
            cols.append(key)
    drawnDF=gamesDF.loc[:,cols]
    totals=drawnDF.sum(axis=1)
    distributionDF=totals.value_counts()
    return distributionDF

def cardsSeenPercentiles(distributionDF): #used to determine reasonable bucket ranges for total cards seen
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



def cardInfo(set="ltr"): #returns dict of card info for given set. -1 for lands to distinguish from 0 drops
    # 0- manavalue, <-any special treatment for x spells?
    # 1- color (C for colorless)
    # 2- card type-A, C, E, L, P, I, S, B (first letter of type)
    # 3- Rarity- C, U, R, M, B (basic land)
    if(set=="ltr"):
        #in this set, land cyclers are flagged appending y to their card type
        cardInf={
            "Andúril, Flame of the West" : [3, 'C', 'A', 'M'],
            "Aragorn, Company Leader" : [3, 'WG', 'C', 'R'],
            "Aragorn, the Uniter" : [4, 'WURG', 'C', 'M'],
            "Arwen Undómiel" : [2, 'UG', 'C', 'U'], 
            "Arwen's Gift" : [4, 'U', 'S', 'C'],
            "Arwen, Mortal Queen" : [3, 'WG', 'C', 'M'],
            "Bag End Porter" : [4, 'G', 'C', 'C'],
            "Banish from Edoras" : [5, 'W', 'S', 'C'],
            "Barad-dûr" : [-1, 'C', 'L', 'R'],
            "Barrow-Blade" : [1, 'C', 'A', 'U'],
            "Battle-Scarred Goblin" : [2, 'R', 'C', 'C'],
            "Bewitching Leechcraft" : [2, 'U', 'E', 'C'],
            "Bilbo, Retired Burglar" : [3, 'UR', 'C', 'U'],
            "Bill Ferny, Bree Swindler" : [2, 'U', 'C', 'U'],
            "Bill the Pony" : [4, 'W', 'C', 'U'],
            "Birthday Escape" : [1, 'U', 'S', 'C'],
            "Bitter Downfall" : [4, 'B', 'I', 'U'],
            "Bombadil's Song" : [2, 'G', 'I', 'C'],
            "Book of Mazarbul" : [3, 'R', 'E', 'U'],
            "Borne Upon a Wind" : [2, 'U', 'I', 'R'],
            "Boromir, Warden of the Tower" : [3, 'W', 'C', 'R'],
            "Brandywine Farmer" : [3, 'G', 'C', 'C'],
            "Breaking of the Fellowship" : [2, 'R', 'S', 'C'],
            "Butterbur, Bree Innkeeper" : [4, 'WG', 'C', 'U'],
            "Call of the Ring" : [2, 'B', 'E', 'R'],
            "Captain of Umbar" : [3, 'U', 'C', 'C'],
            "Cast into the Fire" : [2, 'R', 'I', 'C'],
            "Celeborn the Wise" : [4, 'G', 'C', 'U'],
            "Chance-Met Elves" : [3, 'G', 'C', 'C'],
            "Cirith Ungol Patrol" : [5, 'B', 'C', 'C'],
            "Claim the Precious" : [3, 'B', 'S', 'C'],
            "Council's Deliberation" : [2, 'U', 'I', 'U'],
            "Deceive the Messenger" : [1, 'U', 'I', 'C'],
            "Delighted Halfling" : [1, 'G', 'C', 'R'],
            "Denethor, Ruling Steward" : [3, 'WB', 'C', 'U'],
            "Display of Power" : [3, 'R', 'I', 'R'],
            "Doors of Durin" : [5, 'RG', 'A', 'R'],
            "Dreadful as the Storm" : [3, 'U', 'I', 'C'],
            "Dunland Crebain" : [3, 'B', 'C', 'C'],
            "Dúnedain Blade" : [2, 'W', 'A', 'C'],
            "Dúnedain Rangers" : [4, 'G', 'C', 'U'],
            "Eagles of the North" : [6, 'W', 'Cy', 'C'], #land cycler
            "East-Mark Cavalier" : [2, 'W', 'C', 'C'],
            "Easterling Vanguard" : [2, 'B', 'C', 'C'], 
            "Eastfarthing Farmer" : [3, 'W', 'C', 'C'],
            "Elrond, Lord of Rivendell" : [3, 'U', 'C', 'U'],
            "Elrond, Master of Healing" : [4, 'UG', 'C', 'R'],
            "Elven Chorus" : [4, 'G', 'E', 'R'],
            "Elven Farsight" : [1, 'G', 'S', 'C'],
            "Enraged Huorn" : [5, 'G', 'C', 'C'], 
            "Ent's Fury" : [2, 'G', 'S', 'C'],
            "Ent-Draught Basin" : [2, 'C', 'A', 'U'],
            "Entish Restoration" : [3, 'G', 'I', 'U'],
            "Erebor Flamesmith" : [2, 'R', 'C', 'C'],
            "Erkenbrand, Lord of Westfold" : [4, 'R', 'U', 'C'],
            "Errand-Rider of Gondor" : [3, 'W', 'C', 'C'],
            "Escape from Orthanc" : [1, 'W', 'I', 'C'],
            "Esquire of the King" : [1, 'W', 'C', 'C'],
            "Fall of Cair Andros" : [3, 'R', 'E', 'R'],
            "Fall of Gil-galad" : [2, 'G', 'E', 'R'],
            "Fangorn, Tree Shepherd" : [7, 'G', 'C', 'R'],
            "Faramir, Field Commander" : [4, 'W', 'C', 'U'],
            "Faramir, Prince of Ithilien" : [4, 'WU', 'C', 'R'],
            "Fear, Fire, Foes!" : [1, 'R', 'S', 'U'], #* x spell
            "Fiery Inscription" : [3, 'R', 'E', 'U'],
            "Fire of Orthanc" : [4, 'R', 'S', 'C'],
            "Flame of Anor" : [3, 'UR', 'I', 'R'],
            "Flowering of the White Tree" : [2, 'W', 'E', 'R'],
            "Fog on the Barrow-Downs" : [3, 'W', 'E', 'C'],
            "Foray of Orcs" : [4, 'R', 'S', 'U'],
            "Forest" : [-1, 'C', 'L', 'B'],
            "Forge Anew" : [3, 'W', 'E', 'R'],
            "Friendly Rivalry" : [2, 'RG', 'I', 'U'],
            "Frodo Baggins" : [2, 'GW', 'C', 'U'],
            "Frodo, Sauron's Bane" : [1, 'W', 'C', 'R'], #sort of WB but not actually
            "Galadhrim Bow" : [3, 'G', 'A', 'C'],
            "Galadhrim Guide" : [4, 'G', 'C', 'C'],
            "Galadriel of Lothlórien" : [3, 'UG','C','R'],
            "Gandalf the Grey" : [5, 'UR','C','R'],
            "Gandalf the White" : [5, 'W','C','M'],
            "Gandalf's Sanction" : [3, 'UR','S','U'],
            "Gandalf, Friend of the Shire" : [4,'U','C','U'],
            "Generous Ent" : [6, 'G', 'Cy', 'C'],#land cycler
            "Gift of Strands" : [4, 'G','E','U'],
            "Gimli's Axe" : [3,'R','A','C'],
            "Gimli's Fury" : [2,'R','I','C'],
            "Gimli, Counter of Kills" : [4,'R','C','U'],
            "Gimli, Mournful Avenger" : [3,'RG','C','R'],
            "Glamdring" : [2,'C','A','M'],
            "Glorfindel, Dauntless Rescuer" : [3,'G','C','U'],
            "Glorious Gale" : [2,'U','I','C'],
            "Glóin, Dwarf Emissary" : [3,'R','C','R'],
            "Goblin Fireleaper" : [2,'R','C','U'],
            "Goldberry, River-Daughter" : [2,'U','C','R'],
            "Gollum's Bite" : [1,'B','I','U'],
            "Gollum, Patient Plotter" : [2, 'B','C','U'],
            "Gorbag of Minas Morgul" : [2,'B','C','U'],
            "Gothmog, Morgul Lieutenant" : [4,'B','C','U'],
            "Great Hall of the Citadel" : [-1,'C','L','C'],
            "Grey Havens Navigator" : [3,'U','C','C'],
            "Grishnákh, Brash Instigator" : [3,'R','C','U'],
            "Grond, the Gatebreaker" : [4,'B','A','U'],
            "Gríma Wormtongue" : [3,'B','C','U'],
            "Gwaihir the Windlord" : [6,'WU','C','U'], 
            "Haradrim Spearmaster" : [3,'R','C','C'],
            "Haunt of the Dead Marshes" : [1,'B','C','C'],
            "Hew the Entwood" : [5,'R','S','M'],
            "Hithlain Knots" : [2,'U','I','C'],
            "Hobbit's Sting" : [2,'W','I','C'],
            "Horn of Gondor" : [3,'C','A','R'],
            "Horn of the Mark" : [2,'C','A','R'],
            "Horses of the Bruinen" : [5,'U','S','U'],
            "Improvised Club" : [2,'R','I','C'],
            "Inherited Envelope" : [3,'C','A','C'],
            "Ioreth of the Healing House" : [3,'U','C','U'],
            "Isildur's Fateful Strike" : [4,'B','I','R'],
            "Island" : [-1,'C','L','B'],
            "Isolation at Orthanc" : [4,'U','I','C'],
            "Ithilien Kingfisher" : [3,'U','C','C'],
            "King of the Oathbreakers" : [4,'WB','C','R'],
            "Knights of Dol Amroth" : [4,'U','C','C'],
            "Landroval, Horizon Witness" : [5,'W','C','U'],
            "Lash of the Balrog" : [1,'B','S','C'],
            "Last March of the Ents" : [8,'G','S','M'],
            "Legolas, Counter of Kills" : [4,'UG','C','U'],
            "Legolas, Master Archer" : [3,'G','C','R'],
            "Lembas" : [2,'C','A','C'],
            "Lobelia Sackville-Baggins" : [3,'B','C','R'],
            "Long List of the Ents" : [1,'G','E','U'],
            "Lost Isle Calling" : [2,'U','E','R'],
            "Lost to Legend" : [2, 'W','I','U'],
            "Lothlórien Lookout" : [2,'G','C','C'],
            "Lotho, Corrupt Shirriff" : [2,'WB','C','R'],
            "Lórien Revealed" : [5,'U','S','C'], #land cycler
            "Many Partings" : [1,'G','S','C'],
            "March from the Black Gate" : [2,'B','E','U'],
            "Mauhúr, Uruk-hai Captain" : [2,'BR','C','U'],
            "Meneldor, Swift Savior" : [4,'U','C','U'],
            "Meriadoc Brandybuck" : [2,'G','C','U'],
            "Merry, Esquire of Rohan" : [2,'WR','C','R'],
            "Minas Tirith" : [-1,'C','L','R'], 
            "Mines of Moria" : [-1,'C','L','R'],
            "Mirkwood Bats" : [4,'B','C','C'],
            "Mirkwood Spider" : [1,'G','C','C'],
            "Mirror of Galadriel" : [2,'C','A','U'],
            "Mirrormere Guardian" : [3,'G','C','C'],
            "Mithril Coat" : [3,'C','A','R'],
            "Mordor Muster" : [2,'B','S','C'],
            "Mordor Trebuchet" : [3,'B','AC','C'],
            "Morgul-Knife Wound" : [2,'B','E','C'],
            "Moria Marauder" : [2,'R','C','R'],
            "Mount Doom" : [-1,'C','L','R'],
            "Mountain" : [-1,'C','L','B'],
            "Mushroom Watchdogs" : [2,'G','C','C'],
            "Nasty End" : [2,'B','I','C'],
            "Nazgûl" : [3,'B','C','U'],
            "Nimble Hobbit" : [2,'W','C','C'],
            "Nimrodel Watcher" : [2,'U','C','C'],
            "Now for Wrath, Now for Ruin!" : [4,'W','S','C'],
            "Oath of the Grey Host" : [4,'B','E','U'],
            "Old Man Willow" : [4,'BG','C','U'],
            "Oliphaunt" : [6,'R','Cy','C'],#land cycler
            "Olog-hai Crusher" : [4,'R','C','C'],
            "One Ring to Rule Them All" : [4,'B','E','R'],
            "Orcish Bowmasters" : [2,'B','C','R'],
            "Orcish Medicine" : [2,'B','I','C'],
            "Palantír of Orthanc" : [3,'C','A','R'],
            "Pelargir Survivor" : [2,'U','C','C'],
            "Peregrin Took" : [3,'G','C','U'],
            "Phial of Galadriel" : [3,'C','A','R'],
            "Pippin's Bravery" : [1,'G','I','C'],
            "Pippin, Guard of the Citadel" : [2,'WU','C','R'],
            "Plains" : [-1,'C','L','B'],
            "Press the Enemy" : [4,'U','I','R'],
            "Prince Imrahil the Fair" : [2,'WU','C','U'],
            "Protector of Gondor" : [4,'W','C','C'],
            "Quarrel's End" : [3,'R','S','C'],
            "Quickbeam, Upstart Ent" : [6,'G','C','U'],
            "Radagast the Brown" : [4,'G','C','M'],
            "Rally at the Hornburg" : [2,'R','S','C'],
            "Ranger's Firebrand" : [1,'R','S','U'],
            "Rangers of Ithilien" : [4,'U','C','R'],
            "Relentless Rohirrim" : [4,'R','C','C'],
            "Reprieve" : [2,'W','I','U'],
            "Revive the Shire" : [2,'G','S','C'],
            "Ringsight" : [3,'UB','S','U'],
            "Rise of the Witch-king" : [4,'BG','S','U'],
            "Rising of the Day" : [3,'R','E','U'],
            "Rivendell" : [-1,'C','L','R'],
            "Rohirrim Lancer" : [1,'R','C','C'],
            "Rosie Cotton of South Lane" : [3,'W','C','U'],
            "Rush the Room" : [1,'R','I','C'],
            "Sam's Desperate Rescue" : [1,'B','S','C'],
            "Samwise Gamgee" : [2,'WG','C','R'],
            "Samwise the Stouthearted" : [2,'W','C','U'],
            "Saruman of Many Colors" : [6,'WUB','C','M'],
            "Saruman the White" : [5,'U','C','U'],
            "Saruman's Trickery" : [3,'U','I','U'],
            "Sauron's Ransom" : [3,'UB','I','R'],
            "Sauron, the Dark Lord" : [6,'UBR','C','M'],
            "Sauron, the Necromancer" : [5,'B','C','R'],
            "Scroll of Isildur" : [3,'U','E','R'],
            "Second Breakfast" : [3,'W','I','C'],
            "Shadow Summoning" : [2,'WB','S','U'],
            "Shadow of the Enemy" : [6,'B','S','M'],
            "Shadowfax, Lord of Horses" : [5,'WR','C','U'],
            "Shagrat, Loot Bearer" : [4,'BR','C','R'],
            "Sharkey, Tyrant of the Shire" : [4,'UB','C','R'],
            "Shelob's Ambush" : [1,'B','I','C'],
            "Shelob, Child of Ungoliant" : [6,'BG','C','R'],
            "Shire Scarecrow" : [2,'C','AC','C'],
            "Shire Shirriff" : [2,'W','C','U'],
            "Shire Terrace" : [-1,'C','L','C'],
            "Shortcut to Mushrooms" : [2,'G','E','U'],
            "Shower of Arrows" : [3,'G','I','C'],
            "Slip On the Ring" : [2,'W','I','C'],
            "Smite the Deathless" : [2,'R','I','C'],
            "Sméagol, Helpful Guide" : [3,'BG','C','R'],
            "Snarling Warg" : [4,'B','C','C'],
            "Soldier of the Grey Host" : [4,'W','C','C'],
            "Soothing of Sméagol" : [2,'U','I','C'],
            "Spiteful Banditry" : [2, 'R', 'E','M'],#x spell
            "Stalwarts of Osgiliath" : [5,'W','C','C'],
            "Stern Scolding" : [1,'W','I','U'],
            "Stew the Coneys" : [3,'G','I','U'],
            "Sting, the Glinting Dagger" : [2,'C','A','R'],
            "Stone of Erech" : [1,'C','A','U'],
            "Storm of Saruman" : [6,'U','E','M'],
            "Strider, Ranger of the North" : [4,'RG','C','U'],
            "Surrounded by Orcs" : [4,'U','S','C'],
            "Swamp" : [-1,'C','L','B'],
            "Swarming of Moria" : [3,'R','S','C'],
            "Tale of Tinúviel" : [5,'W','E','U'],
            "The Balrog, Durin's Bane" : [7,'BR','C','R'],
            "The Bath Song" : [4,'U','E','U'],
            "The Battle of Bywater" :[3,'W','S','R'],
            "The Black Breath" : [3,'B','S','C'],
            "The Grey Havens" : [-1,'C','L','U'],
            "The Mouth of Sauron" : [5,'UB','C','U'],
            "The One Ring" : [4,'C','A','M'],
            "The Ring Goes South" : [4,'G','S','R'],
            "The Shire" : [-1,'C','L','R'],
            "The Torment of Gollum" : [4,'B','S','C'],
            "The Watcher in the Water" : [5,'U','C','M'],
            "There and Back Again" : [5,'R','E','R'],
            "Théoden, King of Rohan" : [3,'WR','C','U'],
            "Tom Bombadil" : [5,'WUBRG','C','M'],
            "Took Reaper" : [2,'W','C','C'],
            "Treason of Isengard" : [3,'U','S','C'],
            "Troll of Khazad-dûm" : [6,'B','Cy','C'], #land cycler
            "Uglúk of the White Hand" : [4,'BR','C','U'],
            "Uruk-hai Berserker" : [3,'B','C','C'],
            "Voracious Fell Beast" : [6,'B','C','U'],
            "War of the Last Alliance" : [4,'W','E','R'],
            "Warbeast of Gorgoroth" : [5,'R','C','C'],
            "Westfold Rider" : [2,'W','C','C'],
            "Willow-Wind" : [5,'U','C','C'],
            "Witch-king of Angmar" : [5,'B','C','M'],
            "Wizard's Rockets" : [1,'C','A','C'],
            "Wose Pathfinder" : [2,'G','C','C'],
            "You Cannot Pass!" : [1,'W','I','U'],
            "Éomer of the Riddermark" : [5,'R','C','U'],
            "Éomer, Marshal of Rohan" : [4,'R','C','R'],
            "Éowyn, Fearless Knight" : [4,'WR','C','R'],
            "Éowyn, Lady of Rohan" : [3,'W','C','U'],
            "Dawn of a New Age" : [2,'W','E','M']
        }
        return cardInf
    else:
        raise RuntimeError("Tried to get mana values from invalid set")
    
def getCardsWithMV(mv, set="ltr"): #returns a list of all card names from a given set with a given mana value (mv)
    #would be more efficient to have this hard-coded like the card->mv function, but probably not a big deal
    #all cards with 8+ mv get sorted into the same bucket
    cardMV=cardInfo(set) 
    cards=[]
    for card in cardMV.keys():
        n=cardMV.get(card)[0]
        if n==mv or (mv==8 and n>=8):
            cards.append(card)
    return cards



def getGameDataFrame(archLabel, minRank=0, maxRank=6): 
    #returns the data gamedata rows of all games fitting the given criteria as a dataframe
    #trade off of using too much memory vs reading the raw data too many times which is slow
    game_data_table=metadata.tables['game_data']
    ranks=[None,'bronze','silver','gold','platinum','diamond','mythic']
    ranks=ranks[minRank:maxRank+1]
    q=select(game_data_table).where(game_data_table.c.main_colors==archLabel,
                                        game_data_table.c.rank.in_(ranks))
    df=pd.read_sql_query(q,conn)

    return df

def countCurve(gamesDF):
    #given a dataframe of games, returns total number of cards of each MV in those games
    #returns in the form [0 drops, ..., 8+ drops, lands]
    curve=[0]*10
    for m in range(-1,9):
        cards=getCardsWithMV(m)
        mv=m%10
        for card in cards:
            c='deck_'+card
            curve[mv]+=int(gamesDF[c].sum())
    return curve

def getArchAvgCurve(archLabel, minRank=0, maxRank=6):
    #returns mean values of lands and each n drop for given archetype
    archStats_table=metadata.tables['ltrArchGameStats']
    arid_table=metadata.tables['ltrArchRank']
    q=select(archStats_table).join(arid_table, archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel, 
                                         arid_table.c.rank>=minRank, arid_table.c.rank<=maxRank)
    df=pd.read_sql_query(q,conn)
    dfTotal=df.iloc[0:,3:].sum() 
    if dfTotal['game_count']!=0:
        n=dfTotal['game_count']
        avgs=dfTotal.iloc[1:]/n
        return avgs
    else:
        return dfTotal.iloc[1:] #should be all 0s as this is the 'no games meet these conditions' case

def getArchWinRate(archLabel, minRank=0, maxRank=6, set='ltr'):
    archStats_table=metadata.tables['ltrArchGameStats']
    arid_table=metadata.tables['ltrArchRank']
    q1=select(func.sum(archStats_table.c.game_count).label('games')).join(arid_table,archStats_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel,
                                                                                    arid_table.c.rank>=minRank,
                                                                                    arid_table.c.rank<=maxRank)                                                                                 
    games_played=pd.read_sql_query(q1,conn).at[0,'games']
    q1=q1.where(archStats_table.c.won==True)
    wins=pd.read_sql_query(q1,conn).at[0,'games']
    if games_played==0: return 0
    else: return wins/games_played

def getCardInDeckWinRate(cardID, archLabel='ALL', minCopies=1, maxCopies=40, minRank=0, maxRank=6, set='ltr'):
    cg_table=metadata.tables['ltrCardGameStats']
    arid_table=metadata.tables['ltrArchRank']
    q1=sqla.select(func.sum(cg_table.c.game_count).label("wins")).join(arid_table, cg_table.c.arid==arid_table.c.id).where(
                                                                            cg_table.c.id==cardID, cg_table.c.won==True, 
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies,
                                                                            arid_table.c.rank>=minRank,arid_table.c.rank<=maxRank)
    q2=sqla.select(sqla.func.sum(cg_table.c.game_count).label("games")).join(arid_table, cg_table.c.arid==arid_table.c.id).where(
                                                                            cg_table.c.id==cardID, arid_table.c.rank>=minRank,
                                                                            cg_table.c.copies>=minCopies,
                                                                            cg_table.c.copies<=maxCopies,
                                                                            arid_table.c.rank<=maxRank)
    if archLabel!='ALL':
        q1=q1.where(arid_table.c.name==archLabel)
        q2=q2.where(arid_table.c.name==archLabel)

    wins=pd.read_sql_query(q1,conn).at[0,'wins']
    games=pd.read_sql_query(q2,conn).at[0,'games']
    if games==0: return 0
    else: return wins/games
def getDFWinRate(df): #returns win rate of data frame with a game count and a win/loss column, i.e. subset of CGStats or ArcStats
    games=df['game_count'].sum()
    wins=df[[df['won']==True]]['game_count'].sum()
    if games==0: return 0
    else: return wins/games

def recordByTurn(df, card_name): #df should be dataframe of games with given card in the deck
    deck_card="deck_"+card_name
    drawn_card="drawn_"+card_name
    dfw=df[df['won']==True]

def meanGameLength(archLabel, minRank=0, maxRank=6, won=-1): 
    ag_table=metadata.tables['ltrArchGameStats']
    arid_table=metadata.tables['ltrArchRank']
    #use won=0 to only count losses, won=1 to only count wins, archLabel='any' to include all archetypes
    q1=sqla.select(sqla.func.sum(ag_table.c.game_count).label('games'),
        sqla.func.sum(ag_table.c.game_count * ag_table.c.turns).label('turns')).join(
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

def gameLengthDistDB(archLabel, minRank=0, maxRank=6):
    #given archetype and range of ranks, returns series with game lengths as indices and proportion of games of that length as values
    #game lengths <=5 turns and >=14 turns are grouped together 
    ag_table=metadata.tables['ltrArchGameStats']
    arid_table=metadata.tables['ltrArchRank']
    q=sqla.select(ag_table.c.turns,ag_table.c.game_count).join(arid_table, ag_table.c.arid==arid_table.c.id).where(arid_table.c.name==archLabel, 
                                         arid_table.c.rank>=minRank, arid_table.c.rank<=maxRank)
    df=pd.read_sql_query(q,conn)
    counts=df[['turns','game_count']].groupby('turns').sum()
    total=df['game_count'].sum()
    if total==0:
        print("Insufficient data")
        return pd.Series([0]*(MAXTURNS-MINTURNS+1), index=range(MINTURNS,MAXTURNS+1))        
    counts.loc[MINTURNS]=counts.loc[:MINTURNS].sum()
    counts.loc[MAXTURNS]=counts.loc[MAXTURNS:].sum()
    counts=counts.loc[MINTURNS:MAXTURNS]/total
    return counts

def winRatesByTurnDF(df):
    games=df['num_turns'].value_counts().sort_index()
    games.loc[MINTURNS]=games.loc[:MINTURNS].sum()
    games.loc[MAXTURNS]=games.loc[MAXTURNS:].sum()
    games=games.loc[MINTURNS:MAXTURNS]
    wins=df[df['won']==True]['num_turns'].value_counts().sort_index()
    wins.loc[MINTURNS]=wins.loc[:MINTURNS].sum()
    wins.loc[MAXTURNS]=wins.loc[MAXTURNS:].sum()
    wins=wins.loc[MINTURNS:MAXTURNS]
    #if there are no games in one of the turn 5 to 14 buckets, then there isn't enough data
    if 0 in games.values: 
        print("Insufficient data")
        return pd.Series([0]*(MAXTURNS-MINTURNS+1), index=range(MINTURNS,MAXTURNS+1))
    else: return wins/games 

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
    gameCount=df.shape[0]
    zeros=pd.Series([0]*60, index=range(0,60))
    records=df[['num_turns','won']].value_counts().sort_index()
    games2=records.values.sum()
    wins=(zeros+records[:,1]).replace({np.nan:0}).astype('int')
    winCount=wins.sum()
    losses=(zeros+records[:,0]).replace({np.nan:0}).astype('int')
    lossCount=losses.sum()
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

def getAIWD(df, card_name):
    #df should already be filtered to be the relevant archetype
    card_table=metadata.tables['ltrCardInfo']
    cardq=select(card_table.c.name).where(card_table.c.id==cardID)
    card_name=pd.read_sql_query(cardq,conn).at[0,0]
    
    
def winRate(df):
    #df should be a game dataframe
    if df.shape[0]==0: return 0
    else: return df[df['won']==True].shape[0]/df.shape[0]

    


#testing




          