from bs4 import BeautifulSoup
import requests
import json
import html
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from cardinfopatch import manualCardInfo


def get_parsed(url):
    req = requests.get(url)
    stuff = req.text
    return BeautifulSoup(stuff,features="html.parser")

def extractCostInfo(cost:str): 
    #Will need updates for sets with unusual mana symbols (e.g. hybrid/phyrexian)
    #Card color is currently aligned with game rules meaning, but could opt for color identity instead by checking rules text
    #For split cards, color is the union of both parts, and mana value is the minimum of the two. 
    #If one side has no mana cost (e.g. spell//land MDFCs), then the mana value is that of the other side's cost, rather than 0.
    symbols=(cost.replace('//','}')).split('}')
    color_num=0
    if "{W" in symbols or "/W" in symbols:
        color_num+=1
    if "{U" in symbols or "/U" in symbols:
        color_num+=2
    if "{B" in symbols or "/B" in symbols:
        color_num+=4
    if "{R" in symbols or "/R" in symbols:
        color_num+=8
    if "{G" in symbols or "/G" in symbols:
        color_num+=16 
    costs=cost.split('//') #Will create 2 separate costs if card is a split card. Otherwise should just be an array of the one mana cost line.
    mv=0
    for c in costs:
        this_mv=0
        these_symbols=c.split('}')
        for s in these_symbols:
            if s.startswith('{'):
                s=s[1:]
                if s.isnumeric():
                    this_mv+=int(s)
                elif s!='X':
                    this_mv+=1
        if mv==0:
            mv=this_mv
        elif this_mv>0:
            mv=min(mv,this_mv)
    return mv,color_num
def getTypes(card_type):
    #Given the full type line (e.g. 'Artifact Creature - Golem'),
    #returns list of first letters of card types (['A','C'])
    #Split cards and DFCs are union of each sides' types.
    type_names=['Enchantment','Artifact','Creature','Land','Battle','Planeswalker','Instant','Sorcery']
    type_words=card_type.split()
    type_letters=""
    for t in type_words:
        if t in type_names:
            type_letters+=t[0]
    return type_letters
def getCardNames(set_abbr):
    engine = create_engine("sqlite:///23spells.db", echo=False) 
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    game_data_table=metadata.tables[set_abbr+'GameData']
    cols=game_data_table.c.keys()
    card_names=[]
    for c in cols:
        if c[:5]=="deck_":
            card_names.append(c[5:])
    conn.close()
    return card_names

def scrape_scryfall(set_abbr, maxID=400):
    names=getCardNames(set_abbr)
    card_info=scrape_page(set_abbr,names,maxID=maxID)
    return card_info
def scrape_page(set_abbr,names,maxID=400)->dict:
    soup = get_parsed("https://scryfall.com/sets/"+set_abbr+"?as=checklist")
    table_start=soup.find("tbody")
    cards = table_start.findAll("tr")
    card_info={}
    row_num=0
    all_names=names.copy()
    unmatched={}
    card_index=0
    while row_num<min(len(cards),maxID):
        card=cards[row_num]
        fields = card.findAll("td")
        #Because of list cards, set number may not be unique within a set, only useful for ballparking boundary between real set and commander cards
        title = html.unescape(fields[2].text.strip())
        cost = fields[3].text.strip()
        card_type = fields[4].text.strip()
        rarity= fields[5].text.strip()
        types=getTypes(card_type)
        mv,color=extractCostInfo(cost) 
        if 'L' in types: mv=-1 #Separating lands out from 0 drops by setting their mana value as -1
        if title in names:
            names.remove(title)
            card_info[card_index]={
                "name": title,
                "mv": mv,
                "color": color, #stored as 5 bit int with each bit representing presence of a color
                "type": types,
                "rarity": rarity
                }
            card_index+=1
        elif title not in all_names:
            unmatched[card_index]={
                "name": title,
                "mv": mv,
                "color": color, #stored as 5 bit int with each bit representing presence of a color
                "type": types,
                "rarity": rarity
                }
            card_index+=1
            if set_abbr!='plst':
                print("Card", title," has no initial match")
        row_num+=1
    if set_abbr!='plst': 
        #If there are cards on the scryfall page that didn't get matched to card names from 17lands, 
        #check for ways in which the name might have been written differently
        #We skip this step for the scryfall list page as it has thousands of cards and we only expect a few of them to get matched.
        unmatched_keys=list(unmatched.keys()).copy()
        for n in unmatched_keys: 
            #Check if one of scryfall/17lands wrote a longer version of the name than the other
            #Implemented because for adventures, 17lands just uses the name of the main card, while scryfall treats as a split card and uses both names.
            unmatched_name=unmatched[n]["name"]
            for name in names:
                if len(unmatched_name)>len(name):
                    if unmatched_name[:len(name)]==name:
                        print("Secondary match found: ",name," = ",unmatched_name)
                        card_info[n]=unmatched[n]
                        card_info[n]['name']=name
                        names.remove(name)
                        unmatched.pop(n)   
                else:
                    if name[:len(unmatched_name)]==unmatched_name:
                        if unmatched_name[:len(name)]==name:
                            print("Secondary match found: ",name," = ",unmatched_name)
                            card_info[n]=unmatched[n]
                            card_info[n]['name']=name
                            names.remove(name)
                            unmatched.pop(n)      
        leftovers={}
        for name in names:
            alphaname=""
            nameU=name.upper()
            for i in range(len(nameU)):
                if nameU[i].isalpha() and nameU[i].isascii():
                    alphaname+=nameU[i]
            leftovers[alphaname]=name
        for n in unmatched.keys(): 
            #Check if 17lands and scryfall wrote card names slightly differently by dropping all special characters and punctuation
            alphaname=""
            nameU=unmatched[n].get('name').upper()
            for i in range(len(nameU)):
                if nameU[i].isalpha() and nameU[i].isascii():
                    alphaname+=nameU[i]
            if alphaname in leftovers.keys():
                print("Secondary match found: ",leftovers[alphaname]," = ",unmatched[n]['name'])
                card_info[n]=unmatched[n]
                card_info[n]['name']=leftovers[alphaname]
                names.remove(leftovers[alphaname])
                leftovers.pop(alphaname)    
        if len(names)>0:
            bonus_sheets={'bro':'brr','woe':'wot'}
            list_sets={'mkm'}
            if set_abbr in bonus_sheets.keys():
                max_id=max(card_info.keys())
                print("Adding bonus sheet cards to set")
                bonusInfo=scrape_page(bonus_sheets[set_abbr],names)
                id_increment=1
                for key in bonusInfo.keys():
                    card_info[max_id+id_increment]=bonusInfo[key]
                    id_increment+=1
            if set_abbr in list_sets:
                print("Adding list cards to set")
                max_id=max(card_info.keys())
                listInfo=scrape_page('plst',names,maxID=10000)
                id_increment=1
                for key in listInfo.keys():
                    card_info[max_id+id_increment]=listInfo[key]
                    id_increment+=1
        manual_info=manualCardInfo(set_abbr=set_abbr)
        if len(manual_info)>0:
            print("Adding in special case cards")
            max_id=max(card_info.keys())
            id_increment=1
            for key in manual_info.keys():
                if manual_info[key]['name'] in names:
                    card_info[max_id+id_increment]=manual_info[key]
                    names.remove(manual_info[key]['name'])
                    id_increment+=1
        if len(names)>0: print("Warning! No match found for the following cards:", *names)
    return card_info
def displaySet(set_abbr,maxID=400):
    print(
        json.dumps(
            scrape_scryfall(set_abbr=set_abbr,maxID=maxID),
            indent=4,
        )
    )

