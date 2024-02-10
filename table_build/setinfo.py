from bs4 import BeautifulSoup
import requests
import json
import html
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker



#BRO bonus sheet causes an issue


def get_parsed(url):
    req = requests.get(url)
    stuff = req.text
    return BeautifulSoup(stuff,features="html.parser")

def extractCostInfo(cost:str): 
    #Will need updates for sets with unusual mana symbols (e.g. hybrid/phyrexian)
    #Card color is currently aligned with game rules meaning, but could opt for color identity instead by checking rules text
    symbols=cost.split('}')
    color_num=0
    mv=0
    for s in symbols:
        if s.startswith('{'):
            s=s[1:]
            if s.isnumeric():
                mv+=int(s)
            elif s!='X':
                mv+=1
    if "{W" in symbols:
        color_num+=1
    if "{U" in symbols:
        color_num+=2
    if "{B" in symbols:
        color_num+=4
    if "{R" in symbols:
        color_num+=8
    if "{G" in symbols:
        color_num+=16 
    return mv,color_num
def getTypes(card_type):
    #Given the full type line (e.g. 'Artifact Creature - Golem'),
    #returns list of first letters of card types (['A','C'])
    type_names=['Enchantment','Artifact','Creature','Land','Battle','Planeswalker','Instant','Sorcery']
    typeWords=card_type.split()
    typeLetters=""
    for t in typeWords:
        if t in type_names:
            typeLetters+=t[0]
    return typeLetters
def getCardNames(setName='ltr'):
    engine = create_engine("sqlite:///23spells.db", echo=False) 
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    game_data_table=metadata.tables[setName+'GameData']
    cols=game_data_table.c.keys()
    cardNames=[]
    for c in cols:
        if c[:5]=="deck_":
            cardNames.append(c[5:])
    conn.close()
    return cardNames

def scrape_scryfall(set_abbr,maxID=400):
    soup = get_parsed("https://scryfall.com/sets/"+set_abbr+"?as=checklist")
    table_start=soup.find("tbody")
    cards = table_start.findAll("tr")
    cardInfo={}
    rowNum=0
    names=getCardNames(set_abbr)
    allNames=names.copy()
    unmatched={}
    while rowNum<min(len(cards),maxID):
        card=cards[rowNum]
        fields = card.findAll("td")
        setNum=fields[1].text.strip()
        if str(setNum).isnumeric():
            setNum=int(setNum)
            title = html.unescape(fields[2].text.strip())
            cost = fields[3].text.strip()
            card_type = fields[4].text.strip()
            rarity= fields[5].text.strip()
            types=getTypes(card_type)
            mv,color=extractCostInfo(cost) 
            if 'L' in types: mv=-1 #Separating lands out from 0 drops by setting their mana value as -1
            if title in names:
                names.remove(title)
                cardInfo[setNum]={
                    "name": title,
                    "mv": mv,
                    "color": color, #stored as 5 bit int with each bit representing presence of a color
                    "type": types,
                    "rarity": rarity
                    }
            elif title not in allNames and setNum<=len(allNames):
                unmatched[setNum]={
                    "name": title,
                    "mv": mv,
                    "color": color, #stored as 5 bit int with each bit representing presence of a color
                    "type": types,
                    "rarity": rarity
                    }
                print("Card #", setNum, ":", title," has no initial match")
        rowNum+=1
    leftovers={}
    for name in names:
        alphaname=""
        nameU=name.upper()
        for i in range(len(nameU)):
            if nameU[i].isalpha() and nameU[i].isascii():
                alphaname+=nameU[i]
        leftovers[alphaname]=name
    for n in unmatched.keys():
        alphaname=""
        nameU=unmatched[n].get('name').upper()
        for i in range(len(nameU)):
            if nameU[i].isalpha() and nameU[i].isascii():
                alphaname+=nameU[i]
        if alphaname in leftovers.keys():
            print("Secondary match found: ",leftovers[alphaname]," = ",unmatched[n]['name'])
            cardInfo[n]=unmatched[n]
            cardInfo[n]['name']=leftovers[alphaname]
            leftovers.pop(alphaname)    
    if len(leftovers)>0:
        if set_abbr=='bro': #Needed for woe too
            print("Bonus sheet dude")
            #TODO: scrape set brr and match to leftover names
            
        print("WARNING! No scryfall data found matching the following cards:")
        print(leftovers)
    return cardInfo
def displaySet(set_abbr='dmu',maxID=400):
    print(
        json.dumps(
            scrape_scryfall(set_abbr=set_abbr,maxID=maxID),
            indent=4,
        )
    )