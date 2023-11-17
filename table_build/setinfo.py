from bs4 import BeautifulSoup
import requests
import json
import html
import pandas
import statfunctions as sf


def get_parsed(url):
    req = requests.get(url)
    stuff = req.text
    return BeautifulSoup(stuff)

def extractCostInfo(cost:str):
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
    names=['Artifact','Creature','Enchantment','Land','Battle','Planeswalker','Instant','Sorcery']
    typeWords=card_type.split()
    typeLetters=[]
    for t in typeWords:
        if t in names:
            typeLetters.append(t[0])
    return typeLetters


def scrape_scryfall(abbr='ltr',maxID=400):
    soup = get_parsed("https://scryfall.com/sets/"+abbr+"?as=checklist")
    table_start=soup.find("tbody")
    cards = table_start.findAll("tr")
    cardInfo={}
    setNum=0
    setNum=0
    names=sf.getCardNames(abbr)
    while setNum<min(len(cards),maxID):
        card=cards[setNum]
        setNum+=1
        fields = card.findAll("td")
        title = html.unescape(fields[2].text.strip())
        cost = fields[3].text.strip()
        card_type = fields[4].text.strip()
        rarity= fields[5].text.strip()
        types=getTypes(card_type)
        mv,color=extractCostInfo(cost)
        # card_text = html.unescape(card.find("div", class_="card-text-box").text.strip())
        #stats = card.find("div", class_="card-text-stats")
        if title in names:
            names.remove(title)
            cardInfo[setNum]={
                "name": title,
                "mv": mv,
                "color": color, 
                "type": types,
                "rarity": rarity
                # "card_text": card_text,
                }

    return cardInfo


print(
    json.dumps(
        scrape_scryfall(abbr='bro'),
        indent=4,
    )
)