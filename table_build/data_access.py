import pandas as pd
from flask import Flask, request, abort
from dbpgstrings import host, database, user, password
from sqlalchemy import create_engine
from statfunctions import *

app = Flask(__name__)
validSets=['ltr','dmu']

@app.get('/cards/<set_abbr>')
def getSetInfo(set_abbr='ltr'):
    df=cardInfo(set_abbr)
    return df.to_json()

@app.get('/<set_abbr>/<main_colors>/curve')
def getCurveStatsByColors(main_colors,set_abbr):
    #how to get params??
    if set_abbr not in validSets:
        abort(404)
    min_rank=request.args.get('min_rank',0,type=int)
    max_rank=request.args.get('max_rank',6,type=int)
    curve=getArchAvgCurve(archLabel=main_colors,minRank=min_rank,maxRank=max_rank,set_abbr=set_abbr)
    return curve.to_json()

@app.get('/<set_abbr>/<main_colors>/turns')    
def getRecordByGameLength(main_colors,set_abbr):
    if set_abbr not in validSets:
        abort(404)
    min_rank=request.args.get('min_rank',0,type=int)
    max_rank=request.args.get('max_rank',6,type=int)
    recorddf=recordByLengthDB(archLabel=main_colors,minRank=min_rank,maxRank=max_rank,set_abbr=set_abbr)
    return recorddf.to_json()

@app.get('/<set_abbr>/<main_colors>/winrate')
def getDeckWR(main_colors,set_abbr):
    minRank=request.args.get('min_rank',0,type=int)
    maxRank=request.args.get('max_rank',6,type=int)
    return getArchWinRate(archLabel=main_colors,minRank=minRank,maxRank=maxRank,set_abbr=set_abbr)

@app.get('/<set_abbr>/<main_colors>/cardStats')
def getCardStats(main_colors,set_abbr):
    #this currently combines all decks that have 1+ copies of the given card.
    minRank=request.args.get('min_rank',0,type=int)
    maxRank=request.args.get('max_rank',6,type=int)
    return getCardInDeckWinRates(archLabel=main_colors,minRank=minRank,maxRank=maxRank,set_abbr=set_abbr).to_json()

