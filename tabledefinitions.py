from sqlalchemy import ForeignKey, Integer, String, Boolean, Float
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase
#Table Definitions
class Base(DeclarativeBase):
    pass
class LtrArchRank(Base):
    __tablename__="ltrArchRank"
    id=mapped_column(Integer, primary_key=True)
    name=mapped_column(String)
    rank=mapped_column(Integer)
    #Table assigns an integer to each pairing of archetype and rank, as many stats are organized by those two categories
    #name: Currently the same as "main deck colors," i.e. "BR" or "WUB," etc. colors are always in WUBRG order.
    #rank: 0=None, 1="bronze", 2="silver", ... 6="mythic"
    #id1 and id2 have the same rank iff id1%7==id2%7
    #id1 and id2 have the same name iff id1//7==id2//7
    #ToDo:would be a bit simpler to have an an int that corresponds to archetype name, so that id=nameInt*7+rank

class LtrCardInfo(Base):
    __tablename__="ltrCardInfo"
    id=mapped_column(Integer, primary_key=True)
    name=mapped_column(String)
    mana_value=mapped_column(Integer)
    color=mapped_column(String)
    card_type=mapped_column(String)
    rarity=mapped_column(String)
    # color: WUBRG+C for colorless
    # card type-A(rtifact), C(reature), E(nchantment), L(and), P(laneswalker), I(nstant), S(orcery), B(attle) 
    # lower case letters may be appended to card type to flag particular cards. currently y for land cyclers and x for x spells
    # Rarity- C(ommon), U(ncommon), R(are), M(ythic), B(asic land)

class LtrArchGameStats(Base):
    __tablename__="ltrArchGameStats"
    arid=mapped_column(Integer, ForeignKey("ltrArchRank.id"), primary_key=True)
    won=mapped_column(Boolean, primary_key=True)
    turns=mapped_column(Integer, primary_key=True) #how long the game lasted
    game_count=mapped_column(Integer) #number of games that fit the preceding criteria
    lands=mapped_column(Integer) #total number of lands in decks that played those games
    n0_drops=mapped_column(Integer) #total number of 0 mana spells
    n1_drops=mapped_column(Integer)
    n2_drops=mapped_column(Integer)
    n3_drops=mapped_column(Integer)
    n4_drops=mapped_column(Integer)
    n5_drops=mapped_column(Integer)
    n6_drops=mapped_column(Integer)
    n7_drops=mapped_column(Integer)
    n8p_drops=mapped_column(Integer) #total number of 8+ drops.
    #Table stores game related data about archetypes as a whole (as opposed to individual cards)
    #Each game in the data set is categorized by it's archetype, rank (arid=ArchetypeRankID),
    #whether the game was a win or loss, and how long the game was.
    #n1_drops is the total number of 1 drops in all decks in that category, so that n1_drops/game_count=average 1 drops.


class LtrCardGameStats(Base):
    __tablename__="ltrCardGameStats"
    id=mapped_column(Integer, ForeignKey('ltrCardInfo.id'), primary_key=True)
    arid=mapped_column(Integer, ForeignKey('ltrArchRank.id'), primary_key=True)
    won=mapped_column(Boolean, primary_key=True)
    copies=mapped_column(Integer, primary_key=True)
    game_count=mapped_column(Integer)
    #Table stores game related data that applies to an individual card
    #id: the card number from ltrCardInfo
    #arid: the number that indicates both archetype of the deck and the rank of the player from ltrArchRank
    #won: whether the game was a win or loss
    #copies: number of copies of the card that were in the deck (minimum 1)
    #game_count: the number of games that match the preceding variable values

class LtrCardDerivedStats(Base):
    #Table is not yet actually built
    __tablename__="ltrCardDerivedStats"
    id=mapped_column(Integer, ForeignKey('ltrCardInfo.id'), primary_key=True)
    arid=mapped_column(Integer, ForeignKey('ltrArchRank.id'), primary_key=True)
    aIWD=mapped_column(Float) #adjusted impact when drawn
    #Some other stats too. There are some ideas on the Figma.

