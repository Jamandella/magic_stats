#Just the section of tablebuilding that defines the tables for reading convenience. Not runnable code.

class ArchRank(Base):
    __tablename__=set_abbr+"ArchRank"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    arc_num=mapped_column(SmallInteger)
    rank=mapped_column(SmallInteger)
    #lots of things are split up by both archetype and rank, this table encodes both as an int
    #name: main colors as a string (e.g. "B", "WU", "URG", etc.) (this will change after archetypes get refined)
    #arcnum: an int corresponding to the archetype
    #ranks: 0=None, 1=Bronze, 2=Silver, 3=Gold, 4=Platinum, 5=Diamond, 6=Mythic
    #id encodes both archetype and rank by id=arc_num*7+rank, so id//7=arc_num and id%7=rank
    

class CardInfo(Base):
    #The information describing each card in the set
    __tablename__=set_abbr+"CardInfo"
    id=mapped_column(SmallInteger, primary_key=True)
    name=mapped_column(String)
    mana_value=mapped_column(SmallInteger)
    color=mapped_column(SmallInteger)
    card_type=mapped_column(String)
    rarity=mapped_column(String)
    # name: Card name
    # color: 5 bit int with each bit representing presence of a color
    # W=1, U=2, B=4, R=8, G=16, so color:22=10110 means UBG, color=0 means colorless.
    # card type-A(rtifact), C(reature), E(nchantment), L(and), P(laneswalker), I(nstant), S(orcery), B(attle) 
    # multi-type cards are stored like 'AC' for 'Artifact Creature'.
    # Rarity- C(ommon), U(ncommon), R(are), M(ythic), B(asic land)

class ArchGameStats(Base):
    #For each archetype and rank and length of game (by turns), there is a row for wins and a row for losses
    #In these rows the recorded data is the number of games and the total number of cards of each mana value
    #played in decks in those games.
    #For example: in the row for arid(UB,silver), turns=8, won=True, 
    #n2_drops/game_count would correspond to the average number of 2 drops in UB decks at rank silver that won a game in 8 turns.
    #To get the average number of 2 drops in all UB decks, sum n2_drops and game_count over all rows where the arid corresponds to a UB deck
    #and divide the totals.
    #To get the win rate of arid 40 (currently WU diamond) in 6-turn games: total wins=game count from row arid=40, turns=6 won=True
    #total losses=game_count from row arid=40, turns=6, won=False
    __tablename__=set_abbr+"ArchGameStats"
    arid=mapped_column(SmallInteger, ForeignKey(set_abbr+'ArchRank.id',ondelete='CASCADE',onupdate='CASCADE'),primary_key=True) 
    won=mapped_column(Boolean, primary_key=True)
    turns=mapped_column(SmallInteger, primary_key=True) #how long the game lasted
    game_count=mapped_column(Integer) #number of games that fit the preceding criteria
    lands=mapped_column(Integer) #total number of lands in decks that played those games
    n0_drops=mapped_column(Integer) #total number of 0 mana cards in all of those games
    n1_drops=mapped_column(Integer)
    n2_drops=mapped_column(Integer) 
    n3_drops=mapped_column(Integer)
    n4_drops=mapped_column(Integer)
    n5_drops=mapped_column(Integer)
    n6_drops=mapped_column(Integer)
    n7_drops=mapped_column(Integer)
    n8p_drops=mapped_column(Integer)
    #arid: int that encodes both archetype and rank via the ArchRank table
    #won: whether this row is wins or losses
    #turns: how many turns the game lasted
    #game_count: number of games that match the previous 3 variables (e.g. number of times a WR diamond deck lost in 9 turns)
    #lands: total number of lands in deck in those games
    #nx_drops: total number of x drops in deck in those games (with n8p_drops meaning number of 8+ drops)
    

class CardGameStats(Base):
    #better structure id/arid/copies : wins/losses
    #id: card id from CardInfo table
    #arid: archetype and rank encoded as one int from ArchRank table
    #copies: number of copies in deck
    #win count: number of wins in deck
    #game count: number of games in deck
    #game played win rate=sum over copies>=1 of win_count/sum over copies>=1 of game_count
    #more stats to come
    __tablename__=set_abbr+"CardGameStats"
    id=mapped_column(SmallInteger, ForeignKey(set_abbr+'CardInfo.id',ondelete='CASCADE',onupdate='CASCADE'), primary_key=True)
    arid=mapped_column(SmallInteger, ForeignKey(set_abbr+'ArchRank.id',ondelete='CASCADE',onupdate='CASCADE'), primary_key=True)
    copies=mapped_column(SmallInteger, primary_key=True)
    win_count=mapped_column(Integer)
    game_count=mapped_column(Integer)