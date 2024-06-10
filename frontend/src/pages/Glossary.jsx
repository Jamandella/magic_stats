import React from "react";

const glossaryItems = [
  {
    term: "Game in Hand Win Rate",
    definition: "Per card, the proportion of games where it shows up that are wins."
  },
  {
    term: "Impact (general)",
    definition: "The notion of how much a given card affects the odds of winning the game."
  },
  {
    term: "Inclusion Impact",
    definition: "The difference in win % between decks running a given card and similar decks (archetypes) that don't."
  },
  {
    term: "Impact When Drawn",
    definition: "The difference in win % for decks in games in which they ever draw a given card and games where they don't."
  },
  {
    term: "Mana Curve",
    definition: "The number of cards at each mana value for a given deck."
  },
  {
    term: "Win Rate (card)",
    definition: "The total % of games won for decks that run that card."
  },
  {
    term: "Win Rate (deck)",
    definition: "The total % of games won for an archetype."
  },
]

const Glossary = () => {
  return (
    <div className="glossary">
      <div className="container flow margin-block-xl">
        <h2 className="heading-2">Glossary</h2>
        <dl className="flow margin-block-start-m">
          {glossaryItems.map(item => (
            <div className="glossary-item" key={item.term}>
              <dt className="fw-bold clr-neutral-1000">{item.term}</dt>
              <dd>{item.definition}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  );
}

export default Glossary;
