const SetCard = ({ set }) => {
  const imgUrl = new URL(`../assets/sets/${set.img}`, import.meta.url).href

  return (
    <div className="set-card | box-shadow-3">
      <img className="set-card__image" src={imgUrl} alt={`${set.title} image`} />
      <h3 className="set-card__title">{ set.title }</h3>
    </div>
  );
}

export default SetCard;
