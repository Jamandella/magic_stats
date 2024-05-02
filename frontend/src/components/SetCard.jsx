import { Link } from "react-router-dom";

const SetCard = ({ set }) => {
  const imgUrl = new URL(`../assets/sets/${set.code}.jpg`, import.meta.url).href

  return (
    <Link to={`sets/${set.code}`} className="set-card | box-shadow-3">
      <img className="set-card__image" src={imgUrl} alt={`${set.title} image`} />
      <h3 className="set-card__title">{ set.title }</h3>
    </Link>
  );
}

export default SetCard;
