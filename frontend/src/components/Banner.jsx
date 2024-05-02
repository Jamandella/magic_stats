import React from "react";

const Banner = ({ set }) => {
  const imgUrl = new URL(`../assets/sets/${set.code}.jpg`, import.meta.url).href;

  return (
    <div
      className="banner"
      style={{ backgroundImage: `url(${imgUrl})` }}
    >
      <div className="banner__overlay">
        <div className="container" data-type="wide">
          <h2 className="banner__title | heading-3">{ set.title }</h2>
        </div>
      </div>
    </div>
  );
}

export default Banner;
