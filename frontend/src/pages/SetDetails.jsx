import React from "react";
import { useLoaderData } from "react-router-dom";
import { Banner } from "../components";

const SetDetails = () => {
  const set = useLoaderData();

  return (
    <div className="set-details">
      <Banner set={set} />
    </div>
  );
}

export default SetDetails;
