import { Objective, UsefulLinks } from "../components"

const About = () => {
  return (
    <div className="about">
      <div className="container flow margin-block-xl">
        <h2 className="heading-2">About</h2>
        <Objective />
        <UsefulLinks />
      </div>
    </div>
  );
}

export default About;
