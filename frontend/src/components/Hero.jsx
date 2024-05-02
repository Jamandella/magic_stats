import RecentSets from './RecentSets';

const Hero = () => {
  return (
    <section className="hero | section flow">
      <div className="hero__intro | container margin-block-end-xl">
        <div className="hero__image">
          <span className="heading-1">Logo</span>
        </div>
        <div className="flow text-center">
          <h1 className="visually-hidden">23 Spells</h1>
          <p className="heading-1">MTG Draft Stats Simplified</p>
          <p className="fs-500">Magic: the Gathering draft stats for everyone.</p>
        </div>
      </div>
      <RecentSets />
    </section>
  );
}

export default Hero;
