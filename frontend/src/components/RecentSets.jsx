import { useLoaderData } from 'react-router-dom';
import { SetCard } from '../components';

const RecentSets = () => {
  const sets = useLoaderData();

  return (
    <section className="recent-sets">
      <div className="container flow" data-type="wide">
        <div className="recent-sets__list">
          {sets.slice(-4).map(set => (
            <SetCard set={set} key={set.title} />
          ))}
        </div>
      </div>
    </section>
  );
}

export default RecentSets;
