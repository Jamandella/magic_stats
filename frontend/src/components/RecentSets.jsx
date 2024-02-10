import { useLoaderData } from 'react-router-dom';
import { SetCard } from '../components'

const sets = [
  {
    id: 1,
    title: "Lost Caverns of Ixalan",
    img: 'LCI.jpg',
  },
  {
    id: 2,
    title: "Wilds of Eldraine",
    img: 'WOE.jpg',
  },
  {
    id: 3,
    title: "March of the Machine",
    img: 'MOM.jpg',
  },
  {
    id: 4,
    title: "Tales of Middle Earth",
    img: 'LTR.jpg',
  },
];

export async function loader() {
  const res = await fetch('https://jsonplaceholder.typicode.com/posts');

  if (!res.ok) {
    throw Error('Could not fetch the data')
  }

  return res.json();
}

const RecentSets = () => {
  const posts = useLoaderData();
  console.log(posts);

  return (
    <section className="recent-sets">
      <div className="container flow" data-type="wide">
        <div className="recent-sets__list | ">
          {sets.map(set => (
            <SetCard set={set} key={set.id} />
          ))}
        </div>
      </div>
    </section>
  );
}

export default RecentSets;
