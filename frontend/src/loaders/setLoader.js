export async function setLoader({ params }) {

  const res = await fetch('http://localhost:3000/MTGsets');

  if (!res.ok) {
    throw Error('Could not fetch the data');
  }

  const sets = await res.json();
  const formattedSets = sets.map(([title, code]) => ({ title, code }));
  const formattedSet = formattedSets.find(set => set.code === params.code);
  return formattedSet;

}