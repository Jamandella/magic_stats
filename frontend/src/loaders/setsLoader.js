export async function setsLoader() {
  
  const res = await fetch('http://localhost:3000/MTGsets');

  if (!res.ok) {
    throw Error('Could not fetch the data');
  }

  const sets = await res.json();
  const formattedSets = sets.map(([title, code]) => ({ title, code }));
  return formattedSets;
  
}