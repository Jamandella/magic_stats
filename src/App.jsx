import { useState } from 'react'
import reactLogo from './assets/react.svg'
import viteLogo from '/vite.svg'
import './App.css'

function App() {
<<<<<<< Updated upstream
  const [count, setCount] = useState(0)

=======
  const links = [
    {name: 'Github Repo', url: 'https://github.com/Jamandella/magic_stats'},
    {name: 'Figma Wireframe', url: 'https://www.figma.com/file/YqYaigEWARHy5NL5g08Kpq/Magic-Stats-Project?type=whiteboard&t=UKJd6ZK53VYmdl8B-0'},
    {name: 'Meeting Notes', url: 'https://drive.google.com/drive/folders/126ZhLhKfrLapQaeJPCU4REnxilpgR3qA'},
    {name: 'Lucid Chart', url: 'https://lucid.app/lucidchart/002d3ba4-b5e1-44b8-9c6c-3b6e62869340/edit?invitationId=inv_d3a938cc-a15e-49bf-8d0a-d7902a952bda&page=0_0#'},
    {name: '17 Lands', url: 'https://www.17lands.com/'},
    {name: 'Recharts', url: 'https://recharts.org/en-US/'},
  ];
//Ben was here
>>>>>>> Stashed changes
  return (
    <>
      <div>
        <a href="https://vitejs.dev" target="_blank">
          <img src={viteLogo} className="logo" alt="Vite logo" />
        </a>
        <a href="https://react.dev" target="_blank">
          <img src={reactLogo} className="logo react" alt="React logo" />
        </a>
      </div>
      <h1>Vite + React</h1>
      <div className="card">
        <button onClick={() => setCount((count) => count + 1)}>
          count is {count}
        </button>
        <p>
          Edit <code>src/App.jsx</code> and save to test HMR
        </p>
      </div>
      <p className="read-the-docs">
        Click on the Vite and React logos to learn more
      </p>
    </>
  )
}

export default App
