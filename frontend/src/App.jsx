import {
  createBrowserRouter,
  createRoutesFromElements,
  Route,
  RouterProvider,
 } from "react-router-dom";

import {
  RootLayout,
} from "./layouts"

import { loader as setLoader } from "./components/RecentSets";

import {
  About,
  Contact,
  FAQ, 
  Home,
} from './pages';

const router = createBrowserRouter(
  createRoutesFromElements(
    <Route 
      path="/"
      element={<RootLayout />}
      errorElement={<div>Oops! There was an error.</div>}
    >
      <Route 
        index
        element={<Home />}
        loader={setLoader}
      />
      <Route 
        path="about"
        element={<About />}
      />
      <Route 
        path="contact"
        element={<Contact />}
      />
      <Route 
        path="faq"
        element={<FAQ />}
      />
    </Route>
  )
)

function App() {
  return (
    <RouterProvider router={router} />
  )
}

export default App
