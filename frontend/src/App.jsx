import {
  createBrowserRouter,
  createRoutesFromElements,
  Route,
  RouterProvider,
 } from "react-router-dom";

import {
  RootLayout,
  SetLayout
} from "./layouts"

import { 
  setLoader, 
  setsLoader 
} from "./loaders";

import {
  About,
  Contact,
  ErrorPage,
  Glossary, 
  Home,
  SetDetails
} from "./pages";

const router = createBrowserRouter(
  createRoutesFromElements(
    <Route 
      path="/"
      element={<RootLayout />}
      errorElement={<ErrorPage />}
      loader={setsLoader}
    >
      <Route 
        index
        element={<Home />}
        loader={setsLoader}
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
        path="glossary"
        element={<Glossary />}
      />
      <Route
        path="sets"
        element={<SetLayout />}
      >
        <Route 
          path=":code"
          element={<SetDetails />}
          loader={setLoader}
        />
      </Route>
    </Route>
  )
)

function App() {
  return (
    <RouterProvider router={router} />
  )
}

export default App
