import { Outlet, useLoaderData } from "react-router-dom";
import { SiteHeader } from "../components";

const RootLayout = () => {
  const sets = useLoaderData();

  return (
    <div className="root-layout">
      <SiteHeader sets={sets} />
      <main>
        <Outlet />
      </main>
    </div>
  );
}

export default RootLayout;
