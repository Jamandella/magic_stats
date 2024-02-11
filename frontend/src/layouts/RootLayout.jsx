import { Outlet } from "react-router-dom";
import { SiteHeader } from "../components";

const RootLayout = () => {
  return (
    <div className="root-layout">
      <SiteHeader />
      <main>
        <Outlet />
      </main>
    </div>
  );
}

export default RootLayout;
