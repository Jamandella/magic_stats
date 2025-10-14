import { SidebarProvider, SidebarTrigger } from "../ui/sidebar"
import StatsSidebar from "@/components/layout/StatsSidebar"

const Layout = ({ children }: {children: React.ReactNode}) => {
  return (
    <SidebarProvider>
      <StatsSidebar />
      <main>
        <SidebarTrigger />
        {children}
      </main>
    </SidebarProvider>
  )
}

export default Layout