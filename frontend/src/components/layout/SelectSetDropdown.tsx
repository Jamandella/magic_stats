import { useState } from "react"

import { ChevronsUpDown, Frame, Command, Book } from "lucide-react"

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar"

const sets = [
  {
    name: "Tarkir Dragonstorm",
    symbol: Frame
  },
  {
    name: "Aetherdrift",
    symbol: Command
  },
  {
    name: "Foundations",
    symbol: Book
  },
]

const SelectSetDropdown = () => {
  const { isMobile } = useSidebar()
  const [selectedSet, setSelectedSet] = useState(sets[0])

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <SidebarMenuButton
              size="lg"
              className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
            >
              <div className="bg-sidebar-primary text-sidebar-primary-foreground flex aspect-square size-7 items-center justify-center rounded-md">
                <selectedSet.symbol className="size-4" />
              </div>
              <div className="grid flex-1 text-left text-sm leading-tight">
                <span className="truncate font-medium">
                  {selectedSet.name}
                </span>
              </div>
              <ChevronsUpDown className="ml-auto" />
            </SidebarMenuButton>
          </DropdownMenuTrigger>
          <DropdownMenuContent
            className="w-(--radix-dropdown-menu-trigger-width) min-w-56 rounded-lg"
            align="start"
            side={isMobile ? "bottom" : "right"}
            sideOffset={4}
          >
            <DropdownMenuLabel className="text-muted-foreground text-xs">
              Sets
            </DropdownMenuLabel>
            {sets.map(set => (
              <DropdownMenuItem
                className="gap-2 p-2"
                key={set.name}
                onClick={() => setSelectedSet(set)}
              >
                <div className="flex size-6 items-center justify-center rounded-md border">
                  <set.symbol className="size-3.5 shrink-0" />
                </div>
                {set.name}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  )
}

export default SelectSetDropdown