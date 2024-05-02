import { useState } from "react";
import { Link } from "react-router-dom";
import IconCaretDown from "./icons/IconCaretDown";

const Dropdown = ({ title, links }) => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div className="dropdown">
      <div 
        className="dropdown__title"
        onClick={() => setIsOpen(!isOpen)}
      >
        {title}
        <IconCaretDown />
      </div>
      {isOpen && (
        <ul className="dropdown__menu | flow">
          {links.slice(-7).map(link => (
            <li className="dropdown__menu-item" key={link.code}>
              <Link onClick={() => setIsOpen(false)} to={`sets/${link.code}`}>{link.title}</Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default Dropdown;
