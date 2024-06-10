import { NavLink } from 'react-router-dom';
import Dropdown from './Dropdown';

const navLinks = [
  {
    name: 'Glossary',
    url: '/glossary'
  },
  {
    name: 'About',
    url: '/about'
  },
  {
    name: 'Contact',
    url: '/contact'
  },
];

const SiteHeader = ({ sets }) => {
  return (
    <header className="site-header | box-shadow-5">
      <div className="container" data-type="wide">
        <div className="site-header__inner">
          <div className="site-header__brand">
            <NavLink to="/">
              23 Spells
            </NavLink>
          </div>

          <nav className="primary-navigation">
            <ul className="primary-navigation__links | flex-group">
              <Dropdown title={'Sets'} links={sets} />
              {navLinks.map(link => (
                <li key={link.url}>
                  <NavLink to={link.url}>{ link.name }</NavLink>
                </li>
              ))}
            </ul>
          </nav>
        </div>
      </div>      
    </header>
  );
}

export default SiteHeader;
