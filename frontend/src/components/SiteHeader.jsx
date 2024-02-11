import { NavLink } from 'react-router-dom';

const navLinks = [
  {
    name: 'Home',
    url: '/'
  },
  {
    name: 'About',
    url: '/about'
  },
  {
    name: 'Contact',
    url: '/contact'
  },
  {
    name: 'FAQ',
    url: '/faq'
  }
];

const SiteHeader = () => {
  return (
    <header className="site-header | box-shadow-5">
      <div className="container" data-type="wide">
        <div className="site-header__inner">
          <div className="site-header__brand">
            <span className="fw-semi-bold fs-500">23 Spells</span>
          </div>

          <nav className="primary-navigation">
            <ul className="primary-navigation__links | flex-group">
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
