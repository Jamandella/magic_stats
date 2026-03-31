# 23 Spells

## Table of Contents
- [About](#about)
  - [Project Objectives](#project-objectives)
  - [Motivation](#motivation)
  - [Target Audience](#target-audience)
  - [Scope](#scope)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
  - [Frontend](#frontend)
  - [Backend](#backend)
- [Contributing](#contributing)
- [License](#license)
- [Useful Links](#useful-links)

## About

The goal of this project is to provide Magic: the Gathering (MTG) draft archetype statistics and insights. We achieve this by analyzing data from Magic Arena drafts, provided by [17 Lands](https://www.17lands.com/). While we draw inspiration from 17 Lands and share some similarities, it's essential to note that we are not affiliated with 17 Lands. Instead, we consider 23 Spells a supplement to 17 Lands, with a specific focus on providing archetype contextual statistics.

### Project Objectives

Our primary objectives are to:
- Analyze data from Magic Arena drafts to programmatically identify draft archetypes and their performance.
- Provide archetype contextual statistics, shedding light on how card performance varies across different deck archetypes.
- Offer data-driven recommendations for draft picks, helping players make informed decisions.

### Motivation

As avid Magic: the Gathering players and fans of 17 Lands, we were inspired to embark on this project by our shared passion for MTG drafting. We believe in the value of 17 Lands' data and recognize that there is still ample room for expansion in the realm of statistical analysis, offering new and valuable information to drafters.

### Target Audience

This project is specifically designed for Magic: the Gathering draft enthusiasts who are passionate about enhancing their drafting skills and gaining a deeper understanding of 

### Scope

- **Data Source:**
Our analysis is based on data obtained from Magic Arena drafts via 17 Lands. We do not incorporate data from other sources and we are limited by the publicly available data that 17 Lands provides.

- **Archetype Focus:**
Our primary focus is on analyzing the performance of different draft archetypes as well as asessing the performance of individual cards within each archetype.

- **Not Affiliated with 17 Lands:**
23 Sepells is an independent project and is not affiliated with 17 lands. While we draw inspiration from them, our analyses and insights are unique to this project.

- **Continuous Development:**
The scope of this project may expand over time, incorporating new features or refining existing ones. However, our commitment remains centered around Magic: the Gathering draft archetype statistics and insights.

## Getting Started

### Prerequisites

Before you begin, ensure you have installed the following on your computer:

- [Python 3](https://www.python.org/downloads/) (required for the backend)
- [Node.js](https://nodejs.org/) and [npm](https://www.npmjs.com/get-npm) (required for the front end)

### Installation

**1. Clone the repository:**

  ```bash
  git clone https://github.com/Jamandella/magic_stats
  ```

**Note:** If you are contributing to this project you should first fork this repository and then clone your forked repository onto your computer.

**2. Navigate to the project directory:**

  ```bash
  cd magic_stats
  ```

  **Note:** You do not need to run `npm install`. This repository already contains the node modules required for the frontend. While this is unusual, this is because (reasons here). For more details refer to the [project structure](#project-structure) section of this document below.

<!-- Josh, briefly explain why the node modules are already pre-installed in the project -->

**3. Install backend dependencies:**

  ```bash
  pip3 install -r requirements.txt
  ```

**4. Run the project backend:**

  ```bash
  python3 app.py
  ```

**5. Run the project frontend:**

  Navigate to `/frontend` inside the root project directory.

  Next, run the following command:

  ```bash
  npm run serve
  ```
**Note:** In order to see any frontend changes reflected in the backend, you will need to run `npm run build` inside of the `/frontend` directory and then restart the backend server. 

For frontend development, you should only need to run the development server using `npm run serve`.

## Project Structure

### Frontend

The frontend is a React application built using Vite, a frontend build tool. It features:

- **Technology Stack:**
  - Vite: The build tool that enhances the development experience.
  - React: The core framework for building the user interface.

- **Styling & UI Components:**
  - Tailwind CSS: A low-level, utility-first CSS framework that provides design primitives for creating custom, responsive user interfaces without opinionated styles.
  - Shadcn UI: A collection of accessible, customizable UI components built with Radix and Tailwind CSS.


### Backend

<!-- Josh -->
<!-- An explaination of how the mixed deployment is working would be good to include in here as well as anything else that you think is relevant.  -->

## Contributing

To contribute to this project, follow these steps:

**1. Fork the Repository**

Click on the "Fork" button on the top right corner of this repository. This will create a copy of the project in your GitHub account.

**2. Clone your Fork**

```bash
git clone https://github.com/your-username/project-name.git
cd project-name
```

Replace your-username and project-name with your GitHub username and the name of the project, respectively.

**3. Create a Branch**

Create a new branch for your contribution:

```bash
git checkout -b your-feature-name
```

**4. Make Changes**

Make your changes to the codebase. We are utilizing the [Google Style Guide](https://google.github.io/styleguide/) to maintain consistency across this project. Make sure your code follows these guidelines.

**5. Test**

If your contribution includes new features or modifications, please test thoroughly to ensure that existing functionality is not broken.

**6. Commit Changes**

```bash
git add .
git commit -m "Your meaningful commit message"
```

**7. Push Changes**

```bash
git push origin your-feature-name
```

**8. Open a Pull Request**

Go to the [original repository](https://github.com/Jamandella/magic_stats) and open a pull request. Provide a clear and descriptive title for your pull request, and explain the changes you have made.

**9. Participate in Code Review**

Be responsive to any feedback or comments on your pull request. Make necessary changes if requested by the maintainers.

**10. Wait for Approval**

Once your contribution has been reviewed and meets the project standards, it will be merged. Congratulations, and thank you for your contribution!

## Useful Links

- [Github Repository](https://github.com/Jamandella/magic_stats)
- [Google Style Guide](https://developers.google.com/style)
- [Figma Wireframe](https://www.figma.com/file/YqYaigEWARHy5NL5g08Kpq/Magic-Stats-Project?type=whiteboard&t=UKJd6ZK53VYmdl8B-0)
- [Meeting Notes](https://drive.google.com/drive/folders/126ZhLhKfrLapQaeJPCU4REnxilpgR3qA)
- [Lucid Chart](https://lucid.app/lucidchart/002d3ba4-b5e1-44b8-9c6c-3b6e62869340/edit?invitationId=inv_d3a938cc-a15e-49bf-8d0a-d7902a952bda&page=0_0#)
- [17 Lands](https://www.17lands.com/)
- [Recharts](https://recharts.org/en-US/)
- [Trello Workspace](https://trello.com/b/z2cY9T70/magic-project-tracker)

## License

Specify the project's open-source license and provide any additional licensing information if necessary. For example:

This project is licensed under the [License Name](LICENSE) - see the [LICENSE](LICENSE) file for details.

---

<!-- The key is to make the README informative and user-friendly, providing all the necessary information for users, contributors, and team members to understand and work with your project. -->
