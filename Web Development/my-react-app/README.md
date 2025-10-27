# React + Vite

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Babel](https://babeljs.io/) (or [oxc](https://oxc.rs) when used in [rolldown-vite](https://vite.dev/guide/rolldown)) for Fast Refresh
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/) for Fast Refresh

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev & build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the ESLint configuration

If you are developing a production application, we recommend using TypeScript with type-aware lint rules enabled. Check out the [TS template](https://github.com/vitejs/vite/tree/main/packages/create-vite/template-react-ts) for information on how to integrate TypeScript and [`typescript-eslint`](https://typescript-eslint.io) in your project.

## Quick Start
```bash
# 1. Install Vite with React template using npm
npm create vite@latest my-react-app -- --template react

# 2. Navigate to your new project folder
cd my-react-app

# 3. Install dependencies
npm install

# 4. Start the development server
npm run dev
```
**Or, with yarn:**
```bash
yarn create vite my-react-app --template react
cd my-react-app
yarn
yarn dev
```

Refer to [Vite + React Quick Start](https://react.dev/learn/start-a-new-react-project#vite) or [W3Schools React Vite Setup](https://www.w3schools.com/react/react_vite.asp) for more details.
