// tailwind.config.js
/** @type {import('tailwindcss').Config} */
module.exports = {
  corePlugins: {
    preflight: false, // disable Tailwind's reset
  },
  content: ["./src/**/*.{js,jsx,ts,tsx}", "../docs/**/*.mdx"], // my markdown stuff is in ../docs, not /src
  darkMode: ["class", '[data-theme="dark"]'], // hooks into docusaurus' dark mode settigns
  theme: {
    extend: {
      colors: {
        "primary-100": "var(--primary-100)",
        "primary-200": "var(--primary-200)",
        "primary-300": "var(--primary-300)",
        "primary-400": "var(--primary-400)",
        "primary-500": "var(--primary-500)",
        "primary-600": "var(--primary-600)",
        "primary-700": "var(--primary-700)",
        "primary-800": "var(--primary-800)",
        "primary-900": "var(--primary-900)",
        "second-100": "var(--second-100)",
        "second-200": "var(--second-200)",
        "second-300": "var(--second-300)",
        "second-400": "var(--second-400)",
        "second-500": "var(--second-500)",
        "second-600": "var(--second-600)",
        "second-700": "var(--second-700)",
        "second-800": "var(--second-800)",
        "second-900": "var(--second-900)",
        "bg-primary-color": "var(--primary-bg-color)",
      },
    },
  },
  plugins: [],
};
