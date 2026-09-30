import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        normal: {
          bg: "#ecfdf5",
          border: "#10b981",
          text: "#047857",
        },
        watch: {
          bg: "#fffbeb",
          border: "#f59e0b",
          text: "#b45309",
        },
        critical: {
          bg: "#fef2f2",
          border: "#ef4444",
          text: "#b91c1c",
        },
      },
    },
  },
  plugins: [],
};

export default config;
