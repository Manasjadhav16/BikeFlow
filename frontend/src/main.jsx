import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import { ModeProvider } from "./lib/mode";
import "./styles.css";

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <BrowserRouter>
      <ModeProvider>
        <App />
      </ModeProvider>
    </BrowserRouter>
  </React.StrictMode>,
);
