// A gazette figure island that renders Auspice with a row of JS-built buttons for
// stepping the map through time. Follows the gazette figure contract — a pure
// render(container, data, opts) that returns a lifecycle object. Each button calls
// the Auspice viz's `update` method with a `dateMin` to move the temporal window.
//
// data = { main: <auspice dataset JSON> }   // e.g. bdbv-2026.json
// opts = { panels?, query?, ... }            // forwarded to Auspice

import { render as auspiceRender } from "./lib/auspice.js";

// const STEPS = [
//   { label: "From Jan 2026", dateMin: "2026-01-01" },
//   { label: "From Aug 2026", dateMin: "2026-08-01" },
// ];

export function render(container, data, opts = {}) {
  const root = document.createElement("div");
  container.appendChild(root);

  
  // Row of controls, rendered via JS.
  const controls = document.createElement("div");
  Object.assign(controls.style, {
    display: "flex",
    gap: "8px",
    margin: "0 0 8px",
  });
  root.appendChild(controls);

  // Host for the Auspice viz.
  const vizHost = document.createElement("div");
  root.appendChild(vizHost);

  const viz = auspiceRender(vizHost, data, opts);

  for (const step of opts.buttons) {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = step.label;
    button.addEventListener("click", () => {
      viz.update({...step.data});
    });
    controls.appendChild(button);
  }

  return {
    element: root,
    resize(width) {
      viz.resize?.(width);
    },
    destroy() {
      viz.destroy?.();
      root.remove();
    },
    update(arg) {
      viz.update?.(arg);
    },
  };
}
