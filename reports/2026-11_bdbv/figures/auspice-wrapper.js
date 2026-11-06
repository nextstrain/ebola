/**
 * TODO: this wrapper could be automated by `gazette` via some magic keyword, e.g.
 * specify `component: Auspice` (no .js suffix)
 */

import { render as auspiceRender } from "./lib/auspice.js";

export function render(container, data, opts = {}) {
  const viz = auspiceRender(container, data, opts);
  return viz  
}
