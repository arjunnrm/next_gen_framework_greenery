// Parses the inline CSS strings lifted verbatim from the design mockup into
// React style objects, so the design source stays byte-identical to the reference.
const cache = new Map();

function camel(prop) {
  if (prop.startsWith("--")) return prop;
  return prop.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
}

export function sx(css) {
  if (!css) return undefined;
  const hit = cache.get(css);
  if (hit) return hit;
  const out = {};
  String(css)
    .split(";")
    .forEach((decl) => {
      const i = decl.indexOf(":");
      if (i < 0) return;
      const k = decl.slice(0, i).trim();
      const v = decl.slice(i + 1).trim();
      if (!k || !v) return;
      out[camel(k)] = v;
    });
  cache.set(css, out);
  return out;
}
