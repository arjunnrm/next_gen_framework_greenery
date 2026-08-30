// Typed-ish fetch wrapper. Every backend response carries X-Request-Id and, on
// failure, the single error envelope described in the build spec (§9.2).
const BASE = "/api";

export class ApiError extends Error {
  constructor(envelope, requestId, status) {
    super((envelope && envelope.message) || "Request failed");
    this.code = (envelope && envelope.code) || "INTERNAL";
    this.detail = (envelope && envelope.detail) || "";
    this.fieldPath = (envelope && envelope.field_path) || null;
    this.docsUrl = (envelope && envelope.docs_url) || null;
    this.requestId = requestId || (envelope && envelope.request_id) || null;
    this.status = status;
  }
}

async function call(path, { method = "GET", body, signal } = {}) {
  const res = await fetch(BASE + path, {
    method,
    signal,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const requestId = res.headers.get("X-Request-Id");
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (e) {
    data = null;
  }
  if (!res.ok) throw new ApiError(data && data.error, requestId, res.status);
  return data;
}

export const api = {
  health: () => call("/health"),
  config: () => call("/config"),
  access: (rootId) => call("/storage/access?root_id=" + encodeURIComponent(rootId)),
  list: (rootId, prefix) =>
    call("/storage/list?root_id=" + encodeURIComponent(rootId) + "&prefix=" + encodeURIComponent(prefix || "")),
  read: (rootId, path) =>
    call("/storage/read?root_id=" + encodeURIComponent(rootId) + "&path=" + encodeURIComponent(path)),
  write: (payload) => call("/storage/write", { method: "POST", body: payload }),
  // Read a spec from a user-supplied path, parse it and validate it, without loading it.
  validatePath: (payload) => call("/storage/validate", { method: "POST", body: payload }),
  // Template catalogue, discovered from the templates/ directory on the server.
  knowledge: () => call("/attribute-knowledge"),
  templates: () => call("/templates"),
  template: (id) => call("/templates/" + encodeURIComponent(id)),
  render: (spec, format) => call("/spec/render", { method: "POST", body: { spec, format } }),
  validate: (spec) => call("/spec/validate", { method: "POST", body: { spec } }),
  importSpec: (payload) => call("/spec/import", { method: "POST", body: payload }),
  runAction: (actionId, payload) =>
    call("/actions/" + encodeURIComponent(actionId) + "/run", { method: "POST", body: payload }),
  runStatus: (runId) => call("/actions/runs/" + encodeURIComponent(runId)),
  cancelRun: (runId) => call("/actions/runs/" + encodeURIComponent(runId) + "/cancel", { method: "POST" }),
};
