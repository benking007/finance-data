"use strict";

const $ = (id) => document.getElementById(id);
const state = {
  key: "",
  config: null,
  reference: null,
  details: new Map(),
  expanded: new Map(),
  loadRevision: 0,
  viewRevision: 0,
  focusHeading: false,
  scrollHeading: false,
};

function node(tag, text, className) {
  const result = document.createElement(tag);
  if (text !== null && text !== undefined) result.textContent = text;
  if (className) result.className = className;
  return result;
}

function report(text = "", kind = "") {
  const region = $("reference-status");
  region.textContent = text;
  region.dataset.state = kind;
  region.classList.toggle("error", kind === "error");
  region.hidden = !text;
}

function authMessage(text = "", failed = false) {
  const region = $("reference-auth-status");
  if (!region) return;
  region.textContent = text;
  region.classList.toggle("error", failed);
  region.hidden = !text;
}

async function request(path, key = state.key) {
  const headers = {Accept: "application/json"};
  if (key && path.startsWith("/v1/")) headers["X-API-Key"] = key;
  const response = await fetch(path, {headers, cache: "no-store", credentials: "same-origin"});
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    const error = new Error(typeof body.detail === "string" ? body.detail : `请求失败（${response.status}）`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

function decodeRoute() {
  let parts;
  try {
    parts = location.hash.slice(1).split("/").map(decodeURIComponent);
  } catch {
    return {kind: "overview"};
  }
  const [kind, first, second] = parts;
  if (kind === "dataset" && first) return {kind, dataset: first, operation: second};
  if (kind === "endpoint" && first && second) return {kind, group: first, operation: second};
  return {kind: "overview"};
}

function docHref(...parts) {
  return `#${parts.map(encodeURIComponent).join("/")}`;
}

function linked(label, href, className) {
  const link = node("a", label, className);
  link.href = href;
  return link;
}

function docLink(title, href, {active = false, caption, className = "reference-nav-item"} = {}) {
  const link = linked(null, href, className);
  const label = node("span", title);
  link.append(label);
  if (caption) link.append(node("code", caption));
  if (active) link.setAttribute("aria-current", "page");
  link.addEventListener("click", (event) => {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) return;
    state.focusHeading = event.detail === 0;
    state.scrollHeading = true;
    if (location.hash === href) {
      event.preventDefault();
      renderSelection();
    }
  });
  return link;
}

function textMatches(terms, values) {
  const contents = values.filter(Boolean).join(" ").toLocaleLowerCase();
  return terms.every((term) => contents.includes(term));
}

function categoryPath(dataset) {
  const path = Array.isArray(dataset.path) ? dataset.path.filter(Boolean) : [];
  if (path.length > 1) return path;
  return [dataset.category || "数据集", dataset.title || dataset.id];
}

function buildDatasetTree(datasets) {
  const root = {children: new Map(), items: []};
  for (const dataset of datasets) {
    let branch = root;
    const path = categoryPath(dataset).slice(0, -1);
    for (const name of path) {
      if (!branch.children.has(name)) {
        const child = {name, children: new Map(), items: []};
        branch.children.set(name, child);
        branch.items.push(child);
      }
      branch = branch.children.get(name);
    }
    branch.items.push(dataset);
  }
  return root;
}

function branchNode(title, children, {key, forceOpen = false, searchOpen = false, top = false} = {}) {
  const item = node("li", null, top ? "reference-group" : "reference-branch");
  const detail = node("details");
  detail.open = searchOpen || (state.expanded.get(key) ?? forceOpen);
  detail.addEventListener("toggle", () => {
    if (!searchOpen && detail.isConnected) state.expanded.set(key, detail.open);
  });
  const summary = node("summary");
  summary.append(node("span", title));
  detail.append(summary, children);
  item.append(detail);
  return item;
}

function datasetNavigation(branch, selectedPath, selection, searchOpen, parents = []) {
  const list = node("ul", null, "reference-tree-list");
  for (const item of branch.items) {
    if (item.items) {
      const path = [...parents, item.name];
      const inside = path.every((entry, index) => entry === selectedPath[index]);
      list.append(branchNode(item.name, datasetNavigation(item, selectedPath, selection, searchOpen, path), {
        key: `datasets:${JSON.stringify(path)}`, forceOpen: inside, searchOpen: searchOpen || inside,
      }));
      continue;
    }
    const li = node("li");
    li.append(docLink(item.title || item.id, docHref("dataset", item.id), {
      active: selection.kind === "dataset" && selection.dataset === item.id,
      caption: item.id,
    }));
    list.append(li);
  }
  return list;
}

function renderNavigation(selection = decodeRoute()) {
  const reference = state.reference;
  if (!reference) return;
  const terms = $("reference-search").value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
  const searching = terms.length > 0;
  const root = node("ul", null, "reference-tree-list");
  const overview = node("li");
  overview.append(docLink("使用说明", docHref("overview"), {active: selection.kind === "overview"}));
  const matchedOverview = !searching || textMatches(terms, ["使用说明", "认证", "分页", "overview", "token", "API Key"]);
  if (matchedOverview) root.append(overview);

  let endpointCount = 0;
  for (const group of reference.groups || []) {
    const operations = group.operations.filter((operation) => textMatches(terms, [
      group.title, operation.title, operation.id, operation.method, operation.path,
    ]));
    if (!operations.length) continue;
    endpointCount += operations.length;
    const list = node("ul", null, "reference-tree-list");
    for (const operation of operations) {
      const li = node("li");
      li.append(docLink(operation.title, docHref("endpoint", group.id, operation.id), {
        active: selection.kind === "endpoint" && selection.group === group.id && selection.operation === operation.id,
      }));
      list.append(li);
    }
    const current = selection.kind === "endpoint" && selection.group === group.id;
    root.append(branchNode(group.title, list, {key: `endpoint:${group.id}`, forceOpen: true, searchOpen: searching || current, top: true}));
  }

  const selectedDataset = reference.datasets.find((dataset) => dataset.id === selection.dataset);
  const datasets = reference.datasets.filter((dataset) => textMatches(terms, [dataset.id, dataset.title, ...categoryPath(dataset)]));
  if (datasets.length) {
    const selectedPath = selectedDataset ? categoryPath(selectedDataset) : [];
    const sections = datasetNavigation(buildDatasetTree(datasets), selectedPath, selection, searching);
    root.append(branchNode("按数据集浏览", sections, {key: "all-datasets", forceOpen: true, searchOpen: searching || Boolean(selectedDataset), top: true}));
  }
  if (!root.children.length) root.append(node("li", "没有匹配的接口或数据集。", "reference-empty"));
  $("reference-tree").replaceChildren(root);
  const count = $("reference-tree-count");
  if (count) count.textContent = searching ? `找到 ${endpointCount + datasets.length + Number(matchedOverview)} 项` : `${reference.datasets.length} 个数据集`;
}

function breadcrumbs(titles) {
  const list = node("ol", null, "reference-breadcrumb");
  list.setAttribute("aria-label", "当前位置");
  titles.forEach((text) => list.append(node("li", text)));
  return list;
}

function heading(title, description) {
  const wrapper = node("header", null, "reference-title-block");
  const name = node("h2", title, "reference-heading");
  name.tabIndex = -1;
  wrapper.append(name);
  if (description) wrapper.append(node("p", description, "reference-description"));
  return wrapper;
}

function section(title, intro) {
  const wrapper = node("section", null, "reference-section");
  wrapper.append(node("h3", title));
  if (intro) wrapper.append(node("p", intro, "reference-description"));
  return wrapper;
}

function isPresent(value) {
  return value !== null && value !== undefined && value !== "";
}

function describeValue(value) {
  if (!isPresent(value)) return "—";
  if (Array.isArray(value)) return value.map(describeValue).join("、");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function docTable(caption, headings, rows) {
  const wrap = node("div", null, "reference-table-wrap");
  const table = node("table");
  table.append(node("caption", caption, "visually-hidden"));
  const head = node("thead");
  const tr = node("tr");
  for (const item of headings) {
    const th = node("th", item);
    th.scope = "col";
    tr.append(th);
  }
  head.append(tr);
  const body = node("tbody");
  for (const data of rows) {
    const row = node("tr");
    data.forEach((value, index) => {
      const cell = node(index === 0 ? "th" : "td");
      if (index === 0) cell.scope = "row";
      if (value instanceof Node) cell.append(value);
      else cell.textContent = describeValue(value);
      row.append(cell);
    });
    body.append(row);
  }
  table.append(head, body);
  wrap.append(table);
  return wrap;
}

function parametersTable(operation) {
  const parameters = section("请求参数");
  if (!operation.parameters?.length) {
    parameters.append(node("p", "此接口无需请求参数。", "reference-description"));
    return parameters;
  }
  const places = {path: "路径", query: "查询字符串", header: "请求头", body: "请求体"};
  const rows = operation.parameters.map((parameter) => {
    const description = node("div", parameter.description || "—");
    const details = [];
    if (isPresent(parameter.default)) details.push(`默认值 ${describeValue(parameter.default)}`);
    if (isPresent(parameter.example)) details.push(`示例 ${describeValue(parameter.example)}`);
    if (parameter.options?.length) details.push(`可选值 ${describeValue(parameter.options)}`);
    if (details.length) description.append(node("p", details.join(" · "), "reference-meta"));
    return [node("code", parameter.name), places[parameter.location] || parameter.location,
      parameter.type, parameter.required ? "是" : "否", description];
  });
  parameters.append(docTable("请求参数", ["参数", "位置", "类型", "必填", "说明"], rows));
  return parameters;
}

function responseTable(operation) {
  if (!operation.response_fields?.length) return null;
  const response = section("响应结构");
  response.append(docTable("响应结构", ["字段", "类型", "说明"], operation.response_fields.map((field) => [
    node("code", field.name), field.type, field.description,
  ])));
  return response;
}

function fieldTable(fields) {
  if (!fields?.length) return null;
  const showUnit = fields.some((field) => field.unit);
  const dictionary = section("数据字段", "字段定义适用于当前数据集和已授权的列。部分数值会以字符串返回，以保留原始精度。");
  const labels = showUnit ? ["字段", "类型", "单位", "说明"] : ["字段", "类型", "说明"];
  dictionary.append(docTable("数据字段", labels, fields.map((field) => {
    const head = [node("code", field.name), field.type];
    return showUnit ? [...head, field.unit, field.description] : [...head, field.description];
  })));
  return dictionary;
}

function shellQuote(value) {
  return `'${String(value).replaceAll("'", "'\"'\"'")}'`;
}

function requestExample(operation) {
  let path = operation.path;
  for (const parameter of operation.parameters || []) {
    if (parameter.location === "path" && isPresent(parameter.example)) {
      path = path.replace(`{${parameter.name}}`, encodeURIComponent(parameter.example));
    }
  }
  let query = "";
  if (operation.example_query && typeof operation.example_query === "object") {
    query = new URLSearchParams(operation.example_query).toString();
  }
  const address = `${location.origin}${path}${query ? `?${query}` : ""}`;
  const method = operation.method || "GET";
  const curl = `curl${method === "GET" ? "" : ` -X ${method}`} ${shellQuote(address)}`;
  return state.reference.authentication?.required
    ? `${curl} \\\n  -H '${state.reference.authentication.header || "X-API-Key"}: YOUR_API_KEY'`
    : curl;
}

function codeBlock(label, contents) {
  const block = node("figure", null, "reference-code");
  const bar = node("figcaption", null, "reference-code-header");
  const copy = node("button", "复制", "reference-copy");
  copy.type = "button";
  copy.setAttribute("aria-label", `复制${label}`);
  const pre = node("pre");
  pre.tabIndex = 0;
  pre.append(node("code", contents));
  bar.append(node("span", label), copy);
  block.append(bar, pre);
  copy.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(contents);
      copy.textContent = "已复制";
    } catch {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(pre);
      selection.removeAllRanges();
      selection.addRange(range);
      copy.textContent = "已选中";
    }
    window.setTimeout(() => { copy.textContent = "复制"; }, 2000);
  });
  return block;
}

function renderOperation(operation, fields = []) {
  const wrapper = node("div", null, "reference-operation");
  const endpoint = node("div", null, "reference-endpoint");
  endpoint.append(node("span", operation.method, "reference-method"), node("code", operation.path, "reference-path"));
  wrapper.append(endpoint);
  if (operation.description) wrapper.append(node("p", operation.description, "reference-description"));
  wrapper.append(parametersTable(operation));
  const response = responseTable(operation);
  if (response) wrapper.append(response);
  const examples = section("调用示例");
  examples.append(codeBlock("cURL 请求", requestExample(operation)));
  if (operation.response_example !== null && operation.response_example !== undefined) {
    const body = typeof operation.response_example === "string"
      ? operation.response_example : JSON.stringify(operation.response_example, null, 2);
    examples.append(codeBlock("响应示例", body));
  }
  wrapper.append(examples);
  const dictionary = fieldTable(fields);
  if (dictionary) wrapper.append(dictionary);
  return wrapper;
}

function safeSource(url) {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" && parsed.hostname === "tushare.pro" ? parsed.href : null;
  } catch {
    return null;
  }
}

function show(content, title) {
  document.title = `${title} · 金融数据库 API`;
  $("reference-content").replaceChildren(content);
  $("reference-content").removeAttribute("aria-busy");
  const heading = content.querySelector(".reference-heading");
  if (state.focusHeading) heading?.focus();
  else if (state.scrollHeading && heading?.getBoundingClientRect?.().top < 0) heading.scrollIntoView({block: "start"});
  state.focusHeading = false;
  state.scrollHeading = false;
}

function renderOverview() {
  const reference = state.reference;
  const main = node("article");
  main.append(breadcrumbs(["API 文档", "使用说明"]), heading("使用说明", "浏览接口分类，查看每个数据集可用的筛选参数和返回字段。"));
  const authentication = section("认证与请求地址");
  const message = reference.authentication?.description || (reference.authentication?.required
    ? "请求时通过 X-API-Key 请求头携带查询密钥。" : "本机查询可直接发送请求。");
  authentication.append(node("p", message, "reference-description"));
  if (reference.limits?.requests_per_minute) {
    authentication.append(node("p", `当前密钥每分钟最多 ${reference.limits.requests_per_minute} 次调用。`, "reference-description"));
  }
  const guides = [
    ["发现接口", "先从数据集目录查找 ID，再查看数据集定义中的日期列、标的列和字段字典。"],
    ["筛选与翻页", "通过对应接口查看支持的筛选参数。翻页时保留原筛选条件，将响应里的 next_cursor 作为下一页的 cursor。"],
  ];
  authentication.append(docTable("调用步骤", ["用途", "说明"], guides));
  main.append(authentication);

  const preferred = reference.datasets.find((dataset) => dataset.id === "daily") || reference.datasets[0];
  if (preferred) {
    const starter = section("查看一个数据集");
    starter.append(docLink(preferred.title || preferred.id, docHref("dataset", preferred.id), {caption: preferred.id}));
    main.append(starter);
  }
  if (reference.errors?.length) {
    const errors = section("常见响应状态");
    errors.append(docTable("常见响应状态", ["HTTP 状态码", "说明"], reference.errors.map((error) => [error.status, error.description])));
    main.append(errors);
  }
  show(main, "使用说明");
}

function renderEndpoint(selection) {
  const group = state.reference.groups.find((item) => item.id === selection.group);
  const operation = group?.operations.find((item) => item.id === selection.operation);
  if (!operation) {
    renderUnavailable("这个接口不在当前可访问的文档范围内。");
    return;
  }
  const main = node("article");
  main.append(breadcrumbs(["API 文档", group.title]), heading(operation.title), renderOperation(operation));
  show(main, operation.title);
}

function renderUnavailable(message) {
  const main = node("article");
  main.append(breadcrumbs(["API 文档"]), heading("无法显示接口", message), docLink("返回使用说明", docHref("overview")));
  show(main, "无法显示接口");
}

function renderSignIn() {
  const placeholder = node("article");
  placeholder.append(heading("连接查询密钥", "连接后可浏览当前授权的接口分类、参数和字段。"));
  show(placeholder, "连接查询密钥");
}

function renderDataset(dataset, requestedOperation) {
  const main = node("article");
  main.append(breadcrumbs(["数据集接口", ...categoryPath(dataset).slice(0, -1)]), heading(dataset.title || dataset.id, dataset.description));
  const operations = dataset.operations || [];
  const operation = operations.find((item) => item.id === requestedOperation)
    || operations.find((item) => item.path.endsWith("/rows")) || operations[0];
  if (operations.length > 1) {
    const links = node("nav", null, "reference-operation-tabs tabs");
    links.setAttribute("aria-label", "数据集接口");
    for (const item of operations) {
      links.append(docLink(item.title, docHref("dataset", dataset.id, item.id), {active: item === operation}));
    }
    main.append(links);
  }
  if (operation) {
    const fields = operation.path.endsWith("/partitions") ? [] : dataset.fields;
    main.append(renderOperation(operation, fields));
  }
  const source = safeSource(dataset.source_url);
  if (source) {
    const footer = node("p", null, "reference-source");
    const link = linked("查看 Tushare 来源文档", source);
    link.rel = "noopener noreferrer";
    link.target = "_blank";
    footer.append(link);
    main.append(footer);
  }
  show(main, dataset.title || dataset.id);
}

async function renderSelection() {
  if (!state.reference) return;
  const selection = decodeRoute();
  const revision = ++state.viewRevision;
  renderNavigation(selection);
  report();
  if (selection.kind === "overview") {
    renderOverview();
    return;
  }
  if (selection.kind === "endpoint") {
    renderEndpoint(selection);
    return;
  }
  const dataset = state.reference.datasets.find((item) => item.id === selection.dataset);
  if (!dataset) {
    renderUnavailable("这个数据集不在当前可访问的文档范围内。");
    return;
  }
  try {
    let detail = state.details.get(dataset.id);
    if (!detail) {
      $("reference-content").setAttribute("aria-busy", "true");
      report("正在载入接口详情…");
      detail = await request(`/v1/api-reference/datasets/${encodeURIComponent(dataset.id)}`);
      if (revision !== state.viewRevision) return;
      state.details.set(dataset.id, detail);
    }
    renderDataset(detail, selection.operation);
    report();
  } catch (error) {
    if (revision !== state.viewRevision) return;
    if (error.status === 401) disconnectReference("查询密钥已失效，请重新连接。", true);
    else renderUnavailable(error.message);
  }
}

function authRequired() {
  return Boolean(state.reference?.authentication?.required ?? state.config?.api_key_required);
}

function renderAuthStatus() {
  $("reference-auth").hidden = !authRequired();
  const toggle = $("reference-auth-toggle");
  if (toggle) toggle.textContent = state.key ? "查询密钥已连接" : authRequired() ? "连接查询密钥" : "本机查询无需密钥";
  $("reference-key-form").hidden = !authRequired();
  const clear = $("reference-key-clear");
  if (clear) clear.hidden = !state.key;
  const explanation = $("reference-auth-description");
  if (explanation) {
    explanation.textContent = authRequired()
      ? "查询密钥保存在当前浏览器，可在数据目录和 API 文档中使用。"
      : "本机模式可直接查看接口和字段。";
  }
}

function disconnectReference(message = "", failed = false) {
  ++state.loadRevision;
  ++state.viewRevision;
  queryKeyStorage.remove(state.key);
  state.key = "";
  state.reference = null;
  state.details.clear();
  $("reference-search").disabled = true;
  $("reference-tree").replaceChildren();
  const count = $("reference-tree-count");
  if (count) count.textContent = "";
  const submit = $("reference-key-form").querySelector("button[type=submit]");
  if (submit) submit.disabled = false;
  $("reference-auth").open = true;
  renderAuthStatus();
  authMessage(message, failed);
  report();
  renderSignIn();
  $("reference-key").focus();
}

async function loadReference(key = "") {
  const revision = ++state.loadRevision;
  ++state.viewRevision;
  report("正在载入 API 文档…");
  const submit = $("reference-key-form").querySelector("button[type=submit]");
  if (submit) submit.disabled = true;
  try {
    const reference = await request("/v1/api-reference", key);
    if (revision !== state.loadRevision) return;
    state.key = key;
    const saved = !key || queryKeyStorage.save(key);
    state.reference = reference;
    state.details.clear();
    $("reference-search").disabled = false;
    renderAuthStatus();
    authMessage(!saved ? "已连接。当前浏览器无法保存查询密钥。" : "");
    if (key) $("reference-auth").open = !saved;
    await renderSelection();
  } catch (error) {
    if (revision !== state.loadRevision) return;
    if (error.status === 401) {
      queryKeyStorage.remove(key);
      const message = key ? "查询密钥无效，请检查后重试。" : "请输入查询用 API 密钥。";
      if (!state.reference || state.key === key) {
        disconnectReference(message, Boolean(key));
        return;
      }
      $("reference-auth").open = true;
      authMessage(message, Boolean(key));
    }
    if (state.reference) {
      report(error.message, "error");
    } else {
      renderUnavailable(error.message);
      report();
    }
  } finally {
    if (revision === state.loadRevision && submit) submit.disabled = false;
  }
}

async function initialize() {
  $("reference-base-url").textContent = location.origin;
  $("reference-search").disabled = true;
  try {
    state.config = await request("/app-config");
    $("reference-operations-nav").hidden = !state.config.operations;
    renderAuthStatus();
    const savedKey = authRequired() ? queryKeyStorage.read() : "";
    if (authRequired() && !savedKey) {
      $("reference-auth").open = true;
      renderSignIn();
      report();
      return;
    }
    await loadReference(savedKey);
  } catch (error) {
    renderUnavailable(error.message);
    report(error.message, "error");
  }
}

$("reference-key-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const field = $("reference-key");
  const key = field.value.trim();
  field.value = "";
  if (key) loadReference(key);
});

$("reference-key-clear")?.addEventListener("click", () => disconnectReference());
queryKeyStorage.listen((key) => {
  if (!authRequired()) return;
  if (!key) disconnectReference();
  else if (key !== state.key) void loadReference(key);
});

$("reference-search").addEventListener("input", () => renderNavigation());
document.querySelector(".reference-skip")?.addEventListener("click", (event) => {
  event.preventDefault();
  $("reference-content").focus();
});
window.addEventListener("hashchange", renderSelection);
initialize();
