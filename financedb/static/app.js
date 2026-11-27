"use strict";

const element = (id) => document.getElementById(id);
const number = new Intl.NumberFormat("zh-CN");
const app = {
  config: null,
  key: "",
  keyPrompt: false,
  catalogStarted: false,
  datasets: [],
  sourceCatalog: null,
  selectedSource: null,
  expandedCatalog: new Set(),
  catalogDatabaseError: false,
  selected: null,
  details: null,
  catalogRequest: 0,
  rowRequest: 0,
  detailRequest: 0,
  query: null,
  cursors: [null],
  page: 0,
  rows: [],
  ops: null,
  opsRequest: 0,
  opsBusy: false,
  targetDirty: false,
  targetSubmitting: false,
  fullJob: null,
  scheduleCards: new Map(),
};

const jobModes = {sample: "样本验证", history: "历史回补", incremental: "增量采集"};
const statuses = {
  waiting: "等待", waiting_token: "等待 Token", pending: "等待", running: "运行中", queued: "排队中", starting: "正在启动",
  stopping: "正在停止", cancelling: "正在停止", stopped: "已停止", cancelled: "已停止",
  completed: "已完成", complete: "已完成", success: "成功", failed: "失败",
  partial: "部分完成", skipped: "已跳过", blocked: "受阻", interrupted: "已中断", unsupported: "未适配",
  unstarted: "待开始", done: "已完成", empty: "无新增", idle: "待开始",
};

function node(tag, content, className) {
  const result = document.createElement(tag);
  if (content !== undefined) result.textContent = content;
  if (className) result.className = className;
  return result;
}

function feedback(id, message = "", isError = false) {
  const target = typeof id === "string" ? element(id) : id;
  target.textContent = message;
  target.classList.toggle("error", isError);
}

function pill(id, label, state = "") {
  const target = typeof id === "string" ? element(id) : id;
  target.textContent = label;
  target.dataset.state = state;
}

function display(value) {
  if (value === undefined || value === null || value === "") return "—";
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}

function displayDate(value) {
  const text = display(value);
  return /^\d{8}$/.test(text) ? `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6)}` : text;
}

function displayTime(value) {
  if (!value) return "—";
  const time = new Date(value);
  if (Number.isNaN(time.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", {dateStyle: "short", timeStyle: "short", timeZone: "Asia/Shanghai"}).format(time);
}

function kv(target, pairs) {
  target.replaceChildren(...pairs.map(([key, value]) => {
    const item = node("div");
    item.append(node("dt", key), node("dd", value));
    return item;
  }));
}

function asTable(target, header, rows, empty, dataTable = false) {
  target.replaceChildren();
  if (!rows.length) {
    target.append(node("p", empty, "empty"));
    return;
  }
  const table = node("table", undefined, dataTable ? "data-table" : "");
  const headings = node("tr");
  for (const label of header) {
    const cell = node("th", label);
    cell.scope = "col";
    headings.append(cell);
  }
  const head = node("thead");
  head.append(headings);
  const body = node("tbody");
  for (const row of rows) {
    const line = node("tr");
    for (const value of row) {
      const cell = node("td");
      if (value instanceof Node) {
        cell.append(value);
      } else {
        cell.textContent = display(value);
        if (dataTable) cell.title = display(value);
      }
      line.append(cell);
    }
    body.append(line);
  }
  table.append(head, body);
  target.append(table);
}

async function request(path, options = {}, responseType = "json") {
  const headers = new Headers(options.headers || {});
  const key = path.startsWith("/v1/") ? app.key : "";
  if (key) headers.set("X-API-Key", key);
  if (options.body !== undefined) headers.set("Content-Type", "application/json");
  const response = await fetch(path, {...options, headers, cache: "no-store"});
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    const failure = new Error(typeof error.detail === "string" ? error.detail : `请求失败（${response.status}）`);
    failure.status = response.status;
    if (failure.status === 401 && key && key === app.key) disconnectQueryKey(failure.message);
    if (failure.status === 401 && path.startsWith("/ops/")) {
      location.replace("/login?next=operations");
    } else if (failure.status === 401 && !key && path.startsWith("/v1/") && app.config?.user) {
      location.replace("/login");
    }
    throw failure;
  }
  return responseType === "blob" ? response.blob() : response.json();
}

const collectionLabels = {
  configured: ["已配置", "ok"], pending_review: ["待适配", "warning"],
  realtime: ["实时接口", "busy"], file: ["文件导入", "busy"],
  discontinued: ["已停更", ""], derived: ["计算生成", ""],
  private: ["账户数据", ""], write: ["账户操作", ""],
};

function collectionStatus(entry) {
  if (entry.mode === "discontinued" && entry.collection_status === "configured") return ["停更历史已配置", "ok"];
  const special = ["realtime", "file", "discontinued", "derived", "private", "write"];
  const status = special.includes(entry.mode) ? entry.mode : entry.collection_status;
  const normalized = entry.profile_ids?.length ? "configured" : "pending_review";
  return collectionLabels[status] || collectionLabels[normalized];
}

function sourceAvailability(entry) {
  const [label, tone] = collectionStatus(entry);
  const tag = node("span", undefined, "state-pill catalog-entry-state");
  pill(tag, label, tone);
  return tag;
}

function makeCatalogBranches(entries) {
  const root = {groups: new Map(), items: []};
  for (const entry of entries) {
    const path = entry.path?.length > 1 ? entry.path : [entry.category, entry.title];
    let branch = root;
    for (const [index, title] of path.slice(0, -1).entries()) {
      if (!branch.groups.has(title)) {
        const group = {title, key: JSON.stringify(path.slice(0, index + 1)), groups: new Map(), items: [], count: 0};
        branch.groups.set(title, group);
        branch.items.push(group);
      }
      branch = branch.groups.get(title);
      branch.count += 1;
    }
    branch.items.push(entry);
  }
  return root.items;
}

function renderCatalogNode(item, options, level = 0) {
  const row = node("li");
  if (item.items) {
    row.className = "catalog-branch";
    const button = node("button", undefined, "catalog-branch-button");
    button.type = "button";
    const children = node("ul", undefined, "catalog-children");
    children.append(...item.items.map((child) => renderCatalogNode(child, options, level + 1)));
    const open = options.searching || (!level && options.classification) || app.expandedCatalog.has(item.key);
    const setExpanded = (expanded) => {
      button.setAttribute("aria-expanded", String(expanded));
      children.hidden = !expanded;
    };
    setExpanded(Boolean(open));
    button.append(node("i", undefined, "catalog-chevron"), node("span", item.title, "catalog-branch-title"),
      node("span", number.format(item.count), "catalog-branch-count"));
    button.addEventListener("click", () => {
      const expanded = children.hidden;
      if (expanded) app.expandedCatalog.add(item.key);
      else app.expandedCatalog.delete(item.key);
      setExpanded(expanded);
    });
    row.append(button, children);
    return row;
  }

  row.className = "catalog-leaf";
  const button = node("button", undefined, "catalog-leaf-button");
  button.type = "button";
  button.dataset.doc = String(item.doc_id);
  if (String(item.doc_id) === app.selectedSource) button.setAttribute("aria-current", "true");
  const title = node("div", undefined, "catalog-leaf-title");
  title.append(node("strong", item.title));
  if (item.api_names?.length) title.append(node("span", item.api_names.join(" · "), "catalog-api-name"));
  button.append(title, sourceAvailability(item));
  button.addEventListener("click", () => selectSource(item));
  row.append(button);
  return row;
}

function renderSourceList(list, term, classification) {
  const entries = app.sourceCatalog.entries;
  const shown = entries.filter((entry) => (!classification || entry.category === classification)
    && (!term || [entry.title, entry.doc_id, ...(entry.path || []), ...(entry.api_names || []),
      ...(entry.profile_ids || []), collectionStatus(entry)[0]].some((value) => String(value).toLocaleLowerCase().includes(term))));
  element("visible-count").textContent = `${number.format(shown.length)} 项`;
  list.classList.add("catalog-tree");
  list.replaceChildren(...makeCatalogBranches(shown).map((item) => renderCatalogNode(item, {searching: Boolean(term), classification})));
  if (!shown.length) list.append(node("li", "无匹配目录条目。", "no-items"));
}

function renderList() {
  const term = element("search").value.trim().toLocaleLowerCase();
  const classification = element("category").value;
  const list = element("datasets");
  if (app.sourceCatalog) {
    renderSourceList(list, term, classification);
    return;
  }
  list.classList.remove("catalog-tree");
  const shown = app.datasets.filter((dataset) => (
    (!classification || classification === dataset.category)
    && (!term || [dataset.id, dataset.title, dataset.category].some((value) => String(value).toLocaleLowerCase().includes(term)))
  ));
  element("visible-count").textContent = shown.length ? `${number.format(shown.length)} 项` : "";
  list.replaceChildren();
  if (!shown.length) {
    list.append(node("li", app.datasets.length ? "无匹配数据集。" : "暂无可浏览的数据集。", "no-items"));
  }
  for (const dataset of shown) {
    const entry = node("li");
    const button = node("button");
    button.type = "button";
    if (dataset.id === app.selected) button.setAttribute("aria-current", "true");
    button.append(node("strong", dataset.title), node("span", dataset.id));
    button.addEventListener("click", () => selectDataset(dataset.id));
    entry.append(button);
    list.append(entry);
  }
}

function updateCatalogMetrics(dataAvailable) {
  const collection = app.sourceCatalog;
  element("catalog-description").textContent = collection
    ? "浏览 Tushare 官方分类，查看采集配置和本地入库覆盖。"
    : "查看数据分类、字段定义和本地入库覆盖。";
  element("metric-datasets-label").textContent = collection ? "官方目录条目" : "查询目录";
  element("metric-configured-card").hidden = !collection;
  element("metric-configured-card").parentElement.classList.toggle("metrics-official", Boolean(collection));
  element("metric-datasets").textContent = number.format(collection?.entries.length ?? app.datasets.length);
  element("metric-configured").textContent = collection
    ? number.format(collection.configured_count ?? collection.entries.filter((entry) => entry.collection_status === "configured").length) : "—";
  element("metric-loaded").textContent = dataAvailable
    ? number.format(app.datasets.filter((entry) => entry.row_count > 0).length) : "—";
  element("metric-rows").textContent = dataAvailable
    ? number.format(app.datasets.reduce((sum, dataset) => sum + (dataset.row_count || 0), 0)) : "—";
}

function openSourceAncestors(entry) {
  const path = entry.path || [entry.category, entry.title];
  for (let index = 1; index < path.length; index += 1) app.expandedCatalog.add(JSON.stringify(path.slice(0, index)));
}

async function loadCatalog() {
  const sequence = ++app.catalogRequest;
  feedback("catalog-status", "正在载入目录…");
  try {
    const fullCatalog = app.config?.mode === "local" && (app.config.user?.is_admin || Boolean(app.key));
    const [dataResult, sourceResult] = await Promise.allSettled([
      request("/v1/datasets"), fullCatalog ? request("/v1/source-catalog") : Promise.resolve(null),
    ]);
    if (sequence !== app.catalogRequest) return false;
    app.sourceCatalog = sourceResult.status === "fulfilled" ? sourceResult.value : null;
    const dataAvailable = dataResult.status === "fulfilled";
    if (!dataAvailable && !app.sourceCatalog) throw dataResult.reason;
    app.catalogDatabaseError = !dataAvailable;
    app.datasets = dataAvailable ? (dataResult.value.datasets || []) : [];
    updateCatalogMetrics(dataAvailable);
    const category = element("category");
    const selected = category.value;
    const groups = [...new Set((app.sourceCatalog?.entries || app.datasets).map((dataset) => dataset.category))];
    category.replaceChildren(new Option("全部分类", ""), ...groups.map((title) => new Option(title, title)));
    if (groups.includes(selected)) category.value = selected;
    const sourceFailure = sourceResult.status === "rejected" && ![401, 403].includes(sourceResult.reason.status);
    const message = !dataAvailable ? dataResult.reason.message : sourceFailure ? "完整来源目录暂时无法载入。" : "";
    feedback("catalog-status", message, Boolean(message));
    app.keyPrompt = false;
    if (app.key && !queryKeyStorage.save(app.key) && !message) {
      feedback("catalog-status", "已连接。当前浏览器无法保存查询密钥。");
    }
    updateQueryKeyControls();
    if (app.sourceCatalog?.entries.length) {
      const entries = app.sourceCatalog.entries;
      const preferred = entries.find((entry) => String(entry.doc_id) === app.selectedSource)
        || entries.find((entry) => entry.profile_ids?.includes(app.selected || "daily")) || entries[0];
      openSourceAncestors(preferred);
      renderList();
      await selectSource(preferred, app.selected);
    } else if (app.datasets.length) {
      const preferred = app.datasets.find((dataset) => dataset.id === app.selected)
        || app.datasets.find((dataset) => dataset.id === "daily")
        || app.datasets[0];
      await selectDataset(preferred.id);
    } else {
      clearSelection("当前密钥暂无已开放的数据集。");
      renderList();
    }
    return true;
  } catch (error) {
    if (sequence !== app.catalogRequest) return false;
    if (error.status === 401) {
      revealKey(error.message);
      feedback("catalog-status");
    } else {
      feedback("catalog-status", error.message, true);
    }
    return false;
  }
}

function clearSelection(message) {
  app.selected = null;
  app.selectedSource = null;
  app.details = null;
  app.query = null;
  app.detailRequest += 1;
  app.rowRequest += 1;
  element("detail").hidden = true;
  element("detail-empty").hidden = false;
  element("detail-empty").replaceChildren(node("p", message));
}

function markSourceSelection() {
  for (const button of element("datasets").querySelectorAll(".catalog-leaf-button")) {
    if (button.dataset.doc === app.selectedSource) button.setAttribute("aria-current", "true");
    else button.removeAttribute("aria-current");
  }
}

function sourceProfileOptions(entry) {
  return (entry?.profile_ids || []).map((id) => app.datasets.find((profile) => profile.id === id)).filter(Boolean);
}

function renderSourceFields(entry, queryable) {
  const container = element("source-fields");
  const lists = [
    ["输入参数", entry?.input_fields || []],
    ["输出字段", entry?.output_fields || []],
  ].filter(([, fields]) => fields.length);
  container.hidden = !lists.length;
  container.open = !queryable;
  const list = element("source-field-lists");
  list.replaceChildren(...lists.map(([title, fields]) => {
    const section = node("section");
    const values = node("ul");
    values.append(...fields.map((field) => {
      const li = node("li");
      li.append(node("code", typeof field === "string" ? field : field.name));
      return li;
    }));
    section.append(node("h3", title), values);
    return section;
  }));
}

function unconfiguredDescription(entry) {
  if (entry.profile_ids?.length) {
    return app.catalogDatabaseError ? "目标数据库连接后可查看入库记录。" : "采集已配置，当前会话无法查询该数据集的入库记录。";
  }
  const descriptions = {
    realtime: "来源提供实时数据，离线查询功能尚未配置。",
    file: "来源为独立文件，导入后可查询历史记录。",
    discontinued: "来源接口已停更，官方文档保留其历史定义。",
    derived: "这是由其他行情或指标计算生成的数据。",
    private: "此接口读取账户私有数据。",
    write: "此接口用于管理账户数据。",
  };
  return descriptions[entry.mode] || "已登记官方文档和字段，采集配置待接入。";
}

function renderDatasetDetails(dataset, entry = null) {
  app.details = dataset;
  app.rows = [];
  app.query = null;
  const queryable = Boolean(dataset?.queryable);
  const fields = dataset?.fields || [];
  element("detail-empty").hidden = true;
  element("detail").hidden = false;
  element("dataset-title").textContent = entry?.title || dataset.title;
  element("dataset-category").textContent = entry?.path?.slice(0, -1).join(" / ") || dataset?.category || entry?.category || "";
  const identifier = entry?.api_names?.join(" / ") || dataset?.id || "";
  element("dataset-id").textContent = identifier;
  element("dataset-id").hidden = !identifier;
  element("dataset-description").textContent = dataset?.description || (entry ? unconfiguredDescription(entry) : "");
  const availability = element("dataset-availability");
  availability.hidden = !entry;
  if (entry) pill(availability, ...collectionStatus(entry));
  const profileOptions = sourceProfileOptions(entry);
  const picker = element("profile-picker");
  element("profile-picker-label").hidden = profileOptions.length < 2;
  picker.replaceChildren(...profileOptions.map((profile) => new Option(profile.id, profile.id)));
  if (dataset) picker.value = dataset.id;

  element("dataset-coverage").hidden = !dataset;
  if (dataset) {
    const ranges = dataset.min_date || dataset.max_date
      ? `${displayDate(dataset.min_date)} 至 ${displayDate(dataset.max_date)}` : "暂无";
    kv(element("dataset-coverage"), [
      ...(dataset.storage_table ? [["业务表", dataset.storage_table]] : []),
      ["已入库记录", number.format(dataset.row_count || 0)],
      ["已入库日期范围", ranges],
      ["最近同步", displayTime(dataset.last_sync)],
    ]);
  }
  const source = element("source-doc");
  const link = entry?.document_url || dataset?.source_url;
  source.hidden = !link || !/^https:\/\/tushare\.pro\//.test(link);
  if (!source.hidden) source.href = link;
  else source.removeAttribute("href");
  element("query-code").value = "";
  element("query-start").value = "";
  element("query-end").value = "";
  element("code-label").hidden = !dataset?.code_field;
  element("start-label").hidden = !dataset?.date_field;
  element("end-label").hidden = !dataset?.date_field;
  element("csv").hidden = !dataset?.can_export;
  element("csv").disabled = true;
  element("table-tab").hidden = !queryable;
  element("dictionary-tab").hidden = !fields.length;
  element("rows-container").replaceChildren();
  element("chart").hidden = true;
  element("page-text").textContent = "";
  if (fields.length) asTable(element("dictionary-pane"), ["字段", "类型", "单位", "定义"],
    fields.map((field) => [field.name, field.type, field.unit, field.description]), "此数据集暂无已登记的字段。");
  renderSourceFields(entry, queryable);
  showDictionary(!queryable);
  if (!fields.length) element("dictionary-pane").hidden = true;
  if (queryable) return beginQuery();
}

function selectSource(entry, preferred) {
  app.selectedSource = String(entry.doc_id);
  markSourceSelection();
  const profiles = sourceProfileOptions(entry);
  const profile = profiles.find((item) => item.id === preferred) || profiles[0];
  if (profile) return selectDataset(profile.id, entry);
  app.detailRequest += 1;
  app.rowRequest += 1;
  app.selected = null;
  renderDatasetDetails(null, entry);
}

async function selectDataset(id, entry = null) {
  const selection = ++app.detailRequest;
  app.rowRequest += 1;
  app.selected = id;
  if (!entry) {
    app.selectedSource = null;
    renderList();
  }
  try {
    const dataset = await request(`/v1/datasets/${encodeURIComponent(id)}`);
    if (selection !== app.detailRequest) return;
    await renderDatasetDetails(dataset, entry);
  } catch (error) {
    if (selection !== app.detailRequest) return;
    if (entry) {
      if (error.status === 503) app.catalogDatabaseError = true;
      renderDatasetDetails(null, entry);
      feedback("catalog-status", error.message, true);
    } else {
      clearSelection(error.message);
      renderList();
    }
  }
}

function showDictionary(open) {
  element("query-pane").hidden = open;
  element("dictionary-pane").hidden = !open;
  element("table-tab").setAttribute("aria-pressed", String(!open));
  element("dictionary-tab").setAttribute("aria-pressed", String(open));
  if (!open) drawChart(app.rows);
}

function queryParams() {
  const params = new URLSearchParams();
  const values = [
    ["code", element("query-code").value.trim()],
    ["start", element("query-start").value],
    ["end", element("query-end").value],
    ["limit", element("query-limit").value],
  ];
  for (const [key, value] of values) {
    if (value) params.set(key, value);
  }
  return params;
}

function beginQuery() {
  app.query = queryParams();
  app.cursors = [null];
  app.page = 0;
  return viewPage(0);
}

function rowsPath(cursor, format) {
  const params = new URLSearchParams(app.query);
  if (cursor) params.set("cursor", cursor);
  if (format) params.set("format", format);
  return `/v1/datasets/${encodeURIComponent(app.details.id)}/rows?${params}`;
}

async function viewPage(page) {
  if (!app.details || !app.query || page < 0 || page >= app.cursors.length) return;
  const sequence = ++app.rowRequest;
  feedback("query-status", "正在读取记录…");
  element("previous").disabled = true;
  element("next").disabled = true;
  try {
    if (!app.details.queryable) {
      element("page-text").textContent = "";
      element("csv").disabled = true;
      app.rows = [];
      drawChart([]);
      asTable(element("rows-container"), [], [], "此目录条目的查询适配尚未配置。");
      feedback("query-status");
      return;
    }
    const result = await request(rowsPath(app.cursors[page]));
    if (sequence !== app.rowRequest) return;
    app.page = page;
    app.rows = result.rows || [];
    const columns = result.fields || [];
    app.cursors.length = page + 1;
    if (result.next_cursor) app.cursors.push(result.next_cursor);
    asTable(element("rows-container"), columns, app.rows.map((row) => columns.map((field) => row[field])),
      "这个筛选范围暂无已入库记录。", true);
    drawChart(app.rows);
    element("page-text").textContent = `第 ${page + 1} 页 · ${number.format(app.rows.length)} 条`;
    element("previous").disabled = page === 0;
    element("next").disabled = !result.next_cursor;
    element("csv").disabled = app.rows.length === 0;
    feedback("query-status");
  } catch (error) {
    if (sequence !== app.rowRequest) return;
    element("page-text").textContent = "";
    element("csv").disabled = true;
    app.rows = [];
    element("rows-container").replaceChildren();
    drawChart([]);
    feedback("query-status", error.message, true);
  }
}

function drawChart(rows) {
  const chart = element("chart");
  const dateField = app.details?.date_field;
  const codeField = app.details?.code_field;
  const code = codeField && rows[0]?.[codeField];
  const single = code ? rows.filter((row) => row[codeField] === code) : rows;
  const definitions = [
    ["close", "收盘价"], ["unit_nav", "单位净值"], ["nav", "净值"],
    ["index_value", "指数值"], ["value", "数值"],
  ];
  const series = definitions.find(([name]) => single.filter((row) => row[dateField] && row[name] !== null
    && row[name] !== "" && Number.isFinite(Number(row[name]))).length >= 2);
  chart.hidden = !series;
  if (!series || element("query-pane").hidden || element("catalog-view").hidden) return;
  const [field, label] = series;
  const data = single.filter((row) => row[dateField] && row[field] !== null && row[field] !== ""
    && Number.isFinite(Number(row[field]))).sort((a, b) => String(a[dateField]).localeCompare(String(b[dateField])));
  element("chart-title").textContent = [code, label, "本页走势"].filter(Boolean).join(" · ");
  const canvas = element("chart-canvas");
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (!width || !height) return;
  canvas.width = Math.round(width * ratio);
  canvas.height = Math.round(height * ratio);
  const context = canvas.getContext("2d");
  if (!context) return;
  context.scale(ratio, ratio);
  const left = 49;
  const right = width - 12;
  const top = 13;
  const bottom = height - 27;
  const values = data.map((row) => Number(row[field]));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min;
  const position = (value) => range ? bottom - (value - min) / range * (bottom - top) : (top + bottom) / 2;
  context.font = "12px system-ui";
  context.fillStyle = "#52627e";
  context.strokeStyle = "#d6e4f6";
  for (let i = 0; i <= 3; i++) {
    const y = top + (bottom - top) / 3 * i;
    context.fillText(number.format(max - range / 3 * i), 0, y + 4, left - 8);
    context.beginPath();
    context.moveTo(left, y);
    context.lineTo(right, y);
    context.stroke();
  }
  const dateText = (row) => displayDate(row[dateField]).slice(0, 10);
  context.fillText(dateText(data[0]), left, height - 6);
  const trailing = dateText(data.at(-1));
  context.fillText(trailing, right - context.measureText(trailing).width, height - 6);
  context.strokeStyle = "#1768e7";
  context.lineWidth = 2;
  context.beginPath();
  values.forEach((value, index) => {
    const x = left + index / (values.length - 1) * (right - left);
    const y = position(value);
    if (index === 0) context.moveTo(x, y);
    else context.lineTo(x, y);
  });
  context.stroke();
}

function updateQueryKeyControls() {
  const visible = !element("catalog-view").hidden;
  element("key-panel").hidden = !visible || !app.keyPrompt;
  element("reconnect-key").hidden = !visible || !app.key || app.keyPrompt;
}

function revealKey(message = "") {
  app.keyPrompt = true;
  updateQueryKeyControls();
  app.datasets = [];
  app.rows = [];
  app.sourceCatalog = null;
  updateCatalogMetrics(false);
  element("metric-datasets").textContent = "—";
  clearSelection("连接查询 API 后可浏览数据。");
  renderList();
  feedback("key-status", message, Boolean(message));
  if (!element("catalog-view").hidden) element("access-key").focus();
}

function disconnectQueryKey(message = "") {
  queryKeyStorage.remove(app.key);
  app.key = "";
  ++app.catalogRequest;
  feedback("catalog-status");
  revealKey(message);
}

function connectCatalog() {
  updateQueryKeyControls();
  if (app.catalogStarted || !app.config || element("catalog-view").hidden) return;
  app.catalogStarted = true;
  if (app.config.api_key_required && !app.key) revealKey();
  else void loadCatalog();
}

function switchView() {
  if (app.config?.mode === "local" && location.pathname === "/" && location.hash === "#operations") {
    location.replace("/operations");
    return;
  }
  const requestedOps = app.config?.operations && location.pathname === "/operations";
  element("catalog-view").hidden = Boolean(requestedOps);
  element("operations-view").hidden = !requestedOps;
  document.querySelectorAll("[data-view]").forEach((link) => {
    if (link.dataset.view === (requestedOps ? "operations" : "catalog")) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  if (requestedOps) pollOperations();
  else drawChart(app.rows);
  void connectCatalog();
}

function jobLabel(status) {
  return statuses[status] || status || "—";
}

function jobActive(status) {
  return ["queued", "starting", "running", "stopping", "cancelling"].includes(status);
}

function databaseReady(state) {
  return !state.target || state.target.status === "ready";
}

function jobBlockedReason(state) {
  if (app.config.demo) return "演示环境仅供查看。";
  if (jobActive(state.job?.status)) return "当前采集任务结束后可启动。";
  if (state.target?.status === "needs_password") return "请在目标数据库中输入 MySQL 密码并连接。";
  if (!databaseReady(state)) return state.target?.error || "请先初始化或重新连接目标数据库。";
  if (!state.has_token) return "请先在 Tushare 连接中填写 Token。";
  return "";
}

function updateDatabaseTargetControls(state) {
  const choicesDisabled = !state?.target || app.config.demo || app.targetSubmitting || jobActive(state.job?.status);
  const showMySQL = element("database-target-mysql").checked;
  element("database-target-choices").disabled = choicesDisabled;
  const fields = element("database-mysql-fields");
  fields.hidden = !showMySQL;
  fields.disabled = choicesDisabled || !showMySQL;
  const submit = element("database-target-submit");
  submit.disabled = choicesDisabled;
  submit.title = jobActive(state?.job?.status) ? "采集任务结束后可切换目标数据库" : "";
}

function renderDatabaseTarget(state) {
  const target = state.target;
  const mysql = target?.mysql;
  const labels = {ready: ["已就绪", "ok"], needs_password: ["待输入密码", "warning"], unavailable: ["连接不可用", "error"]};
  const [label, tone] = labels[target?.status] || ["待连接", ""];
  pill("database-target-status", app.config.demo ? "演示库" : label, app.config.demo ? "" : tone);

  let summary = "";
  if (target?.kind === "mysql") {
    const host = mysql?.host;
    const address = host?.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
    const destination = [address && `${address}:${mysql.port}`, mysql?.database].filter(Boolean).join(" / ");
    summary = `当前目标为 MySQL${destination ? ` · ${destination}` : ""}${target.password_saved ? " · 密码已加密保存" : ""}`;
  } else if (target?.kind === "sqlite") {
    summary = `当前目标为本地 SQLite${target.sqlite_path ? ` · ${target.sqlite_path}` : ""}`;
  }
  element("database-target-current").textContent = [summary, target?.error].filter(Boolean).join("。 ");

  if (!app.targetDirty) {
    element("database-target-sqlite").checked = target?.kind !== "mysql";
    element("database-target-mysql").checked = target?.kind === "mysql";
    element("database-mysql-host").value = mysql?.host || "127.0.0.1";
    element("database-mysql-port").value = mysql?.port || 3306;
    element("database-mysql-name").value = mysql?.database || "";
    element("database-mysql-user").value = mysql?.username || "";
    element("database-mysql-tls").checked = Boolean(mysql?.tls);
    element("database-mysql-remember").checked = !mysql || target.status === "needs_password" || Boolean(target.password_saved);
    element("database-mysql-password").placeholder = target?.password_saved ? "留空使用已保存的密码" : "";
  }
  updateDatabaseTargetControls(state);
}

function targetIdentity(target) {
  if (!target) return null;
  const {kind, status, sqlite_path, mysql} = target;
  return JSON.stringify([kind, status, sqlite_path, mysql?.host, mysql?.port, mysql?.database, mysql?.username, mysql?.tls]);
}

function resetForDatabaseTarget(state) {
  app.fullJob = null;
  for (const card of app.scheduleCards.values()) card.panel.remove();
  app.scheduleCards.clear();
  app.catalogRequest += 1;
  app.datasets = [];
  app.rows = [];
  element("rows-container").replaceChildren();
  updateCatalogMetrics(false);
  const ready = databaseReady(state);
  const source = app.selectedSource;
  clearSelection(ready ? "正在读取目标数据库…" : "请先在数据运营中初始化目标数据库。");
  app.selectedSource = source;
  renderList();
  if (ready && app.catalogStarted && (!app.config.api_key_required || app.key)) {
    void loadCatalog();
  } else if (!ready && app.sourceCatalog) {
    app.catalogDatabaseError = true;
    const entry = app.sourceCatalog.entries.find((item) => String(item.doc_id) === source);
    if (entry) selectSource(entry);
  }
}

function statusKind(status) {
  if (jobActive(status)) return "busy";
  if (["failed", "blocked", "interrupted"].includes(status)) return "error";
  if (["partial", "waiting_token"].includes(status)) return "warning";
  if (["completed", "complete", "success", "done", "empty", "skipped"].includes(status)) return "ok";
  return "";
}

function badge(status, fallback) {
  const result = node("span", undefined, "state-pill");
  pill(result, status ? jobLabel(status) : fallback, statusKind(status));
  return result;
}

function sumGroups(groups, calculate) {
  return groups.reduce((sum, group) => sum + (calculate(group) || 0), 0);
}

function processedDatasets(groups, datasets, total) {
  if (datasets.length !== total) return sumGroups(groups, (group) => group.datasets_finished);
  const unprocessed = ["queued", "pending", "waiting", "unstarted", "cancelled", "stopped", "interrupted", "running", "starting", "stopping"];
  return datasets.filter((dataset) => dataset.status && !unprocessed.includes(dataset.status)).length;
}

function progressOf(groups, datasets = [], status) {
  if (groups.length && groups.every((group) => Number.isInteger(group.partitions_total) && group.partitions_total >= 0)) {
    const total = sumGroups(groups, (group) => group.partitions_total);
    if (total > 0) {
      return {done: sumGroups(groups, (group) => (group.partitions_done || 0) + (group.partitions_failed || 0)), total, unit: "分片"};
    }
  }
  const total = sumGroups(groups, (group) => group.datasets_total);
  if (total > 0) return {done: processedDatasets(groups, datasets, total), total, unit: "数据集"};
  return status && !["complete", "completed", "success"].includes(status) ? null : {done: Number(Boolean(status)), total: 1};
}

function drawProgress(progress, label, measure, status) {
  progress.dataset.state = statusKind(status);
  if (!measure) {
    progress.removeAttribute("value");
    progress.removeAttribute("title");
    progress.removeAttribute("aria-valuetext");
    label.textContent = jobActive(status) ? "统计中" : "—";
    return;
  }
  progress.max = measure.total;
  progress.value = Math.min(measure.total, measure.done);
  progress.title = measure.unit ? `已处理 ${number.format(measure.done)} / ${number.format(measure.total)} ${measure.unit}` : "";
  if (progress.title) progress.setAttribute("aria-valuetext", progress.title);
  else progress.removeAttribute("aria-valuetext");
  label.textContent = `${Math.floor((progress.value / measure.total) * 100)}%`;
}

function datasetIssue(datasets) {
  const issue = datasets.find((dataset) => dataset.message && (dataset.failed > 0
    || ["failed", "blocked", "partial", "unsupported"].includes(dataset.status)));
  if (!issue) return "";
  const label = issue.title || issue.dataset_id;
  return label ? `${label} · ${issue.message}` : issue.message;
}

function resultDetails(group, datasets = [], currentDataset) {
  const details = node("div", undefined, "stage-result");
  const issue = datasetIssue(datasets);
  if (currentDataset) details.append(node("strong", `正在采集 ${currentDataset}`));
  else if (group.waiting_for?.length) details.append(node("span", `等待依赖 ${group.waiting_for.join("、")}`));
  const metrics = [];
  if (group.datasets_total) metrics.push(`${number.format(processedDatasets([group], datasets, group.datasets_total))} / ${number.format(group.datasets_total)} 个数据集`);
  if (group.partitions_done) metrics.push(`${number.format(group.partitions_done)} 个分片完成`);
  if (group.partitions_failed) metrics.push(`${number.format(group.partitions_failed)} 个分片失败`);
  if (group.rows) metrics.push(`${number.format(group.rows)} 行入库`);
  details.append(node("span", metrics.join(" · ") || "—"));
  if (issue) details.append(node("span", issue, "stage-issue"));
  return details;
}

function isFullJob(entry) {
  return entry?.is_full ?? (entry?.mode === "history" && !entry.start && !entry.end);
}

function renderFullJob(state, current) {
  const job = isFullJob(current) ? current : state.full_job || app.fullJob || (state.history || []).find(isFullJob);
  if (job) app.fullJob = job;
  const status = job?.status;
  const groups = job?.categories || [];
  const datasets = job?.datasets || [];
  pill("job-state", status ? jobLabel(status) : "待开始", statusKind(status));
  const button = element("job-submit");
  const blocked = jobBlockedReason(state);
  button.disabled = Boolean(blocked);
  button.title = blocked;
  button.textContent = ["cancelled", "interrupted"].includes(status) ? "继续采集"
    : status && !jobActive(status) ? "重新执行" : "启动全量";
  element("job-blocked").textContent = jobActive(current?.status) ? "" : blocked;
  const stop = element("job-stop");
  stop.hidden = !isFullJob(current) || !jobActive(current.status);
  stop.disabled = app.config.demo || !["queued", "starting", "running"].includes(current?.status);
  const timing = [job?.started_at && `开始 ${displayTime(job.started_at)}`, job?.finished_at && `结束 ${displayTime(job.finished_at)}`].filter(Boolean);
  element("job-timing").textContent = timing.join(" · ");
  element("job-timing").hidden = !timing.length;
  element("job-message").textContent = job?.message || "各分类并行回补历史记录。重新执行时跳过已提交的分片。";
  drawProgress(element("job-overall-progress"), element("job-overall-percent"), progressOf(groups, datasets, status), status);

  const stages = groups.length ? groups : schedulesFrom(state).map((item) => ({name: item.category, status: "idle", datasets_total: item.datasets_total}));
  asTable(element("job-progress"), ["阶段", "状态", "进度 / 结果"], stages.map((group) => {
    const entries = datasets.filter((dataset) => dataset.category === group.name);
    const currentDataset = group.current_dataset ?? entries.find((dataset) => dataset.status === "running")?.dataset_id;
    const waiting = group.status === "queued" && !group.waiting_for?.length
      || entries.length && entries.every((item) => item.status === "unstarted");
    const stageStatus = waiting ? "unstarted" : group.status;
    return [group.name, badge(stageStatus, "待开始"), resultDetails(group, entries, currentDataset)];
  }), "启动后显示各分类的采集进度。");
}

function createScheduleCard(category) {
  const panel = node("article", undefined, "panel schedule-card");
  const header = node("header", undefined, "schedule-card-header");
  const actions = node("div", undefined, "schedule-card-actions");
  const summary = badge(null, "未启用");
  const start = node("button", "启动", "primary schedule-start");
  start.type = "button";
  start.setAttribute("aria-label", `启动${category}增量采集`);
  const stop = node("button", "停止", "button schedule-stop");
  stop.type = "button";
  stop.setAttribute("aria-label", `停止${category}增量采集`);
  actions.append(summary, start, stop);
  header.append(node("h3", category), actions);
  const body = node("div", undefined, "schedule-card-body");
  const description = node("p", undefined, "muted schedule-description");
  const progressLine = node("div", undefined, "progress-cell schedule-progress");
  const progress = node("progress");
  progress.setAttribute("aria-label", `${category}增量进度`);
  const percent = node("span");
  progressLine.append(progress, percent);
  const result = node("p", undefined, "muted schedule-result");

  const form = node("form", undefined, "inline-form schedule-controls");
  const enableLabel = node("label", undefined, "checkbox");
  const enable = node("input");
  enable.type = "checkbox";
  enable.setAttribute("aria-label", `启用${category}定时增量`);
  enableLabel.append(enable, document.createTextNode("启用定时"));
  const timeLabel = node("label", "执行时间");
  const time = node("input");
  time.type = "time";
  time.required = true;
  time.setAttribute("aria-label", `${category}执行时间`);
  timeLabel.append(time);
  const save = node("button", "保存计划", "button");
  save.type = "submit";
  form.append(enableLabel, timeLabel, save);
  const meta = node("dl", undefined, "ops-kv schedule-meta");
  const status = node("p", undefined, "feedback schedule-card-feedback");
  status.setAttribute("role", "status");
  body.append(description, progressLine, result, form, meta, status);
  panel.append(header, body);

  const card = {panel, summary, start, stop, description, progress, percent, result, enable, time, save, meta, status, saved: null, dirty: false};
  form.addEventListener("input", () => { card.dirty = !card.saved || enable.checked !== card.saved.enabled || time.value !== card.saved.time; });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const body = {category, enabled: enable.checked, time: time.value};
    const saved = await performOperation("/ops/category-schedules", "PUT", body, "计划已保存。", status);
    if (saved && enable.checked === body.enabled && time.value === body.time) card.dirty = false;
  });
  start.addEventListener("click", () => performOperation("/ops/category-jobs", "POST", {category}, "增量任务已提交。", status));
  stop.addEventListener("click", () => performOperation("/ops/jobs/stop", "POST", undefined, "已请求停止。", status));
  return card;
}

function updateScheduleCard(card, schedule, state, viewOnly) {
  const scheduled = state.job?.scheduled_categories || {};
  const current = state.job?.category === schedule.category || scheduled[schedule.category] ? state.job : null;
  const activeHere = jobActive(current?.status);
  const status = activeHere ? schedule.last_status || current.status : schedule.queued ? "queued" : schedule.last_status;
  const fallback = schedule.enabled ? "待执行" : "未启用";
  pill(card.summary, status ? jobLabel(status) : fallback, statusKind(status));
  const blocked = jobBlockedReason(state);
  card.start.hidden = activeHere;
  card.start.disabled = Boolean(blocked);
  card.start.title = blocked;
  const batch = Object.keys(scheduled).length > 1;
  card.stop.textContent = batch ? "停止本批" : "停止";
  card.stop.setAttribute("aria-label", batch ? "停止本批分类增量采集" : `停止${schedule.category}增量采集`);
  card.stop.hidden = !activeHere || !jobActive(status);
  card.stop.disabled = viewOnly || !["queued", "starting", "running"].includes(current?.status);
  card.description.textContent = `增量更新 ${number.format(schedule.datasets_total || 0)} 个已配置数据集。`;
  const progress = schedule.progress && !schedule.queued ? [schedule.progress] : [];
  const datasetProgress = current?.datasets?.filter((item) => item.category === schedule.category) || [];
  drawProgress(card.progress, card.percent, progressOf(progress, datasetProgress, schedule.queued ? null : status), status);
  const issue = !schedule.queued && datasetIssue(datasetProgress);
  const currentDataset = activeHere && schedule.progress?.current_dataset;
  const dependencies = activeHere && schedule.progress?.waiting_for || [];
  const result = issue || (currentDataset ? `正在采集 ${currentDataset}`
    : dependencies.length ? `等待依赖 ${dependencies.join("、")}`
    : schedule.queued ? "已到执行时间，等待当前采集结束。" : schedule.last_message);
  card.result.textContent = result || "";
  card.result.hidden = !result;
  card.result.classList.toggle("stage-issue", Boolean(issue));

  const latest = {enabled: Boolean(schedule.enabled), time: schedule.time || "21:30"};
  card.saved = latest;
  card.dirty = card.dirty && (card.enable.checked !== latest.enabled || card.time.value !== latest.time);
  if (!card.dirty) {
    card.enable.checked = latest.enabled;
    card.time.value = latest.time;
  }
  card.enable.disabled = viewOnly || !databaseReady(state);
  card.time.disabled = viewOnly || !databaseReady(state);
  card.save.disabled = viewOnly || !databaseReady(state);
  kv(card.meta, [
    ["下次运行", displayTime(schedule.next_run)],
    ["上次启动", displayTime(schedule.last_started_at)],
    ["上次结果", schedule.last_status ? jobLabel(schedule.last_status) : "—"],
  ]);
}

function schedulesFrom(state) {
  const suggestions = Object.entries(state.catalog_coverage || {}).filter(([, entry]) => entry.configured > 0);
  return state.category_schedules || suggestions.map(([category, entry]) => ({category, datasets_total: entry.configured}));
}

function renderScheduleCards(state) {
  const target = element("category-schedule-grid");
  const viewOnly = app.config.demo || !Array.isArray(state.category_schedules);
  const schedules = schedulesFrom(state);
  const active = new Set(schedules.map((item) => item.category));
  for (const [category, card] of app.scheduleCards) {
    if (!active.has(category)) {
      card.panel.remove();
      app.scheduleCards.delete(category);
    }
  }
  feedback("category-schedule-feedback", schedules.length ? "" : "暂无可定时采集的分类。");
  for (const schedule of schedules) {
    let card = app.scheduleCards.get(schedule.category);
    if (!card) {
      card = createScheduleCard(schedule.category);
      app.scheduleCards.set(schedule.category, card);
      target.append(card.panel);
    }
    updateScheduleCard(card, schedule, state, viewOnly);
  }
}

function renderOperations(state) {
  const previousTarget = targetIdentity(app.ops?.target);
  const previouslySavedToken = app.ops?.token_saved;
  if (previousTarget && previousTarget !== targetIdentity(state.target)) resetForDatabaseTarget(state);
  app.ops = state;
  renderDatabaseTarget(state);
  pill("token-state", state.token_saved ? "已加密保存" : state.has_token ? "已配置" : "待配置", state.has_token ? "ok" : "");
  if (state.token_saved !== previouslySavedToken) element("remember-token").checked = Boolean(state.token_saved);
  element("clear-token").disabled = app.config.demo || !state.has_token || jobActive(state.job?.status);
  renderFullJob(state, state.job);
  renderScheduleCards(state);
  asTable(element("job-history"), ["任务", "开始时间", "结束时间", "状态", "结果"],
    (state.history || []).map((entry) => [
      entry.category ? `${entry.category}增量` : entry.trigger === "category_schedule"
        ? `定时增量（${number.format(Object.keys(entry.scheduled_categories || {}).length)} 类）`
        : isFullJob(entry) ? "全量离线" : jobModes[entry.mode] || entry.mode,
      displayTime(entry.started_at), displayTime(entry.finished_at), badge(entry.status, "—"), entry.message,
    ]), "暂无运行记录。");
  asTable(element("ops-coverage"), ["分类", "目录条目", "已配置", "待适配", "其他交付"],
    Object.entries(state.catalog_coverage || {}).map(([name, value]) => [
      name, number.format(value.total), number.format(value.configured),
      number.format(value.pending_review), number.format(value.other),
    ]), "暂无目录覆盖信息。");
}

async function pollOperations() {
  if (!app.config?.operations || app.opsBusy || element("operations-view").hidden) return;
  const revision = ++app.opsRequest;
  try {
    const state = await request("/ops/state");
    if (revision === app.opsRequest) renderOperations(state);
  } catch (error) {
    if (revision === app.opsRequest) feedback("ops-status", error.message, true);
  }
}

async function performOperation(path, method, body, success, target = "ops-status") {
  if (app.opsBusy) return false;
  app.opsBusy = true;
  app.opsRequest += 1;
  feedback(target, "正在提交…");
  try {
    const options = {method};
    if (body !== undefined) options.body = JSON.stringify(body);
    const result = await request(path, options);
    const state = result.has_token === undefined ? await request("/ops/state") : result;
    renderOperations(state);
    feedback(target, success);
    return true;
  } catch (error) {
    feedback(target, error.message, true);
    return false;
  } finally {
    app.opsBusy = false;
  }
}

element("refresh").addEventListener("click", loadCatalog);
element("ops-refresh").addEventListener("click", pollOperations);
element("search").addEventListener("input", renderList);
element("category").addEventListener("change", () => {
  renderList();
  const category = element("category").value;
  if (!category || !app.sourceCatalog) return;
  const entries = app.sourceCatalog.entries;
  const current = entries.find((item) => String(item.doc_id) === app.selectedSource);
  const preferred = entries.find((item) => item.category === category);
  if (preferred && current?.category !== category) {
    openSourceAncestors(preferred);
    renderList();
    selectSource(preferred);
  }
});
element("profile-picker").addEventListener("change", () => {
  const entry = app.sourceCatalog?.entries.find((item) => String(item.doc_id) === app.selectedSource);
  if (entry) selectSource(entry, element("profile-picker").value);
});
element("table-tab").addEventListener("click", () => showDictionary(false));
element("dictionary-tab").addEventListener("click", () => showDictionary(true));
element("query-form").addEventListener("submit", (event) => {
  event.preventDefault();
  beginQuery();
});
element("previous").addEventListener("click", () => viewPage(app.page - 1));
element("next").addEventListener("click", () => viewPage(app.page + 1));
element("csv").addEventListener("click", async () => {
  if (!app.details?.can_export) return;
  const filename = `${app.details.id}.csv`;
  try {
    const file = await request(rowsPath(app.cursors[app.page], "csv"), {}, "blob");
    const url = URL.createObjectURL(file);
    const download = node("a");
    download.href = url;
    download.download = filename;
    document.body.append(download);
    download.click();
    download.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (error) {
    feedback("query-status", error.message, true);
  }
});
element("reconnect-key").addEventListener("click", () => {
  disconnectQueryKey();
});
element("key-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const field = element("access-key");
  const key = field.value.trim();
  field.value = "";
  if (key) {
    app.key = key;
    await loadCatalog();
  }
});
queryKeyStorage.listen((key) => {
  if (!app.config?.api_key_required || app.key === key) return;
  if (!key) {
    disconnectQueryKey();
    return;
  }
  revealKey();
  ++app.catalogRequest;
  app.key = key;
  app.keyPrompt = false;
  app.catalogStarted = false;
  connectCatalog();
});
element("token-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const input = element("token");
  const token = input.value.trim();
  input.value = "";
  if (token) await performOperation("/ops/token", "POST", {token, remember: element("remember-token").checked}, "Tushare Token 已配置。");
});
element("clear-token").addEventListener("click", () => performOperation("/ops/token", "DELETE", undefined, "Tushare Token 已清除。"));
element("database-target-form").addEventListener("input", () => {
  app.targetDirty = true;
  if (!element("database-target-mysql").checked) element("database-mysql-password").value = "";
  feedback("database-target-feedback");
  updateDatabaseTargetControls(app.ops);
});
element("database-target-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (app.opsBusy || element("database-target-submit").disabled) return;
  const previousTarget = targetIdentity(app.ops?.target);
  const mysql = element("database-target-mysql").checked;
  const body = {kind: mysql ? "mysql" : "sqlite"};
  const password = element("database-mysql-password");
  if (mysql) {
    body.mysql = {
      host: element("database-mysql-host").value.trim(),
      port: element("database-mysql-port").valueAsNumber,
      database: element("database-mysql-name").value.trim(),
      username: element("database-mysql-user").value.trim(),
      ...(password.value ? {password: password.value} : {}),
      tls: element("database-mysql-tls").checked,
      remember_password: element("database-mysql-remember").checked,
    };
  }
  password.value = "";
  app.targetSubmitting = true;
  updateDatabaseTargetControls(app.ops);
  try {
    const saved = await performOperation("/ops/database-target", "POST", body,
      `已初始化并使用${mysql ? " MySQL" : "本地 SQLite"}。`, "database-target-feedback");
    if (saved) {
      app.targetDirty = false;
      if (previousTarget && previousTarget === targetIdentity(app.ops?.target)) {
        resetForDatabaseTarget(app.ops);
        renderOperations(app.ops);
      }
    }
  } finally {
    app.targetSubmitting = false;
    if (app.ops) renderDatabaseTarget(app.ops);
  }
});
element("job-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  await performOperation("/ops/jobs", "POST", {mode: "history"}, "全量离线任务已提交。");
});
element("job-stop").addEventListener("click", () => {
  performOperation("/ops/jobs/stop", "POST", undefined, "已请求停止。");
});
window.addEventListener("hashchange", switchView);
window.addEventListener("resize", () => drawChart(app.rows));
window.setInterval(() => { if (!document.hidden) pollOperations(); }, 2000);

async function initialize() {
  try {
    app.config = await request("/app-config");
    if (app.config.api_key_required) app.key = queryKeyStorage.read();
    element("ops-nav").hidden = !app.config.operations;
    if (app.config.user) {
      const accountNav = element("account-nav");
      accountNav.href = "/account";
      accountNav.textContent = "我的";
    }
    element("mode-text").textContent = app.config.mode === "published" ? "数据发布服务" : app.config.demo ? "SQLite 模拟演示" : "本地数据库";
    element("catalog-demo").hidden = !app.config.demo;
    element("ops-demo").hidden = !app.config.demo;
    if (app.config.demo) {
      for (const control of document.querySelectorAll("#token-form input, #token-form button, #job-form button")) {
        control.disabled = true;
      }
    }
    switchView();
  } catch (error) {
    element("mode-text").textContent = "连接失败";
    feedback("catalog-status", error.message, true);
  }
}

initialize();
