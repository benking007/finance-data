"use strict";

const accountElement = (id) => document.getElementById(id);
const quotaNumber = new Intl.NumberFormat("zh-CN");
const accountPath = location.pathname;
const nextPage = new URLSearchParams(location.search).get("next");
const nextPaths = {operations: "/operations", users: "/admin/users"};
const authSuffix = Object.hasOwn(nextPaths, nextPage) ? `?next=${nextPage}` : "";
let viewer = null;

function showAccountStatus(id, message = "", failed = false) {
  const target = accountElement(id);
  target.textContent = message;
  target.classList.toggle("error", failed);
}

function page(section, title) {
  document.title = `${title} · 金融数据台`;
  showAccountStatus("account-status");
  accountElement(section).hidden = false;
}

async function accountRequest(path, method = "GET", data) {
  const request = {method, cache: "no-store"};
  if (data !== undefined) {
    request.headers = {"Content-Type": "application/json"};
    request.body = JSON.stringify(data);
  }
  let response;
  try {
    response = await fetch(path, request);
  } catch {
    throw new Error("无法连接站点，请重试。");
  }
  if (response.status === 204) return null;
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    const error = new Error(typeof body?.detail === "string" ? body.detail : `操作失败（${response.status}）`);
    error.status = response.status;
    throw error;
  }
  return body;
}

function renderAccount(user) {
  accountElement("account-welcome").textContent = `${user.username} · ${user.is_admin ? "管理员" : "普通用户"}`;
  accountElement("account-user-admin-link").hidden = !user.is_admin;
  accountElement("account-limit").textContent = quotaNumber.format(user.daily_limit);
  accountElement("account-used").textContent = quotaNumber.format(user.used_today);
  accountElement("account-remaining").textContent = quotaNumber.format(user.remaining_today);
  accountElement("account-create-key").textContent = user.has_api_key ? "重置密钥" : "生成密钥";
  accountElement("account-key-status").textContent = user.has_api_key
    ? `已启用 · ${user.api_key_mask || "••••"}` : "暂无密钥";
}

function clearIssuedKey() {
  accountElement("account-key-value").textContent = "";
  accountElement("account-new-key").hidden = true;
  showAccountStatus("account-copy-status");
}

async function submitCredentials(event, registering) {
  event.preventDefault();
  const form = event.currentTarget;
  const action = registering ? "register" : "login";
  const id = `account-${action}`;
  const username = accountElement(`${id}-name`).value.trim();
  const password = accountElement(`${id}-password`).value;
  if (registering && accountElement("account-register-confirm").value !== password) {
    showAccountStatus(`${id}-status`, "两次输入的密码不一致。", true);
    accountElement("account-register-confirm").focus();
    return;
  }
  const button = form.querySelector("button[type=submit]");
  button.disabled = true;
  showAccountStatus(`${id}-status`, registering ? "正在注册…" : "正在登录…");
  try {
    const {user} = await accountRequest(`/auth/${action}`, "POST", {username, password});
    form.reset();
    location.replace(user.is_admin && authSuffix ? nextPaths[nextPage] : "/account");
  } catch (error) {
    showAccountStatus(`${id}-status`, error.message, true);
    button.disabled = false;
  }
}

function appendCell(row, value, tag = "td") {
  const cell = document.createElement(tag);
  if (tag === "th") cell.scope = "row";
  cell.textContent = value;
  row.append(cell);
  return cell;
}

function renderUser(user) {
  const row = document.createElement("tr");
  appendCell(row, user.username, "th");
  appendCell(row, user.is_admin ? "管理员" : "普通用户");
  const used = appendCell(row, quotaNumber.format(user.used_today));
  const remaining = appendCell(row, quotaNumber.format(user.remaining_today));
  const limitCell = appendCell(row, "");
  const form = document.createElement("form");
  form.className = "account-limit-form";
  const limit = document.createElement("input");
  limit.type = "number";
  limit.step = "1";
  limit.min = "0";
  limit.max = "2147483647";
  limit.value = user.daily_limit;
  limit.required = true;
  limit.setAttribute("aria-label", `${user.username} 的每日额度`);
  const button = document.createElement("button");
  button.type = "submit";
  button.className = "button";
  button.textContent = "保存";
  button.disabled = true;
  limit.addEventListener("input", () => {
    button.disabled = !limit.validity.valid || Number(limit.value) === user.daily_limit;
  });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    button.disabled = true;
    limit.disabled = true;
    showAccountStatus("account-users-status", `正在保存 ${user.username} 的额度…`);
    try {
      const result = await accountRequest(`/auth/admin/users/${encodeURIComponent(user.id)}`, "PATCH", {daily_limit: Number(limit.value)});
      user = result.user;
      limit.value = user.daily_limit;
      used.textContent = quotaNumber.format(user.used_today);
      remaining.textContent = quotaNumber.format(user.remaining_today);
      showAccountStatus("account-users-status", `已保存 ${user.username} 的每日额度。`);
    } catch (error) {
      showAccountStatus("account-users-status", error.message, true);
      button.disabled = false;
    } finally {
      limit.disabled = false;
    }
  });
  form.append(limit, button);
  limitCell.append(form);
  return row;
}

accountElement("account-login-form").addEventListener("submit", (event) => void submitCredentials(event, false));
accountElement("account-register-form").addEventListener("submit", (event) => void submitCredentials(event, true));
accountElement("account-register-link").querySelector("a").href = `/register${authSuffix}`;
accountElement("account-signin-link").href = `/login${authSuffix}`;

accountElement("account-logout").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  button.disabled = true;
  try {
    await accountRequest("/auth/logout", "POST");
    clearIssuedKey();
    location.replace("/login");
  } catch (error) {
    showAccountStatus("account-status", error.message, true);
    button.disabled = false;
  }
});

accountElement("account-create-key").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  if (viewer.has_api_key && !window.confirm("重置会立即停用现有密钥。确定继续吗？")) return;
  button.disabled = true;
  clearIssuedKey();
  showAccountStatus("account-key-feedback", "正在生成密钥…");
  try {
    const result = await accountRequest("/account/api-key", "POST");
    viewer = result.user;
    renderAccount(viewer);
    accountElement("account-key-value").textContent = result.api_key;
    accountElement("account-new-key").hidden = false;
    showAccountStatus("account-key-feedback");
  } catch (error) {
    showAccountStatus("account-key-feedback", error.message, true);
  } finally {
    button.disabled = false;
  }
});

accountElement("account-copy-key").addEventListener("click", async () => {
  const code = accountElement("account-key-value");
  if (!code.textContent) return;
  try {
    await navigator.clipboard.writeText(code.textContent);
    showAccountStatus("account-copy-status", "已复制。");
  } catch {
    const range = document.createRange();
    range.selectNodeContents(code);
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
    showAccountStatus("account-copy-status", "已选中密钥，请使用复制快捷键。");
  }
});

window.addEventListener("pagehide", clearIssuedKey);
window.addEventListener("pageshow", (event) => {
  if (event.persisted) location.reload();
});

async function initializeAccount() {
  try {
    const session = await accountRequest("/auth/session");
    viewer = session.user;
    accountElement("account-nav").hidden = !viewer;
    if (accountPath === "/account") accountElement("account-nav").setAttribute("aria-current", "page");
    accountElement("account-logout").hidden = !viewer;
    accountElement("account-operations-nav").hidden = !(viewer?.is_admin && session.operations);
    if (viewer && (accountPath === "/login" || accountPath === "/register")) {
      location.replace(viewer.is_admin && authSuffix ? nextPaths[nextPage] : "/account");
    } else if (accountPath === "/login") {
      page("account-login", "登录");
    } else if (accountPath === "/register") {
      page("account-register", "注册账户");
    } else if (!viewer) {
      location.replace(accountPath === "/admin/users" ? "/login?next=users" : "/login");
    } else if (accountPath === "/admin/users" && !viewer.is_admin) {
      showAccountStatus("account-status", "此页面仅供管理员访问。", true);
    } else if (accountPath === "/admin/users") {
      page("account-users", "用户管理");
      showAccountStatus("account-users-status", "正在读取用户…");
      const result = await accountRequest("/auth/admin/users");
      accountElement("account-user-rows").replaceChildren(...result.users.map(renderUser));
      showAccountStatus("account-users-status", result.users.length ? "" : "暂无用户。");
    } else {
      renderAccount(viewer);
      page("account-overview", "我的账户");
    }
  } catch (error) {
    showAccountStatus("account-status", error.message, true);
    showAccountStatus("account-users-status");
  }
}

void initializeAccount();
