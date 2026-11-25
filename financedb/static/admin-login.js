"use strict";

const form = document.getElementById("admin-login-form");
const password = document.getElementById("admin-password");
const submit = document.getElementById("admin-login-submit");
const loginFeedback = document.getElementById("admin-login-status");

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (submit.disabled) return;
  loginFeedback.textContent = "正在登录…";
  loginFeedback.classList.remove("error");
  submit.disabled = true;
  try {
    const response = await fetch("/ops/admin/login", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({password: password.value}),
    });
    if (response.ok) {
      password.value = "";
      location.replace("/operations");
      return;
    }
    const error = await response.json().catch(() => ({}));
    loginFeedback.textContent = typeof error.detail === "string" ? error.detail : "登录失败，请重试。";
    password.select();
  } catch {
    loginFeedback.textContent = "无法连接本地服务，请重试。";
  }
  loginFeedback.classList.add("error");
  submit.disabled = false;
});
