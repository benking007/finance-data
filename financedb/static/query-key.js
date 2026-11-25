"use strict";

const queryKeyStorage = (() => {
  const name = "financedb.query-api-key";

  function read() {
    try {
      return window.localStorage.getItem(name) || "";
    } catch {
      return "";
    }
  }

  function save(key) {
    try {
      window.localStorage.setItem(name, key);
      return true;
    } catch {
      return false;
    }
  }

  function remove(key) {
    try {
      if (key && window.localStorage.getItem(name) === key) window.localStorage.removeItem(name);
    } catch {
      // Storage can be disabled by the browser.
    }
  }

  function listen(callback) {
    window.addEventListener("storage", (event) => {
      if (event.key === name || event.key === null) callback(read());
    });
  }

  return {read, save, remove, listen};
})();
