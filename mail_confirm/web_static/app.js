const typeLabels = {
  interval: "По времени",
  schedule: "По расписанию",
  count: "По порогу",
  keyword: "Кодовое слово",
};

const statusLabels = { active: "Активна", paused: "Пауза" };

const app = document.getElementById("app");
const toastEl = document.getElementById("toast");
const globalSearch = document.getElementById("global-search");


function esc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

function showToast(text, err = false) {
  toastEl.textContent = text;
  toastEl.className = "toast" + (err ? " err" : "");
  toastEl.hidden = false;
  clearTimeout(showToast._t);
  showToast._t = setTimeout(() => {
    toastEl.hidden = true;
  }, 4000);
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "Ошибка запроса");
  return data;
}

function secondsToDays(sec) {
  const n = Number(sec) || 0;
  if (n <= 0) return 1;
  return Math.max(1, Math.round(n / 86400));
}

function pluralDays(n) {
  const abs = Math.abs(n) % 100;
  const last = abs % 10;
  if (abs > 10 && abs < 20) return "дней";
  if (last === 1) return "день";
  if (last >= 2 && last <= 4) return "дня";
  return "дней";
}

function describeTrigger(r) {
  const c = r.trigger_config || {};
  if (r.trigger_type === "interval") {
    const days = secondsToDays(c.interval_seconds || r.interval_seconds);
    return `${days} ${pluralDays(days)}`;
  }
  if (r.trigger_type === "schedule") {
    return `${c.day_of_month || 1}-е, ${String(c.hour || 0).padStart(2, "0")}:${String(c.minute || 0).padStart(2, "0")} (${c.timezone || "Europe/Moscow"})`;
  }
  if (r.trigger_type === "count") return `от ${c.min_count || 1} писем`;
  if (r.trigger_type === "keyword") return `"${c.phrase || ""}"`;
  return "";
}

function formatDt(s) {
  if (!s) return "—";
  const d = new Date(s.replace(" ", "T") + "Z");
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleString("ru-RU", { dateStyle: "short", timeStyle: "short" });
}

function parseRoute() {
  const h = location.hash.slice(1) || "/";
  const mr = h.match(/^\/reconciliation\/(\d+)$/);
  if (mr) return { view: "reconciliation", id: Number(mr[1]) };
  const m = h.match(/^\/company\/(.+)$/);
  if (m) return { view: "company", email: decodeURIComponent(m[1]) };
  return { view: "home" };
}

function formatEventDate(s) {
  if (!s) return "—";
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (m) return `${m[3]}.${m[2]}.${m[1]}`;
  return s;
}

function formatReceivedDate(s) {
  if (!s) return "—";
  const iso = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
  if (iso) return `${iso[3]}.${iso[2]}.${iso[1]}`;
  const d = new Date(s.replace(" ", "T") + "Z");
  if (!Number.isNaN(d.getTime())) {
    return d.toLocaleDateString("ru-RU", { dateStyle: "short" });
  }
  const parsed = new Date(s);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString("ru-RU", { dateStyle: "short" });
  }
  return s;
}

function navigate(hash) {
  location.hash = hash;
}

async function runAction(email, action, confirmText) {
  if (confirmText && !confirm(confirmText)) return;
  const data = await api("/api/companies/action", {
    method: "POST",
    body: JSON.stringify({ email, action }),
  });
  showToast(data.message || "Готово");
}

async function sendReconciliation(id) {
  const data = await api(`/api/reconciliations/${id}/send`, { method: "POST", body: "{}" });
  showToast(data.message || "Отправлено");
}

function bindTriggerFields(root, profile) {
  const typeSel = root.querySelector("#trigger_type");
  const blocks = {
    interval: root.querySelector("#f_interval"),
    schedule: root.querySelector("#f_schedule"),
    count: root.querySelector("#f_count"),
    keyword: root.querySelector("#f_keyword"),
  };
  function showFields() {
    Object.values(blocks).forEach((el) => el && el.classList.remove("active"));
    blocks[typeSel.value]?.classList.add("active");
  }
  typeSel.addEventListener("change", showFields);
  if (profile) {
    typeSel.value = profile.trigger_type;
    const c = profile.trigger_config || {};
    if (profile.trigger_type === "interval") {
      const sec = c.interval_seconds || profile.interval_seconds || 86400;
      root.querySelector("#interval_days").value = secondsToDays(sec);
    }
    if (profile.trigger_type === "schedule") {
      root.querySelector("#day_of_month").value = c.day_of_month ?? 5;
      root.querySelector("#hour").value = c.hour ?? 17;
      root.querySelector("#minute").value = c.minute ?? 0;
      root.querySelector("#timezone").value = c.timezone || "Europe/Moscow";
    }
    if (profile.trigger_type === "count") root.querySelector("#min_count").value = c.min_count ?? 10;
    if (profile.trigger_type === "keyword") root.querySelector("#phrase").value = c.phrase || "";
  }
  showFields();
}

function buildPayload(root, email) {
  const trigger_type = root.querySelector("#trigger_type").value;
  let trigger_config = {};
  if (trigger_type === "interval") {
    const days = Math.max(1, +root.querySelector("#interval_days").value || 1);
    trigger_config = { interval_seconds: days * 86400 };
  } else if (trigger_type === "schedule") {
    trigger_config = {
      day_of_month: +root.querySelector("#day_of_month").value,
      hour: +root.querySelector("#hour").value,
      minute: +root.querySelector("#minute").value,
      timezone: root.querySelector("#timezone").value.trim() || "Europe/Moscow",
    };
  } else if (trigger_type === "count") {
    trigger_config = { min_count: +root.querySelector("#min_count").value };
  } else {
    trigger_config = { phrase: root.querySelector("#phrase").value.trim() };
  }
  return {
    email,
    company_name: root.querySelector("#company_name").value.trim(),
    trigger_type,
    trigger_config,
  };
}

async function renderHome(q) {
  const companies = await api("/api/companies" + (q ? `?q=${encodeURIComponent(q)}` : ""));
  app.innerHTML = `
    <h1 class="page-title">Компании</h1>
    <p class="page-sub">Найдите компанию или добавьте новую в профиле</p>

    <div class="company-list" id="company-list"></div>
    <p class="hint" style="margin-top:1rem">
      <a href="#/company/new" class="btn btn-secondary">+ Новая компания</a>
    </p>`;

  const list = app.querySelector("#company-list");

  if (!companies.length) {
    list.innerHTML = `<div class="empty">Ничего не найдено. Создайте компанию по ссылке выше.</div>`;
    return;
  }
  list.innerHTML = companies
    .map((c) => {
      const title = c.company_name || c.email;
      const pill =
        c.status === "paused"
          ? '<span class="pill pill-paused">Пауза</span>'
          : '<span class="pill pill-active">Активна</span>';
      const openInfo = c.open_reconciliation_id
        ? ` · открыта сверка #${c.open_reconciliation_id}`
        : "";
      return `<a class="company-card" href="#/company/${encodeURIComponent(c.email)}">
        <h3>${esc(title)}</h3>
        <div class="meta">${esc(c.email)} · ${esc(typeLabels[c.trigger_type] || c.trigger_type)} · в очереди: ${c.pending_count || 0}${openInfo} ${pill}</div>
      </a>`;
    })
    .join("");
}



function triggerFormHtml() {
  return `
    <div class="grid-2">
      <div><label>Название компании</label><input id="company_name" required placeholder="ООО Пример"></div>
      <div><label>E-mail получателя</label><input id="email" type="email" required placeholder="user@example.com"></div>
    </div>
    <div class="grid-2" style="margin-top:0.75rem">
      <div><label>Тип триггера</label>
        <select id="trigger_type">
          <option value="interval">По времени</option>
          <option value="schedule">По расписанию</option>
          <option value="count">По порогу нежелательных явлений</option>
          <option value="keyword">По кодовому слову</option>
        </select>
      </div>
    </div>
    <div id="f_interval" class="fields"><label>Интервал (дней)</label><input id="interval_days" type="number" min="1" value="1"></div>
    <div id="f_schedule" class="fields"><div class="grid-3">
      <div><label>День месяца</label><input id="day_of_month" type="number" min="1" max="31" value="5"></div>
      <div><label>Час</label><input id="hour" type="number" min="0" max="23" value="17"></div>
      <div><label>Минута</label><input id="minute" type="number" min="0" max="59" value="0"></div>
    </div><label style="margin-top:0.5rem">Часовой пояс</label><input id="timezone" value="Europe/Moscow"></div>
    <div id="f_count" class="fields"><label>Минимум явлений</label><input id="min_count" type="number" min="1" value="10"></div>
    <div id="f_keyword" class="fields"><label>Кодовое слово</label><input id="phrase" placeholder="отправить сверку сейчас"></div>`;
}

async function renderCompany(email) {
  const isNew = email === "new";
  let profile = null;
  let reconciliations = [];
  if (!isNew) {
    profile = await api(`/api/companies/${encodeURIComponent(email)}`);
    reconciliations = await api(`/api/companies/${encodeURIComponent(email)}/reconciliations`);
  }

  const title = isNew ? "Новая компания" : profile.company_name || profile.email;
  app.innerHTML = `
    <nav class="breadcrumb"><a href="#/">← Компании</a></nav>
    <h1 class="page-title">${esc(title)}</h1>
    ${!isNew ? `<p class="page-sub">${esc(profile.email)}</p>` : ""}

    <div class="card">
      <h2>Настройки</h2>
      <form id="profile-form">${triggerFormHtml()}
        <div class="actions" style="margin-top:1rem">
          <button type="submit" class="btn btn-primary">Сохранить</button>
        </div>
      </form>
    </div>

    ${
      !isNew
        ? `<div class="card">
      <h2>Управление</h2>
      <div class="actions" id="mgmt-actions"></div>
    </div>

    <div class="card">
      <h2>История сверок</h2>
      <table>
        <thead><tr><th>ID</th><th>Итерация</th><th>Начало</th><th>Отправлена</th><th>Писем</th><th></th></tr></thead>
        <tbody id="recon-body"></tbody>
      </table>
    </div>`
        : ""
    }`;

  const form = app.querySelector("#profile-form");
  if (isNew) {
    bindTriggerFields(form, null);
  } else {
    form.querySelector("#email").value = profile.email;
    form.querySelector("#email").readOnly = true;
    form.querySelector("#company_name").value = profile.company_name || "";
    bindTriggerFields(form, profile);

    const mgmt = app.querySelector("#mgmt-actions");
    const paused = profile.status === "paused";
    if (paused) {
      mgmt.innerHTML += `<button type="button" class="btn btn-secondary" data-act="resume">Включить накопление</button>`;
    } else {
      mgmt.innerHTML += `<button type="button" class="btn btn-secondary" data-act="pause">Пауза</button>`;
    }
    if (profile.open_reconciliation_id) {
      mgmt.innerHTML += `<button type="button" class="btn btn-primary" data-act="send-open">Отправить текущую (#${profile.open_reconciliation_id})</button>`;
    }
    mgmt.innerHTML += `<button type="button" class="btn btn-danger" data-act="delete">Удалить</button>`;

    mgmt.querySelectorAll("[data-act]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        try {
          const act = btn.dataset.act;
          if (act === "pause") await runAction(profile.email, "pause", "Отключить накопление?");
          if (act === "resume") await runAction(profile.email, "resume");
          if (act === "delete")
            await runAction(
              profile.email,
              "delete",
              "Удалить компанию и очистить очередь?"
            );
          if (act === "send-open") await sendReconciliation(profile.open_reconciliation_id);
          render();
        } catch (e) {
          showToast(e.message, true);
        }
      });
    });

    const tbody = app.querySelector("#recon-body");
    if (!reconciliations.length) {
      tbody.innerHTML = `<tr><td colspan="6" class="empty">Сверок пока нет</td></tr>`;
    } else {
      tbody.innerHTML = reconciliations
        .map((r) => {
          const open = !r.sent_at;
          const pending = Number(r.pending_count || 0);
          const canSend = open || pending > 0;
          const sendBtn = canSend
            ? `<button type="button" class="btn btn-sm btn-primary" data-send="${r.id}">Отправить${
                !open && pending > 0 ? ` (+${pending})` : ""
              }</button>`
            : "";
          const iter = Math.max(1, Number(r.iteration || 1));
          const sentIters = Number(r.sent_iterations || 0);
          const iterHint = pending > 0 && sentIters > 0 ? " (готовится дополнение)" : "";
          const iterCell = `#${iter}`;
          return `<tr>
            <td><a href="#/reconciliation/${r.id}"><strong>#${r.id}</strong></a>${open ? ' <span class="recon-open">открыта</span>' : ""}</td>
            <td title="Отправлений: ${sentIters}${iterHint}">${iterCell}</td>
            <td>${formatDt(r.started_at)}</td>
            <td>${formatDt(r.sent_at)}</td>
            <td>${r.letter_count || 0}</td>
            <td>${sendBtn}</td>
          </tr>`;
        })
        .join("");

      tbody.querySelectorAll("[data-send]").forEach((b) => {
        b.addEventListener("click", async () => {
          try {
            await sendReconciliation(+b.dataset.send);
            render();
          } catch (e) {
            showToast(e.message, true);
          }
        });
      });
    }
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const payload = buildPayload(form, form.querySelector("#email").value.trim());
      await api("/api/companies", { method: "POST", body: JSON.stringify(payload) });
      showToast("Сохранено");
      if (isNew) navigate(`#/company/${encodeURIComponent(payload.email)}`);
      else render();
    } catch (err) {
      showToast(err.message, true);
    }
  });
}

async function renderReconciliation(id) {
  const data = await api(`/api/reconciliations/${id}/rows`);
  const rows = Array.isArray(data.rows) ? data.rows : [];
  const open = !data.sent_at;
  const pending = rows.filter((r) => !r.digest_sent_at).length;
  const canSend = open || pending > 0;
  const email = data.recipient_email || "";
  const backHref = email ? `#/company/${encodeURIComponent(email)}` : "#/";
  const backTitle = email ? `← ${esc(email)}` : "← Компании";

  app.innerHTML = `
    <nav class="breadcrumb"><a href="${backHref}">${backTitle}</a></nav>
    <h1 class="page-title">Сверка #${data.id}${open ? ' <span class="recon-open">открыта</span>' : ""}</h1>
    <p class="page-sub">
      Получатель: <strong>${esc(email)}</strong> ·
      начало: ${esc(formatDt(data.started_at))} ·
      отправлена: ${esc(formatDt(data.sent_at))}
    </p>

    <div class="card">
      <div class="actions" style="margin-bottom:.75rem">
        ${
          canSend
            ? `<button type="button" class="btn btn-primary" id="recon-send">
                 Отправить${!open && pending > 0 ? ` дополнение (+${pending})` : ""}
               </button>`
            : '<span class="hint">Сверка уже отправлена, новых строк нет.</span>'
        }
        <span class="hint">Всего строк: ${rows.length}${pending ? ` · ожидают отправки: ${pending}` : ""}</span>
      </div>

      ${
        rows.length
          ? `<table class="recon-table">
              <thead>
                <tr>
                  <th>ID явления</th>
                  <th>Сопоставленный</th>
                  <th>Дата получения</th>
                  <th>Дата явления</th>
                  <th>Статус</th>
                </tr>
              </thead>
              <tbody>
                ${rows
                  .map((r) => {
                    const isSent = !!r.digest_sent_at;
                    const status = isSent
                      ? `<span class="pill pill-active" title="${esc(formatDt(r.digest_sent_at))}">отправлено</span>`
                      : '<span class="pill pill-paused">ожидание</span>';
                    return `<tr>
                      <td>${esc(r.id_yavleniya)}</td>
                      <td>${esc(r.id_sopostavlennyi)}</td>
                      <td>${esc(formatReceivedDate(r.sent_at))}</td>
                      <td>${esc(formatEventDate(r.event_date))}</td>
                      <td>${status}</td>
                    </tr>`;
                  })
                  .join("")}
              </tbody>
            </table>`
          : '<div class="empty">В сверке пока нет строк.</div>'
      }
    </div>`;

  const sendBtn = app.querySelector("#recon-send");
  if (sendBtn) {
    sendBtn.addEventListener("click", async () => {
      sendBtn.disabled = true;
      try {
        await sendReconciliation(id);
        render();
      } catch (e) {
        showToast(e.message, true);
        sendBtn.disabled = false;
      }
    });
  }
}

async function render() {
  const route = parseRoute();
  const q = globalSearch.value.trim();
  try {
    if (route.view === "company") await renderCompany(route.email);
    else if (route.view === "reconciliation") await renderReconciliation(route.id);
    else await renderHome(q);
  } catch (e) {
    app.innerHTML = `<div class="card empty">${esc(e.message)}</div>`;
  }
}

globalSearch.addEventListener(
  "input",
  (() => {
    let t;
    return () => {
      clearTimeout(t);
      t = setTimeout(() => {
        if (parseRoute().view === "home") render();
        else navigate("#/");
      }, 280);
    };
  })()
);

document.body.dataset.theme = "sky";

function initIntroDrawer() {
  const drawer = document.getElementById("intro-drawer");
  const overlay = document.getElementById("intro-overlay");
  const toggle = document.getElementById("intro-toggle");
  const closeBtn = document.getElementById("intro-close");
  const form = document.getElementById("intro-form");
  const area = document.getElementById("intro_text");
  const defaultBtn = document.getElementById("intro-default");
  let loaded = false;
  let defaultText = "";

  async function open() {
    if (!loaded) {
      try {
        const data = await api("/api/settings/intro_text");
        area.value = data.text || "";
        defaultText = data.default || "";
        loaded = true;
      } catch (e) {
        showToast(e.message, true);
        return;
      }
    }
    drawer.classList.add("open");
    drawer.setAttribute("aria-hidden", "false");
    toggle.setAttribute("aria-expanded", "true");
    overlay.hidden = false;
  }
  function close() {
    drawer.classList.remove("open");
    drawer.setAttribute("aria-hidden", "true");
    toggle.setAttribute("aria-expanded", "false");
    overlay.hidden = true;
  }

  toggle.addEventListener("click", () => {
    if (drawer.classList.contains("open")) close();
    else open();
  });
  closeBtn.addEventListener("click", close);
  overlay.addEventListener("click", close);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && drawer.classList.contains("open")) close();
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await api("/api/settings/intro_text", {
        method: "POST",
        body: JSON.stringify({ text: area.value }),
      });
      showToast("Шаблон письма сохранён");
    } catch (err) {
      showToast(err.message, true);
    }
  });
  defaultBtn.addEventListener("click", () => {
    area.value = defaultText;
  });
}

initIntroDrawer();

window.addEventListener("hashchange", render);
render();


