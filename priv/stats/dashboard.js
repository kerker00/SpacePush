"use strict";

// The widget/other request counters are subsets, not additional requests.
function requests(day) {
  return Object.entries(day).reduce((sum, [key, value]) =>
    sum + (key.startsWith("requests.") && !["requests.widget", "requests.other"].includes(key) ? value : 0), 0);
}

function totals(days) {
  const result = {};
  for (const day of days) for (const [key, value] of Object.entries(day)) {
    if (key !== "date" && typeof value === "number") result[key] = (result[key] || 0) + value;
  }
  return result;
}

function polling(days) {
  const sum = totals(days);
  return {
    rounds: sum["poll.rounds"] || 0,
    average: sum["poll.rounds"] ? Math.round((sum["poll.ms_sum"] || 0) / sum["poll.rounds"]) : null,
    maximum: Math.max(0, ...days.map(day => day["poll.ms_max"] || 0)),
    fetched: (sum["spaces.fetch_ok"] || 0) + (sum["mainframe.fetch_ok"] || 0),
    failed: (sum["spaces.fetch_failed"] || 0) + (sum["mainframe.fetch_failed"] || 0),
    changes: sum["changes.total"] || 0,
    dropped: sum["stats.dropped"] || 0
  };
}

function startDashboard() {
  const $ = id => document.getElementById(id);
  const fmt = new Intl.NumberFormat("de-DE");
  const number = value => fmt.format(value ?? 0);
  const labels = {
    delivered: "Von APNs angenommen", retry: "Erneuter Versuch", invalid_token: "Ungültiges Gerät",
    renew_token: "Provider-Token erneuern", invalid_provider_token: "Provider-Token abgelehnt", drop: "Von APNs abgelehnt",
    dry_run: "Nur protokolliert (Dry Run)", discarded: "Verworfen", invalid_delivery: "Ungültige Zustellung",
    send_failed: "Senden fehlgeschlagen", connection_failed: "Verbindung fehlgeschlagen",
    production: "Production", sandbox: "Sandbox", ios: "iOS", macos: "macOS", unknown: "Unbekannt", other: "Andere",
    accepted_by_apns: "Von APNs bestätigt", unconfirmed: "Noch unbestätigt",
    directory: "SpaceAPI-Verzeichnis", spaces: "Spaces", mainframe: "Mainframe"
  };
  let report = null;
  let inFlight = false;
  let authFailed = false;

  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (className) node.className = className;
    return node;
  }

  function empty(target, text = "Noch keine Daten für diesen Zeitraum.") {
    target.replaceChildren(element("p", text, "empty"));
  }

  function distribution(id, counts) {
    const target = $(id);
    const entries = Object.entries(counts || {}).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const total = entries.reduce((sum, [, count]) => sum + count, 0);
    if (!total) return empty(target);
    const nodes = entries.map(([label, count]) => {
      const row = element("div", undefined, "distribution-row");
      const heading = element("div", undefined, "distribution-label");
      const value = element("b", number(count));
      value.append(element("small", `${Math.round(count / total * 100)} %`));
      heading.append(element("span", labels[label] || label), value);
      const meter = element("meter");
      meter.max = total;
      meter.value = count;
      meter.setAttribute("aria-label", `${labels[label] || label}: ${number(count)} von ${number(total)}`);
      row.append(heading, meter);
      return row;
    });
    target.replaceChildren(...nodes);
  }

  function chart(id, entries, title) {
    const target = $(id);
    if (!entries.length) return empty(target);
    const svgNS = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(svgNS, "svg");
    svg.setAttribute("viewBox", "0 0 520 205");
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", `${title}. ${entries.map(([label, value]) => `${label}: ${number(value)}`).join("; ")}`);
    const add = (tag, attrs, text, parent = svg) => {
      const node = document.createElementNS(svgNS, tag);
      for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
      if (text !== undefined) node.textContent = text;
      parent.append(node);
      return node;
    };
    const maximum = Math.max(1, ...entries.map(([, count]) => count));
    const step = 464 / entries.length;
    for (const fraction of [0, .5, 1]) {
      const y = 172 - fraction * 144;
      add("line", {x1: 44, x2: 508, y1: y, y2: y, class: "chart-gridline"});
      add("text", {x: 35, y: y + 3, "text-anchor": "end", class: "chart-label"}, number(Math.round(maximum * fraction)));
    }
    entries.forEach(([label, count], index) => {
      const height = count / maximum * 144;
      const rect = add("rect", {x: 44 + index * step + step * .17, y: 172 - height, width: step * .66, height, rx: Math.min(3, step * .15), class: "chart-bar"});
      add("title", {}, `${label}: ${number(count)}`, rect);
      const every = Math.max(1, Math.ceil(entries.length / 6));
      if (index === entries.length - 1 || index % every === 0 && entries.length - 1 - index >= every) {
        let short = label;
        if (/^\d{4}-\d{2}-\d{2}$/.test(label)) short = `${label.slice(8, 10)}.${label.slice(5, 7)}.`;
        else if (label.includes("-W")) short = `KW ${label.slice(6)}`;
        else if (/^\d{4}-\d{2}$/.test(label)) short = `${label.slice(5)}.${label.slice(2, 4)}`;
        add("text", {x: 44 + index * step + step / 2, y: 197, "text-anchor": "middle", class: "chart-label"}, short);
      }
    });
    target.replaceChildren(svg);
  }

  function keyValues(id, pairs) {
    $(id).replaceChildren(...pairs.map(([label, value]) => {
      const row = element("div");
      row.append(element("dt", label), element("dd", value));
      return row;
    }));
  }

  function renderSubscriptions() {
    const filter = $("search").value.trim().toLocaleLowerCase("de-DE");
    const rows = (report.devices?.subscriptions || []).filter(row =>
      [row.name, row.endpoint, row.room].some(value => String(value || "").toLocaleLowerCase("de-DE").includes(filter)));
    $("subscriptions").replaceChildren(...rows.map(row => {
      const tr = element("tr");
      const name = element("td", row.name || row.endpoint);
      if (row.name) name.append(element("small", row.endpoint));
      tr.append(name, element("td", row.room === "space" ? "Gesamter Space" : row.room), element("td", number(row.devices), "numeric"));
      return tr;
    }));
    $("subscriptions-empty").hidden = rows.length > 0;
  }

  function renderHistory() {
    const entries = (report.installs?.[$("period").value] || []).slice().reverse();
    chart("installs-chart", entries.map(row => [row.period, row.installs]), "Aktive Apps pro Zeitraum");
  }

  function renderCounters(days) {
    const keys = [...new Set(days.flatMap(day => Object.keys(day)))].filter(key => key !== "date").sort();
    const table = element("table");
    const head = element("thead");
    const heading = element("tr");
    heading.append(element("th", "Datum"), ...keys.map(key => element("th", key)));
    head.append(heading);
    const body = element("tbody");
    for (const day of days) {
      const row = element("tr");
      row.append(element("td", day.date), ...keys.map(key => element("td", number(day[key]))));
      body.append(row);
    }
    table.append(head, body);
    $("counters").replaceChildren(table);
  }

  function render() {
    const {installs = {}, devices = {}, service = {}, days = []} = report;
    for (const [id, value] of Object.entries({today: installs.today, week: installs.this_week, month: installs.this_month,
      devices: devices.total, memory: service.memory_mb, processes: service.processes, pending: report.outbox?.pending})) $(id).textContent = number(value);
    $("device-cap").textContent = `von ${number(devices.cap)} verfügbaren Plätzen`;
    $("confirmation-cap").textContent = `Limit unbestätigter Geräte: ${number(devices.unconfirmed_cap)}`;
    const seconds = service.uptime_s || 0;
    $("uptime").textContent = seconds >= 86400 ? `${Math.floor(seconds / 86400)} T ${Math.floor(seconds % 86400 / 3600)} h` : `${Math.floor(seconds / 3600)} h ${Math.floor(seconds % 3600 / 60)} m`;
    $("version").textContent = `SpacePush ${service.version || ""}`;
    $("updated").textContent = `Stand: ${new Date(report.generated_at).toLocaleString("de-DE")}`;
    distribution("versions", installs.this_week_by?.versions);
    distribution("platforms", installs.this_week_by?.platforms);
    distribution("os-versions", installs.this_week_by?.os_versions);
    distribution("environments", devices.by_environment);
    distribution("device-platforms", devices.by_platform);
    distribution("confirmation", devices.by_confirmation);
    const sum = totals(days);
    distribution("push-results", Object.fromEntries(Object.entries(sum).filter(([key]) => key.startsWith("push.")).map(([key, value]) => [key.slice(5), value])));
    const chronological = days.slice().reverse();
    chart("requests-chart", chronological.map(day => [day.date, requests(day)]), "API-Anfragen pro Tag");
    $("request-total").textContent = number(days.reduce((total, day) => total + requests(day), 0));
    $("subscription-count").textContent = number(devices.subscriptions?.length);
    renderHistory();
    renderSubscriptions();
    const sources = [...new Set(["directory", "spaces", "mainframe", ...Object.keys(report.last_success || {})])];
    keyValues("last-success", sources.map(source => [labels[source] || source,
      report.last_success?.[source] ? new Date(report.last_success[source]).toLocaleString("de-DE") : "Noch kein erfolgreicher Abruf"]));
    const poll = polling(days);
    keyValues("polling", [["Polling-Runden", number(poll.rounds)], ["Dauer Ø / Maximum", `${poll.average === null ? "—" : number(poll.average)} / ${number(poll.maximum)} ms`],
      ["Erfolgreiche / fehlgeschlagene Abrufe", `${number(poll.fetched)} / ${number(poll.failed)}`], ["Bestätigte Zustandsänderungen", number(poll.changes)], ["Verworfene Statistikereignisse", number(poll.dropped)]]);
    if ($("counters").closest("details").open) renderCounters(days);
  }

  async function refresh() {
    if (inFlight || authFailed) return;
    inFlight = true;
    $("refresh").disabled = true;
    $("days").disabled = true;
    const timeout = new AbortController();
    const timer = setTimeout(() => timeout.abort(), 12000);
    try {
      const response = await fetch(`/stats/data.json?days=${$("days").value}`, {credentials: "same-origin", cache: "no-store", signal: timeout.signal});
      if (response.status === 401) {
        authFailed = true;
        throw new Error("Anmeldung erforderlich. Bitte die Seite neu laden und erneut anmelden.");
      }
      if (!response.ok) throw new Error(`Statistik nicht verfügbar (HTTP ${response.status}).`);
      report = await response.json();
      render();
      $("error").hidden = true;
      $("connection").textContent = "Verbunden";
      $("connection").className = "connection online";
    } catch (error) {
      $("error").textContent = (error.name === "AbortError" ? "Zeitüberschreitung beim Laden." : error.message) + (report ? " Angezeigt wird der letzte erfolgreiche Stand." : "");
      $("error").hidden = false;
      $("connection").textContent = authFailed ? "Anmeldung erforderlich" : "Keine aktuellen Daten";
      $("connection").className = "connection offline";
    } finally {
      clearTimeout(timer);
      inFlight = false;
      $("refresh").disabled = authFailed;
      $("days").disabled = authFailed;
    }
  }

  $("refresh").addEventListener("click", refresh);
  $("days").addEventListener("change", refresh);
  $("period").addEventListener("change", () => { if (report) renderHistory(); });
  $("search").addEventListener("input", () => { if (report) renderSubscriptions(); });
  $("counters").closest("details").addEventListener("toggle", event => { if (event.target.open && report) renderCounters(report.days || []); });
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
  setInterval(() => { if (!document.hidden) refresh(); }, 30000);
  refresh();
}

if (typeof document !== "undefined") startDashboard();
if (typeof module !== "undefined") module.exports = {requests, polling};
