(() => {
  const el = (id) => document.getElementById(id);
  const scenarioSelect = el("scenario-select");
  const utterance = el("utterance");
  const runBtn = el("run-btn");
  let scenarios = [];

  // Per-card "?" buttons: one explain bubble at a time, dismissable via "×".
  document.addEventListener("click", (ev) => {
    const help = ev.target.closest(".help-btn");
    if (help) {
      const card = help.closest(".card");
      const wasOpen = card.classList.contains("show-explain");
      document.querySelectorAll(".card.show-explain")
        .forEach(c => c.classList.remove("show-explain"));
      if (!wasOpen) card.classList.add("show-explain");
      return;
    }
    const close = ev.target.closest(".explain-close");
    if (close) {
      close.closest(".card").classList.remove("show-explain");
    }
  });
  // Esc closes any open bubble.
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") {
      document.querySelectorAll(".card.show-explain")
        .forEach(c => c.classList.remove("show-explain"));
    }
  });

  const brandClass = (b) => ({
    "BT": "brand-bt", "EE": "brand-ee", "PLUSNET": "brand-pn"
  }[b] || "");

  const fmtMs = (n) => {
    if (n == null) return "0 ms";
    if (n < 1) return `${n.toFixed(2)} ms`;
    return `${n.toLocaleString(undefined, {maximumFractionDigits: 1})} ms`;
  };

  const fmtDateTime = (iso) => {
    try {
      const d = new Date(iso);
      return d.toLocaleString("en-GB", {
        day: "2-digit", month: "short",
        hour: "2-digit", minute: "2-digit",
      });
    } catch { return iso; }
  };

  const setTelemetry = (t) => {
    const rows = [
      ["Agent 1 — identity", t.agent1_identity_ms],
      ["Agent 2 — household", t.agent2_household_ms],
      ["Agent 3 — context ($rankFusion)", t.agent3_context_ms],
      ["Agent 4 — first token", t.agent4_first_token_ms],
      ["Agent 4 — briefing total", t.agent4_briefing_ms],
    ];
    const tbody = document.querySelector("#telemetry tbody");
    tbody.innerHTML = rows.map(([k, v]) =>
      v == null ? "" :
      `<tr><td>${k}</td><td class="ms">${fmtMs(v)}</td></tr>`
    ).join("") + (t.total_ms != null
      ? `<tr class="total"><td>Total</td><td class="ms">${fmtMs(t.total_ms)}</td></tr>`
      : "");
  };

  const renderTrace = (hits) => {
    const t = el("trace");
    if (!hits || !hits.length) {
      t.innerHTML = `<div class="muted">No hybrid hits (utterance too short, or no interactions in the 7-day window).</div>`;
      return;
    }
    const asArray = (v) => Array.isArray(v) ? v : (v && Array.isArray(v.details) ? v.details : []);
    t.innerHTML = hits.map((h, i) => {
      const details = asArray(h.scoreDetails).map(d => {
        const name = (d.inputPipelineName || "?").padEnd(9);
        const rank = String(d.rank ?? "?").padStart(2);
        const w = typeof d.weight === "number" ? d.weight.toFixed(2) : "?";
        return `  ${name} rank ${rank}  w=${w}  ${d.description || ""}`;
      }).join("\n");
      const score = typeof h.hybridScore === "number" ? h.hybridScore.toFixed(5) : "?";
      return `<div class="hit">
<strong>#${i+1}</strong> <span class="ident">${h._id}</span>  score=${score}
${(h.interactionSummary || "").slice(0, 160)}${(h.interactionSummary||"").length>160?"…":""}
${details}
</div>`;
    }).join("");
  };

  const renderCaller = (identity, household) => {
    const c = identity?.caller;
    if (!c) {
      el("caller-body").textContent = "No caller matched. Transfer to live agent for ID.";
      return;
    }
    const brand = c.brand || "";
    const phone = (c.contact || {}).phone || "";
    const method = identity.method;
    const conf = (identity.confidence * 100).toFixed(0);
    el("caller-body").innerHTML = `
      <div><span class="badge ${brandClass(brand)}">${brand}</span>
        <strong>${c.firstName} ${c.lastName}</strong> · ${phone}</div>
      <div class="muted">Resolution: ${method} (${conf}%) · Premise UPRN ${household.uprn || "–"}</div>`;
  };

  const renderDriver = (briefing) => {
    const conf = (briefing.confidence * 100).toFixed(0);
    el("confidence").textContent = `Confidence ${conf}%`;
    el("driver-body").textContent = briefing.callDriver;
    el("script").textContent = briefing.recommendedScript;
  };

  const renderTimeline = (dossier, callerName) => {
    if (!dossier) { el("timeline").innerHTML = "<li class='muted'>No dossier.</li>"; return; }
    const events = [];
    for (const i of dossier.recentInteractionsSummary || []) {
      const member = (dossier.householdMembers || [])
        .find(m => m.profileId === i.profileId);
      events.push({
        when: i.createdAt,
        who: member ? member.name.split(" ")[0] : "–",
        brand: member?.brand,
        channel: i.channel,
        text: i.summary,
      });
    }
    for (const op of dossier.activeOperations || []) {
      events.push({
        when: op.scheduledWindow?.start,
        who: "WFM",
        channel: op.provider,
        text: `${op.workOrderType} ${op.reference} · ${op.statusDetails || op.status}`,
      });
    }
    for (const o of dossier.openOrders || []) {
      events.push({
        when: o.createdAt,
        who: "Order",
        channel: o.brand,
        text: `${o.orderType} ${o._id} · ${o.statusDetails || o.status}`,
      });
    }
    events.sort((a, b) => new Date(a.when) - new Date(b.when));

    const now = new Date();
    events.push({
      when: now.toISOString(),
      who: callerName || "Call",
      channel: "VOICE",
      text: "INBOUND (current call)",
      current: true,
    });

    el("timeline").innerHTML = events.map(e => {
      const brandCls = brandClass(e.brand || "");
      return `<li class="${e.current ? 'current' : ''}">
        <span class="when">${fmtDateTime(e.when)}</span>
        <span class="${brandCls}">${e.who}</span> ·
        <span class="muted">${e.channel || ""}</span> —
        ${e.text}
      </li>`;
    }).join("");
  };

  const renderDossier = (dossier) => {
    if (!dossier) { el("dossier").innerHTML = "<div class='muted'>No dossier.</div>"; return; }
    const members = (dossier.householdMembers || []).map(m =>
      `<span class="badge ${brandClass(m.brand)}">${m.brand}</span>${m.name} (${m.role})`
    ).join(" · ");
    const converged = dossier.commercialInsights?.convergedHousehold;
    const discounts = (dossier.commercialInsights?.eligibleDiscounts || []).join(", ") || "–";
    const addr = dossier.address || {};
    const line = [addr.subBuildingName, addr.buildingNumber + " " + (addr.thoroughfareName||""),
                  addr.postTown, addr.postcode].filter(Boolean).join(", ");
    el("dossier").innerHTML = `
      <div class="kv">
        <div class="k">Address</div><div>${line}</div>
        <div class="k">Members</div><div>${members}</div>
        <div class="k">Line status</div><div>${dossier.networkInfrastructure?.lineStatus || "–"}</div>
        <div class="k">Converged household</div><div>${converged ? "Yes" : "No"}</div>
        <div class="k">Next best action</div><div>${dossier.commercialInsights?.nextBestAction || "–"}</div>
        <div class="k">Eligible discounts</div><div>${discounts}</div>
      </div>`;
  };

  const renderWriteback = (id, callDriver) => {
    el("writeback").innerHTML = `
      <div>New interaction <code>${id}</code> written back.
      Auto-embedded by Atlas on insert, so this call will participate in
      the next conversation's <code>$rankFusion</code> hybrid search.</div>
      <div class="muted" style="margin-top:4px">Summary cue: “${callDriver}”.</div>`;
  };

  function clearResults() {
    el("caller-body").className = "muted";
    el("caller-body").textContent = "No call yet.";
    el("confidence").textContent = "";
    el("driver-body").className = "muted";
    el("driver-body").textContent = "–";
    el("timeline").innerHTML = "";
    el("dossier").innerHTML = "";
    el("script").textContent = "";
    el("script").classList.remove("streaming");
    el("writeback").className = "muted";
    el("writeback").innerHTML = "The current call will be written back as a " +
      "new interaction and auto-embedded by Atlas on wrap-up.";
    el("trace").innerHTML = "";
    document.querySelector("#telemetry tbody").innerHTML = "";
  }

  async function loadScenarios() {
    const resp = await fetch("/api/scenarios");
    const data = await resp.json();
    scenarios = data.scenarios;
    scenarioSelect.innerHTML = scenarios.map(s =>
      `<option value="${s.id}">${s.label}</option>`
    ).join("");
    scenarioSelect.addEventListener("change", () => {
      const s = scenarios.find(x => x.id === scenarioSelect.value);
      if (s) utterance.value = s.utterance;
      clearResults();
    });
    utterance.value = scenarios[0].utterance;
    const pill = el("mode-pill");
    const bits = [];
    bits.push(data.mockMode ? "mock-mongo" : "atlas");
    bits.push(data.mockLlm ? "mock-llm" : (data.llmLabel || "llm"));
    pill.textContent = bits.join(" · ");
  }

  // --- Streaming SSE over fetch ----------------------------------------

  function extractJsonString(partial, key) {
    const re = new RegExp(`"${key}"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)`);
    const m = partial.match(re);
    if (!m) return null;
    try {
      // Parse as a JSON string by closing the quote; handles \n \" \\ etc.
      return JSON.parse('"' + m[1] + '"');
    } catch {
      return m[1]; // best-effort raw
    }
  }

  async function* readSse(resp) {
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, {stream: true});
      let idx;
      while ((idx = buffer.indexOf("\n\n")) !== -1) {
        const raw = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        let type = "message", data = "";
        for (const line of raw.split("\n")) {
          if (line.startsWith("event: ")) type = line.slice(7).trim();
          else if (line.startsWith("data: ")) data += line.slice(6);
        }
        if (data) {
          try { yield {type, payload: JSON.parse(data)}; }
          catch(e) { console.warn("bad SSE payload", data); }
        }
      }
    }
  }

  runBtn.addEventListener("click", async () => {
    runBtn.disabled = true;
    runBtn.textContent = "Dialling…";
    clearResults();
    el("driver-body").textContent = "Resolving caller, retrieving household context…";
    const scenarioId = scenarioSelect.value;
    const utter = utterance.value.trim();
    const scenario = scenarios.find(s => s.id === scenarioId);
    const custom = scenario.utterance !== utter;
    const url = custom
      ? "/api/calls/inbound/stream"
      : `/api/scenarios/${scenarioId}/run/stream`;
    const body = custom
      ? JSON.stringify({...scenario, utterance: utter})
      : null;

    let partialBriefing = "";
    let finalBriefing = null;
    let lastContext = null;

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body,
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

      let lastIdentity = null;
      for await (const {type, payload} of readSse(resp)) {
        if (type === "identity") {
          lastIdentity = payload.identity;
          renderCaller(payload.identity, {uprn: payload.identity?.caller?.uprn});
          setTelemetry(payload.telemetry);
        } else if (type === "household") {
          // Re-render caller with the resolved UPRN + related-party count.
          if (lastIdentity) renderCaller(lastIdentity, payload.household);
          setTelemetry(payload.telemetry);
        } else if (type === "context") {
          lastContext = payload.context;
          renderTimeline(payload.context?.dossier,
            payload.context?.dossier?.householdMembers?.[0]?.name?.split(" ")[0]);
          renderDossier(payload.context?.dossier);
          renderTrace(payload.context?.hybridHits);
          setTelemetry(payload.telemetry);
        } else if (type === "briefing_delta") {
          partialBriefing += payload.delta;
          el("script").classList.add("streaming");
          const driver = extractJsonString(partialBriefing, "callDriver");
          const script = extractJsonString(partialBriefing, "recommendedScript");
          if (driver) {
            el("driver-body").classList.remove("muted");
            el("driver-body").textContent = driver;
          }
          if (script) {
            el("script").textContent = script;
          }
        } else if (type === "briefing") {
          finalBriefing = payload.briefing;
          renderDriver(finalBriefing);
          el("script").classList.remove("streaming");
          setTelemetry(payload.telemetry);
          renderWriteback(payload.writebackInteractionId, finalBriefing.callDriver);
        }
      }
    } catch (err) {
      el("driver-body").textContent = "Error: " + err.message;
    } finally {
      runBtn.disabled = false;
      runBtn.textContent = "Trigger Inbound Call";
    }
  });

  loadScenarios();
})();
