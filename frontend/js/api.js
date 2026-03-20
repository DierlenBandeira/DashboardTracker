async function postJson(url, payload) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
  return j;
}

async function refreshUnits() {
  const sel = document.getElementById("unitSelect");
  if (!sel) return;
  sel.innerHTML = `<option>Carregando...</option>`;

  try {
    const r = await fetch(apiUrl("/units"), { cache: "no-store" });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || "HTTP " + r.status);

    unitsCache = j.units || [];
    if (!unitsCache.length) {
      sel.innerHTML = `<option>Nenhuma unidade</option>`;
      return;
    }

    const currentId = window.__currentItemId || null;

    sel.innerHTML = unitsCache
      .map((u) => {
        const selected = currentId && Number(u.id) === Number(currentId) ? "selected" : "";
        return `<option value="${u.id}" ${selected}>${u.name}</option>`;
      })
      .join("");
  } catch (e) {
    sel.innerHTML = `<option>Erro ao carregar: ${e}</option>`;
  }
}

window.updateSid = async function () {
  const sid = document.getElementById("sidInput")?.value?.trim();
  if (!sid) return alert("Informe o SID.");
  try {
    await postJson(apiUrl("/set_sid"), { sid });
    alert("SID atualizado.");
    await refreshUnits();
  } catch (e) {
    alert("Erro ao atualizar SID: " + e);
  }
};