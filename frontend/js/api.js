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
        const selected =
          currentId && Number(u.id) === Number(currentId) ? "selected" : "";
        return `<option value="${u.id}" ${selected}>${u.name}</option>`;
      })
      .join("");
  } catch (e) {
    sel.innerHTML = `<option>Erro ao carregar: ${e.message || e}</option>`;
  }
}

window.loginWialon = async function () {
  const user = document.getElementById("wialonUser")?.value?.trim();
  const password = document.getElementById("wialonPassword")?.value?.trim();

  if (!user || !password) {
    alert("Informe usuário e senha.");
    return;
  }

  try {
    const result = await postJson(apiUrl("/login_wialon"), {
      user,
      password,
    });

    alert(`Login realizado com sucesso${result.user ? `: ${result.user}` : ""}.`);
    await refreshUnits();
  } catch (e) {
    alert("Erro ao fazer login: " + (e.message || e));
  }
};

window.updateSid = async function () {
  const sid = document.getElementById("sidInput")?.value?.trim();

  if (!sid) {
    alert("Informe o SID.");
    return;
  }

  try {
    await postJson(apiUrl("/set_sid"), { sid });
    alert("SID atualizado.");
    await refreshUnits();
  } catch (e) {
    alert("Erro ao atualizar SID: " + (e.message || e));
  }
};