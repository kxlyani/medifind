/*
 * Shared by the patient pages (Find medicine, Prescription, My reservations).
 *
 * MediFind has no logins, so each browser gets a random anonymous id, kept in
 * localStorage. Reservations and restock alerts are linked to that id, which is
 * how "My reservations" knows what is yours.
 *
 * Everything lives on one object, MF, so it never clashes with a page's own code.
 */
window.MF = (() => {
  const memory = {};
  const store = {
    get(k) { try { return localStorage.getItem(k); } catch { return memory[k] ?? null; } },
    set(k, v) { try { localStorage.setItem(k, v); } catch { memory[k] = v; } },
  };

  function userId() {
    let id = store.get("medifind-user");
    if (!id) {
      id = "u-" + Math.random().toString(36).slice(2, 10);
      store.set("medifind-user", id);
    }
    return id;
  }

  const customerName = () => store.get("medifind-name") || "Guest";
  const setCustomerName = (name) => store.set("medifind-name", name.trim().slice(0, 50) || "Guest");

  const esc = (s) => String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

  async function request(method, url, body) {
    const res = await fetch(url, { method, headers: { "Content-Type": "application/json" },
                                   body: body === undefined ? undefined : JSON.stringify(body) });
    let data = null;
    try { data = await res.json(); } catch { /* empty body */ }
    if (!res.ok) throw new Error((data && data.detail) || res.statusText);
    return data;
  }

  // Server times are UTC without a "Z"; make them unambiguous.
  const utc = (iso) => new Date(/Z|[+-]\d\d:\d\d$/.test(iso) ? iso : iso + "Z");

  function timeAgo(iso) {
    const secs = Math.max(0, Math.round((Date.now() - utc(iso).getTime()) / 1000));
    if (secs < 5) return "just now";
    if (secs < 60) return `${secs} s ago`;
    if (secs < 3600) return `${Math.floor(secs / 60)} min ago`;
    if (secs < 86400) return `${Math.floor(secs / 3600)} h ago`;
    return `${Math.floor(secs / 86400)} days ago`;
  }

  // "Notify me when it's back": ask the Notification Service to watch for a restock.
  function subscribe(medicine, area) {
    return request("POST", "/notify/subscriptions", {
      user_id: userId(), medicine,
      area: area ? area.name : null, lat: area ? area.lat : null, lng: area ? area.lng : null,
    });
  }

  // Restock alerts appear as a banner under the header on every patient page.
  function startAlerts() {
    const header = document.querySelector(".top");
    const box = document.createElement("div");
    box.className = "alerts";
    box.setAttribute("aria-live", "polite");
    header.after(box);

    async function check() {
      if (document.hidden) return;
      try {
        const { notifications } = await request("GET",
          `/notify/notifications?user_id=${encodeURIComponent(userId())}&unread_only=true`);
        box.innerHTML = notifications.map(n => `
          <div class="alert">
            <p><strong>${esc(n.medicine)} is back in stock</strong> at ${esc(n.pharmacy)}, ${esc(n.area)}:
               ${n.quantity} available${n.distance_km != null ? `, ${n.distance_km} km from you` : ""}.</p>
            <div class="alert-actions">
              <a class="btn" href="/?medicine=${encodeURIComponent(n.medicine)}" data-read>Find it</a>
              <button class="link" type="button" data-read>Dismiss</button>
            </div>
          </div>`).join("");
      } catch { /* alerts are optional; try again next time */ }
    }

    box.addEventListener("click", async (e) => {
      const el = e.target.closest("[data-read]");
      if (!el) return;
      const isLink = el.tagName === "A";
      if (isLink) e.preventDefault();
      try { await request("POST", `/notify/notifications/read?user_id=${encodeURIComponent(userId())}`); } catch {}
      box.innerHTML = "";
      if (isLink) location.href = el.href;
    });

    check();
    setInterval(check, 4000);
  }

  return { userId, customerName, setCustomerName, esc, request, utc, timeAgo, subscribe, startAlerts };
})();
