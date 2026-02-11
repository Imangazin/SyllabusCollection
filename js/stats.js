function pctString(v) {
  if (v == null) return "0%";
  const n = Number(v);
  if (!isFinite(n)) return "0%";
  return `${n.toFixed(1).replace(".0","")}%`;
}

function donutPct(pct) {
  const n = Number(pct);
  const v = isFinite(n) ? Math.max(0, Math.min(100, n)) : 0;

  const r = 8;                   // radius
  const c = 2 * Math.PI * r;     // circumference
  const dash = (v / 100) * c;

  return `
    <span style="display:inline-flex;align-items:center;gap:8px;">
      <span>${v.toFixed(1).replace(".0","")}%</span>
      <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true">
        <circle cx="11" cy="11" r="${r}" fill="none" stroke="#e6e6e6" stroke-width="3"/>
        <circle cx="11" cy="11" r="${r}" fill="none" stroke="#c8a200" stroke-width="3"
          stroke-linecap="round"
          stroke-dasharray="${dash} ${c}"
          transform="rotate(-90 11 11)"/>
      </svg>
    </span>
  `;
}

function makeSummaryTable(selector, rows) {
  new DataTable(selector, {
    data: rows,
    columns: [
      { title: "Academic Year", data: "label" },
      // { title: "Raw Collected", data: "raw_collected" },
      // { title: "Raw Total Courses", data: "raw_total" },
      // { title: "Raw % Complete", data: "raw_pct", render: (d) => pctString(d) },
      { title: "Qualified Collected", data: "qualified_collected" },
      { title: "Qualified Total Courses", data: "qualified_total" },
      { title: "Qualified % Complete", data: "qualified_pct", render: (d) => donutPct(d) },
    ],
    paging: false,
    searching: false,
    info: false,
    order: [[0, "asc"]],
  });
}

function makeDeptTable(selector, years, rows) {
  const cols = [
    { title: "Department", data: "department" },
    ...years.map(y => ({
      title: String(y),
      data: String(y),
      render: (d) => donutPct(d)
    }))
  ];

  new DataTable(selector, {
    data: rows,
    columns: cols,
    paging: true,
    pageLength: 50,
    searching: true,
    info: false,
    order: [[0, "asc"]],
  });
}

async function loadJson(url) {
  const res = await fetch(url, { method: "GET" });
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return await res.json();
}

async function downloadAcademicYearReport(API_BASE, API_ROUTE, facultyId, token) {
  try {
    const sel = document.getElementById("year-select");
    const selectedYear = sel ? String(sel.value || "").trim() : "";

    if (!selectedYear) {
      const msgId = "year-select-message";
      let msg = document.getElementById(msgId);

      if (!msg) {
        msg = document.createElement("div");
        msg.id = msgId;
        msg.style.color = "#b00020";
        msg.style.marginTop = "6px";
        const selEl = document.getElementById("year-select");
        if (selEl && selEl.parentNode) {
          selEl.parentNode.appendChild(msg);
        }
      }

      msg.textContent = "Please select an academic year.";
      return;
    }

    const url = `${API_BASE}${API_ROUTE}/report/academic-year` +
      `?facultyId=${encodeURIComponent(facultyId)}` +
      `&token=${encodeURIComponent(token)}` +
      `&year=${encodeURIComponent(selectedYear)}`;

    const res = await fetch(url, { method: "GET" });
    if (!res.ok) {
      const msg = await res.text().catch(() => "");
      throw new Error(`Download failed (${res.status}). ${msg}`);
    }

    const blob = await res.blob();

    let filename = `syllabus_report_${selectedYear}.xlsx`;
    const cd = res.headers.get("content-disposition");
    if (cd) {
      const m = cd.match(/filename\*?=(?:UTF-8''|"?)([^";]+)"?/i);
      if (m && m[1]) {
        try {
          filename = decodeURIComponent(m[1]);
        } catch (_) {
          filename = m[1];
        }
      }
    }

    const a = document.createElement("a");
    const objectUrl = URL.createObjectURL(blob);
    a.href = objectUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(objectUrl);

  } catch (err) {
    console.error(err);
    console.log("Failed to download report.");
  }
}

window.addEventListener("load", async () => {
  try {
    const facultyId = window.FACULTY_ID;
    const API_BASE = window.API_BASE || "";
    const API_ROUTE = window.API_ROUTE || "/test-api";
    const token =window.token;

    const statsUrl = `${API_BASE}${API_ROUTE}/stats?facultyId=${encodeURIComponent(facultyId)}&token=${token}`;
    const deptUrl  = `${API_BASE}${API_ROUTE}/stats/by-department?facultyId=${encodeURIComponent(facultyId)}&token=${token}`;

    const stats = await loadJson(statsUrl);
    // Populate Academic Year combobox from stats (last three years)
    const yearSelect = document.getElementById("year-select");
    if (yearSelect) {
      const years = (stats.full_year || [])
        .map(r => String(r.label || "").match(/\d{4}/))
        .filter(m => m && m[0])
        .map(m => Number(m[0]))
        .filter(n => Number.isFinite(n));

      // Unique + sort descending
      const uniqueYears = Array.from(new Set(years)).sort((a, b) => b - a);

      // Clear existing options and add placeholder
      yearSelect.innerHTML = "";
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "-- Select Year --";
      yearSelect.appendChild(placeholder);

      for (const y of uniqueYears) {
        const opt = document.createElement("option");
        opt.value = String(y);
        opt.textContent = String(y);
        yearSelect.appendChild(opt);
      }

      // Default to the latest year
      if (uniqueYears.length > 0) {
        yearSelect.value = String(uniqueYears[0]);
      }
    }
    const downloadBtn = document.getElementById("btn-download-year");
    if (downloadBtn) {
      downloadBtn.addEventListener("click", () =>
        downloadAcademicYearReport(API_BASE, API_ROUTE, facultyId, token)
      );
    }
    makeSummaryTable("#tbl-full", stats.full_year || []);
    makeSummaryTable("#tbl-fw", stats.fw || []);
    makeSummaryTable("#tbl-sp", stats.sp || []);
    makeSummaryTable("#tbl-su", stats.su || []);

    const dept = await loadJson(deptUrl);
    makeDeptTable("#tbl-dept", dept.years || [], dept.rows || []);
  } catch (e) {
    console.error(e);
  }
});