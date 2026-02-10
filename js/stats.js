function pctString(v) {
  if (v == null) return "0%";
  const n = Number(v);
  if (!isFinite(n)) return "0%";
  return `${n.toFixed(1).replace(".0","")}%`;
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
      { title: "Qualified % Complete", data: "qualified_pct", render: (d) => pctString(d) },
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
      render: (d) => pctString(d)
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

window.addEventListener("load", async () => {
  try {
    const facultyId = window.FACULTY_ID;
    const API_BASE = window.API_BASE || "";
    const API_ROUTE = window.API_ROUTE || "/test-api";
    const token =window.token;

    const statsUrl = `${API_BASE}${API_ROUTE}/stats?facultyId=${encodeURIComponent(facultyId)}&token=${token}`;
    const deptUrl  = `${API_BASE}${API_ROUTE}/stats/by-department?facultyId=${encodeURIComponent(facultyId)}&token=${token}`;

    const stats = await loadJson(statsUrl);
    makeSummaryTable("#tbl-full", stats.full_year || []);
    makeSummaryTable("#tbl-fw", stats.fw || []);
    makeSummaryTable("#tbl-sp", stats.sp || []);
    makeSummaryTable("#tbl-su", stats.su || []);

    const dept = await loadJson(deptUrl);
    makeDeptTable("#tbl-dept", dept.years || [], dept.rows || []);
  } catch (e) {
    console.error(e);
    alert("Failed to load stats.");
  }
});