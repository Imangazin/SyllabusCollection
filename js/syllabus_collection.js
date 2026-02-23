let activeRequests = 0;
let pendingFileDialogs = 0;

//exempt syllabus
$(document).on('click', '.exempt', function () {
  const $exemptBtn = $(this);
  $exemptBtn.addClass('loading');
  const exemptUrl = $(this).data('url');
  activeRequests++;

    fetch(exemptUrl, {
      method: 'POST',
    })
    .then(res => res.json())
    .then(data => {
      $exemptBtn.removeClass('loading');
      if (data && data.status === 'success') {
        activeRequests--;
        if (activeRequests === 0 && pendingFileDialogs === 0) {
          location.reload();
        }
      }
    })
    .catch(err => {
      console.error('Exempt error:', err);
      $exemptBtn.removeClass('loading');
      activeRequests--;
    });
});


//download report
$(document).on('click', '.download-report', function (e) {
  e.preventDefault();
  const reportUrl = $(this).attr('href');

  fetch(reportUrl)
  .then(res => res.json())
  .then(data => {
    if (data && Array.isArray(data)) {
      // Convert JSON data to CSV
      const csvRows = [
        ['Course Code', 'Syllabus Status', 'Campus Stores Status'], // header
        ...data.map(row => [row.Code, row.Recorded, row.AdoptionStatus])
      ].map(e => e.join(",")).join("\n");

      // Create a blob and trigger download
      const blob = new Blob([csvRows], { type: 'text/csv;charset=utf-8;' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.setAttribute('href', url);
      const urlParams = new URLSearchParams(reportUrl.split('?')[1]);
      const department = urlParams.get('department');
      const year = urlParams.get('year');
      const term = urlParams.get('term');
      const filename = `Syllabus Report ${department} ${year} ${term}.csv`;
      a.setAttribute('download', filename);
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
    }
  })
  .catch(err => console.error('Download report failed:', err));
});


$(document).on('click', '.upload', function () {
  const $uploadBtn = $(this);
  $uploadBtn.addClass('loading');
  const uploadUrl = $uploadBtn.data('url');

  const fileInput = $('<input type="file" style="display: none;" />');
  $('body').append(fileInput);

  pendingFileDialogs++;

  // Set fallback timeout
  const timeoutId = setTimeout(() => {
    pendingFileDialogs = Math.max(0, pendingFileDialogs - 1);

    if (activeRequests === 0 && $uploadBtn.hasClass('loading')) {
      $uploadBtn.removeClass('loading');
    }

    if (activeRequests === 0 && pendingFileDialogs === 0) {
      location.reload();
    }
  }, 60000);
  $uploadBtn.data('timeout-id', timeoutId);

  fileInput.on('change', function () {
    clearTimeout($uploadBtn.data('timeout-id'));

    const file = this.files[0];
    if (!file) {
      pendingFileDialogs--;
      $uploadBtn.removeClass('loading');
      fileInput.remove();
      return;
    }

    pendingFileDialogs--;
    activeRequests++;

    const formData = new FormData();
    formData.append('file', file);

    fetch(uploadUrl, {
      method: 'POST',
      body: formData
    })
    .then(res => res.json())
    .then(data => {
      $uploadBtn.removeClass('loading');
      activeRequests--;
      if (data && data.status === 'success') {
        if (activeRequests === 0 && pendingFileDialogs === 0) {
          location.reload();
        }
      }
    })
    .catch(err => {
      console.error('Upload failed:', err);
      $uploadBtn.removeClass('loading');
      activeRequests--;
    });

    fileInput.remove();
  });

  fileInput.click();
});

// ------------------------------
// Adoption Status Mapping (Frontend)
// ------------------------------

// Map raw Campus Store/Booklist values to friendlier display values.
// Update this object to change wording without touching backend.
const ADOPTION_STATUS_MAP = {
  "none": "Missing",
  "": "Missing",
  "null": "Missing",
  "nan": "Missing",
  "missing": "Missing",
  "no titles/complete": "No Titles/Complete",
  "complete": "Complete",
  "oer": "OER",
  "no store supplied materials/see instructor": "No Store Supplied Materials/See Instructor"
};

function normalizeAdoptionStatusKey(value) {
  if (value == null) return "none";
  return String(value).trim().toLowerCase();
}

function applyAdoptionStatusMapping() {
  // Table columns are: 0 = Course Code, 1 = Adoption Status, 2 = Action
  const STATUS_COLUMN_INDEX = 1;

  $('table tbody tr').each(function () {
    const $cells = $(this).find('td');
    if ($cells.length <= STATUS_COLUMN_INDEX) return;

    const $statusCell = $cells.eq(STATUS_COLUMN_INDEX);
    const rawText = $statusCell.text();
    const key = normalizeAdoptionStatusKey(rawText);

    if (Object.prototype.hasOwnProperty.call(ADOPTION_STATUS_MAP, key)) {
      $statusCell.text(ADOPTION_STATUS_MAP[key]);
    } else if (key === "" || key === "none" || key === "null" || key === "nan") {
      $statusCell.text("Missing");
    }
  });
}

// ------------------------------
// Load Definitions Section
// ------------------------------

function loadDefinitionsSection() {
  const downloadLink = document.querySelector('.download-report');
  if (!downloadLink) return;

  fetch('https://brocktest.brightspace.com/shared/Widgets/SyllabusUpload/definitions.html', { cache: 'no-store' })
    .then(res => res.text())
    .then(html => {
      const wrapper = document.createElement('div');
      wrapper.innerHTML = html;

      downloadLink.parentNode.insertBefore(wrapper, downloadLink);
    })
    .catch(err => console.error('Failed to load definitions:', err));
}

$(document).ready(function () {
  loadDefinitionsSection();
  applyAdoptionStatusMapping();

  // Re-apply after DataTables redraw (paging/search)
  if ($.fn.dataTable) {
    $('table').on('draw.dt', function () {
      applyAdoptionStatusMapping();
    });
  }
});
