$(document).on('click', '.upload', function () {
  const uploadUrl = $(this).data('url');

  // Create a hidden file input dynamically
  const fileInput = $('<input type="file" style="display: none;" />');
  $('body').append(fileInput);

  fileInput.on('change', function () {
    const file = this.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

  
    fetch(uploadUrl, {
      method: 'POST',
      body: formData
    })
    .then(res => res.json())
    .then(data => {
      console.log(data);
      if (data && data.status === 'success') {
        location.reload();
      }
    })
    .catch(err => console.error('Upload failed:', err));

    // Remove the file input from the DOM after use
    fileInput.remove();
  });
  // Trigger the file input dialog
  fileInput.click();
});


$(document).on('click', '.exempt', function () {
    const uploadUrl = $(this).data('url');

    fetch(uploadUrl, {
      method: 'POST',
    })
    .then(res => res.json())
    .then(data => {
      console.log(data);
      if (data && data.status === 'success') {
        location.reload();
      }
    })
    .catch(err => console.error('Exempt error:', err));
});

$(document).on('click', '.download-report', function (e) {
  e.preventDefault();
  const reportUrl = $(this).attr('href');

  fetch(reportUrl)
  .then(res => res.json())
  .then(data => {
    if (data && Array.isArray(data)) {
      // Convert JSON data to CSV
      const csvRows = [
        ['Course Code', 'Syllabus Status'], // header
        ...data.map(row => [row.Code, row.Recorded])
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
