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
      console.log(data);
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

//upload syllabus
$(document).on('click', '.upload', function () {
  const $uploadBtn = $(this);
  $uploadBtn.addClass('loading');
  const uploadUrl = $(this).data('url');

  const fileInput = $('<input type="file" style="display: none;" />');
  $('body').append(fileInput);

  pendingFileDialogs++;

  fileInput.on('change', function () {

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

    // Debug logging before upload
    console.log('Attempting upload to:', uploadUrl);
    console.log('File:', file);
    console.log('File name:', file.name);
    console.log('File size (bytes):', file.size);

    fetch(uploadUrl, {
      method: 'POST',
      body: formData
    })
    .then(res => res.json())
    .then(data => {
      $uploadBtn.removeClass('loading');
      if (data && data.status === 'success') {
        activeRequests--;
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
