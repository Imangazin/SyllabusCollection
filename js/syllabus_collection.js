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

