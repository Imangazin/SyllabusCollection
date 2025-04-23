$(document).on('click', '.upload', function () {
    const uploadUrl = $(this).data('url');
  
    // Optionally, collect a file here or use a predefined one
    // const formData = new FormData();
    // formData.append('file', new Blob(['Hello from browser']), 'syllabus.txt'); // replace with real file
  
    fetch(uploadUrl, {
      method: 'POST',
      //body: formData
    })
    .then(res => res.json())
    .then(data => console.log(data))
    .catch(err => console.error('Upload failed:', err));
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

