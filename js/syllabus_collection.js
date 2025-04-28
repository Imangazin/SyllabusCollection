async function getToken() {
  const res = await fetch('/d2l/lp/auth/xsrf-tokens', {
    method: 'GET'
  });
  const data = await res.json();
  return data.referrerToken;
}


function parseCourseCode(courseCode) {
  const parts = courseCode.split('-');
  if (parts.length < 5) {
    throw new Error('Invalid course code format');
  }

  const year = parts[0];
  const term = parts[1];
  const department = parts[4];

  return { year, term, department };
}


$(document).on('click', '.exempt', function () {
    const exemptUrl = $(this).data('url');

    fetch(exemptUrl, {
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

$(document).on('click', '.upload', function () {
  const uploadUrl = $(this).data('url');

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
      console.log('Upload result:', data);
    })
    .catch(err => console.error('Upload failed:', err));

    fileInput.remove();
  });

  fileInput.click();
});


// $(document).on('click', '.upload', async function () {
//   const uploadUrl = $(this).data('url');
//   const urlParams = new URLSearchParams(uploadUrl.split('?')[1]);
//   const courseCode = urlParams.get('course');
//   const orgUnitId = urlParams.get('projectId');
//   const {year, term, department} = parseCourseCode(courseCode);
//   let uploadPath, fileKey;

//   const fileInput = $('<input type="file" style="display: none;" />');
//   $('body').append(fileInput);

//   fileInput.on('change', async function () {
//     const file = this.files[0];
//     if (!file) return;

//     try {
//       const xsrfToken = await getToken();
//       const newFileName = `syllabus_${courseCode}${file.name.substring(file.name.lastIndexOf('.'))}`;

//       // Step 1: Initiate Resumable Upload. Send it to server since browser can not handle 308
//       const response = await fetch(uploadUrl, {
//         method: 'POST',
//         headers: {
//           'Content-Type': 'application/json'
//         },
//         body: JSON.stringify({
//           fileType: file.type,
//           fileSize: file.size,
//           fileName: newFileName
//         })
//       });
      
//       const data = await response.json();
//       console.log(data);
//       if (data && data.status === 'success') {
//         fileKey = data.fileKey;
//         uploadPath = data.uploadUrl;
//       } else {
//         throw new Error('Failed to initiate upload.');
//       }
//       // Step 2: Upload File Chunk
//       await uploadFileChunk(uploadPath, file, xsrfToken);

//       // Step 3: Save File
//       const savedFile = await saveUploadedFile(orgUnitId, fileKey, newFileName, department, year, term, xsrfToken);



//     } catch (err) {
//       console.error('Upload failed:', err);
//     }

//     fileInput.remove();
//   });

//   fileInput.click();
// });


// async function uploadFileChunk(uploadPath, file, xsrfToken) {
//   console.log(`upload path: ${uploadPath}`);
//   const response = await fetch(uploadPath, {
//     method: 'POST',
//     headers: {
//       'X-Csrf-Token': xsrfToken,
//       'Content-Range': `bytes 0-${file.size - 1}/${file.size}`
//     },
//     credentials: 'include',
//     body: file
//   });

//   if (!response.ok) {
//     throw new Error(`Chunk upload failed: ${response.status}`);
//   }
// }

// async function saveUploadedFile(orgUnitId, fileKey, newFileName, department, year, term, xsrfToken) {
//   const apiVersion = '1.47';
//   const relativePath = `${department}/${year}/${term}`;
//   const saveUrl = `/d2l/api/lp/${apiVersion}/${orgUnitId}/managefiles/file/save?overwriteFile=true`;

//   const response = await fetch(saveUrl, {
//     method: 'POST',
//     headers: {
//       'X-Csrf-Token': xsrfToken
//     },
//     credentials: 'include',
//     body: new URLSearchParams({
//       relativePath: relativePath,
//       fileKey: fileKey
//     })
//   });
//   if (!response.ok) {
//     throw new Error(`Save failed: ${response.status}`);
//   }

//   return await response.json();
// }

