const axios = require('axios');
const FormData = require('form-data');
const { seaweed } = require('./config');


async function uploadToSeaweedFS(fileBuffer) {
  try {
    // 1. Запрашиваем у Master Server новый файл
    const assignUrl = `http://${seaweed.masterHost}:${seaweed.masterPort}/dir/assign`;
    const assignResponse = await axios.get(assignUrl);
    const { fid, url } = assignResponse.data; 

    if (!fid || !url) {
      throw new Error('Не удалось получить fid или url из /dir/assign');
    }

    // 2. Формируем FormData для отправки файла
    const formData = new FormData();
    formData.append('file', fileBuffer, {
      filename: 'uploaded.bin',
      contentType: 'application/octet-stream'
    });

    const uploadUrl = `http://${url}/${fid}`;
    const headers = formData.getHeaders();

    const uploadResponse = await axios.post(uploadUrl, formData, {
      headers,
      maxBodyLength: Infinity, 
      maxContentLength: Infinity
    });

    // Проверяем, что ответ успешный
    if (uploadResponse.status !== 201) {
      throw new Error(`Не удалось загрузить файл. Код: ${uploadResponse.status}`);
    }

    console.log('Файл загружен в Volume Server:', uploadResponse.data);

    return fid;
  } catch (err) {
    console.error('Ошибка при загрузке в SeaweedFS:', err);
    throw err;
  }
}

module.exports = uploadToSeaweedFS;
