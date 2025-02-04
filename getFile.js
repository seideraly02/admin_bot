// getFile.js
const fs = require('fs');
const axios = require('axios');
const { seaweed, paths } = require('./config');

// Пример: node getFile.js 6,023a70d539 192.168.0.101:8080
// Или: node getFile.js "6,023a70d539" "localhost:8080"
async function getFile(fid, volumeUrl) {
  try {
    // Формируем запрос
    const downloadUrl = `http://${volumeUrl}/${fid}`;
    console.log(`Скачиваем файл по адресу: ${downloadUrl}`);

    // axios по умолчанию пытается распарсить ответ как JSON, 
    // поэтому укажем `responseType: 'arraybuffer'`, чтобы сохранить бинарные данные
    const response = await axios.get(downloadUrl, { responseType: 'arraybuffer' });

    if (response.status !== 200) {
      throw new Error(`Ошибка при скачивании файла. Код: ${response.status}`);
    }

    // Сохраняем полученные байты в локальный файл
    fs.writeFileSync(paths.downloadedFile, response.data);
    console.log(`Файл сохранён как ${paths.downloadedFile} (размер: ${response.data.length} байт)`);
  } catch (err) {
    console.error('Ошибка при скачивании файла:', err);
  }
}

// Если хотим передавать параметры из CLI:
const [,, fidArg, volumeUrlArg] = process.argv; 
if (!fidArg || !volumeUrlArg) {
  console.error('Необходимо указать fid и volumeUrl, пример: node getFile.js 6,023a70d539 192.168.0.101:8080');
  process.exit(1);
}
getFile(fidArg, volumeUrlArg);
