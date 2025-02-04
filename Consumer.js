const { Kafka } = require("kafkajs");
const axios = require("axios");
const FormData = require("form-data"); 
const fs = require("fs");

// Kafka Client
const kafka = new Kafka({
  clientId: "file-consumer",
  brokers: ["localhost:9092"],
});

// Consumer
const consumer = kafka.consumer({ groupId: "test-topic" });

// URL SeaweedFS
const SEAWEEDFS_MASTER = "http://localhost:9333";

async function uploadToSeaweedFS(fileBuffer) {
  try {
    // 1. Запрашиваем Volume Server для хранения файла
    const assignRes = await axios.get(`${SEAWEEDFS_MASTER}/dir/assign`);
    const { fid, url } = assignRes.data;

    // 2. Создаём объект FormData
    const formData = new FormData();
    formData.append("file", fileBuffer, { filename: "uploaded_data.txt" });

    // 3. Отправляем файл на Volume Server
    const uploadRes = await axios.post(`http://${url}/${fid}`, formData, {
      headers: formData.getHeaders(),
    });

    if (uploadRes.status === 201) {
      console.log(`✅ Файл загружен в SeaweedFS с ID: ${fid}`);
      return fid;
    } else {
      console.error("Ошибка загрузки файла:", uploadRes.data);
      return null;
    }
  } catch (error) {
    console.error("Ошибка при загрузке в SeaweedFS:", error);
    return null;
  }
}

async function runConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("📩 Получен файл из Kafka, загружаем в SeaweedFS...");

      const fileBuffer = Buffer.from(message.value);
      const fileId = await uploadToSeaweedFS(fileBuffer);

      if (fileId) {
        console.log(`✅ Файл сохранён в SeaweedFS с File ID: ${fileId}`);
      }
    },
  });
}

runConsumer().catch(console.error);
