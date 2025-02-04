const fs = require('fs');
const { Kafka } = require('kafkajs');
const { kafka, paths } = require('./config');

async function runProducer() {
  // 1. Проверяем файл
  if (!fs.existsSync(paths.fileToSend)) {
    console.error(`Файл не найден: ${paths.fileToSend}`);
    process.exit(1);
  }

  // 2. Считываем данные
  const fileBuffer = fs.readFileSync(paths.fileToSend);
  console.log(`Отправляем файл размером ${fileBuffer.length} байт в Kafka...`);

  // 3. Инициализируем Kafka
  const kafkaClient = new Kafka({
    clientId: kafka.clientId,
    brokers: kafka.brokers
  });

  const producer = kafkaClient.producer();

  try {
    await producer.connect();
    await producer.send({
      topic: kafka.topic,
      messages: [{ value: fileBuffer }]
    });
    console.log('Сообщение успешно отправлено в Kafka.');
  } catch (err) {
    console.error('Ошибка отправки в Kafka:', err);
  } finally {
    await producer.disconnect();
  }
}

runProducer();
