module.exports = {
  kafka: {
    clientId: 'my-app',
    brokers: ['localhost:9092'],
    topic: 'file_topic'
  },
  seaweed: {
    masterHost: 'localhost',
    masterPort: 9333,
  },
  paths: {
    fileToSend: './data.txt',
    downloadedFile: './downloaded_data.txt'
  }
};
