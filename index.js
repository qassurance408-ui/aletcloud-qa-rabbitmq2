const amqp = require('amqplib');

const user = process.env.RABBITMQ_USER;
const pass = process.env.RABBITMQ_PASS;

console.log(`connecting with user: "${user}"`); 

const RABBITMQ_URL = `amqp://${user}:${pass}@rabbitmq:5672`;

async function connect() {
  while (true) {
    try {
      const conn = await amqp.connect({
    protocol: 'amqp',
    hostname: 'rabbitmq',
    port: 5672,
    username: process.env.RABBITMQ_USER,
    password: process.env.RABBITMQ_PASS
});
      console.log('Connected to RabbitMQ!');
      return conn;
    } catch (e) {
      console.log(`Connection failed: ${e.message}. Retrying in 5s...`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function run() {
  const conn = await connect();
  const channel = await conn.createChannel();

  await channel.assertQueue('test_queue');
  await channel.assertQueue('response_queue');

  // Send a test message
  channel.sendToQueue('test_queue', Buffer.from('Hello from QA!'));
  console.log('Sent: Hello from QA!');

  // Consume and stay alive
  channel.consume('test_queue', (msg) => {
    if (msg) {
      console.log(`Received: ${msg.content.toString()}`);
      channel.ack(msg);
      channel.sendToQueue('response_queue', Buffer.from(`QA ACK: ${msg.content.toString()}`));
      console.log('Sent acknowledgement to response_queue');
    }
  });

  console.log('Waiting for messages...');
}

run().catch(console.error);
