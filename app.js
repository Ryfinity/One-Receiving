import { Kafka } from 'kafkajs';

const clientId = 'onepdt-sample-client-1';
const brokers = ['lt-6373.smretailinc.com:9092'];
const topic = 'onepdt-rcv-scan';
const consumerGroupId = 'onepdt-sample-group-1';

async function runConsumer() {
  const kafka = new Kafka({
    clientId: clientId,
    brokers: brokers,
  });

  const consumer = kafka.consumer({ groupId: consumerGroupId });

  try {
    await consumer.connect();
    console.log('Consumer connected successfully.');

    await consumer.subscribe({ topic: topic, fromBeginning: true });
    console.log(`Subscribed to topic: ${topic}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {

        try {
        console.log(JSON.parse( message.value.toString()));
        }
        catch(e) {
          console.log('error', e);
        }
      },
    });

  } catch (error) {
    console.error('Error with Kafka consumer:', error);
  } finally {
    // You might want to handle graceful shutdown here, e.g., on process exit
    process.on('SIGTERM', async () => {
      console.log('SIGTERM received. Disconnecting consumer...');
      await consumer.disconnect();
      console.log('Consumer disconnected.');
      process.exit(0);
    });

    process.on('SIGINT', async () => {
      console.log('SIGINT received. Disconnecting consumer...');
      await consumer.disconnect();
      console.log('Consumer disconnected.');
      process.exit(0);
    });
  }
}

runConsumer().catch(console.error);