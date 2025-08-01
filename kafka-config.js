// kafka-config.js
import { Kafka } from 'kafkajs';

// Create Kafka client
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

// Producer example
export async function createProducer() {
  const producer = kafka.producer();
  await producer.connect();
  
  console.log('Kafka producer connected');
  return producer;
}

// Consumer example
export async function createConsumer(groupId = 'my-group') {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  
  console.log('Kafka consumer connected');
  return consumer;
}

// Send message
export async function sendMessage(producer, topic, message) {
  await producer.send({
    topic,
    messages: [
      {
        partition: 0,
        key: 'key1',
        value: JSON.stringify(message),
      },
    ],
  });
  
  console.log('Message sent:', message);
}

// Consume messages
export async function consumeMessages(consumer, topic) {
  await consumer.subscribe({ topic, fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value.toString()}`);
      console.log(`Topic: ${topic}, Partition: ${partition}`);
      console.log(JSON.parse(message.value).data);
      console.log(JSON.parse(message.value).device);
    },
  });
}

// Example usage
async function main() {
  try {
    // Create producer and consumer
    const producer = await createProducer();
    const consumer = await createConsumer();
    
    const topic = 'test-topic';
    
    // Start consuming
    await consumeMessages(consumer, topic);
    
    // Send a test message
    await sendMessage(producer, topic, { 
      id: 4, 
      message: 'Hello from Bun!', 
      timestamp: new Date().toISOString() 
    });
    
    // Keep the process running
    // process.stdin.resume();
    
  } catch (error) {
    console.error('Error:', error);
  }
}

// Run if this file is executed directly
if (import.meta.main) {
  main();
}