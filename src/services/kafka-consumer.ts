const { Kafka } = require('kafkajs');
const { asnOutrightBarcodeDetails, asnBarcodeDetails } = require('../controllers/OneRecieivingController');

const kafkaBroker = process.env.KAFKA_BROKER;
const kafkaClientId = process.env.KAFKA_CLIENT_ID;
const kafkaGroupId = process.env.KAFKA_GROUP_ID;
const kafkaTopic = process.env.KAFKA_TOPIC;

const kafkaConsumer = new Kafka({
    clientId: kafkaClientId,
    brokers: [kafkaBroker],
});

async function createConsumer(groupId = kafkaGroupId) {
    const consumer = kafkaConsumer.consumer({ groupId });
    await consumer.connect();
    
    console.log('Kafka consumer connected');
    return consumer;
}

async function consumeOutrightBarcodeMessage(consumer: any, topic: any) {
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }: any) => {
            await asnBarcodeDetails(message.value.toString(), topic, partition);
        },
    });
}

async function main() {
    const consumer = await createConsumer();
    await consumeOutrightBarcodeMessage(consumer, kafkaTopic);
}

module.exports = {
    main
};


