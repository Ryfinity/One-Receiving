const { Kafka } = require('kafkajs');
const { asnOutrightBarcodeDetails, asnBarcodeDetails } = require('../controllers/OneRecieivingController');

const kafkaBroker = process.env.KAFKA_BROKER;
const kafkaClientId = process.env.KAFKA_CLIENT_ID;
const kafkaGroupId = process.env.KAFKA_GROUP_ID;
const kafkaTopic =  "onepdt-rcv-scan-producer"// process.env.KAFKA_TOPIC;

const kafkaProducer = new Kafka({
    clientId: kafkaClientId,
    brokers: [kafkaBroker],
});

async function createProducer(groupId = kafkaGroupId) {
    const producer = kafkaProducer.producer();
    await producer.connect();
    
    console.log('Kafka producer connected');
    return producer;
}

async function sendMessage(producer: any, topic: any, message: any) {
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

async function main(data: any) {
    const producer = await createProducer();

    await sendMessage(producer, kafkaTopic, { 
        ...data
    });
}

module.exports = {
    main
};


