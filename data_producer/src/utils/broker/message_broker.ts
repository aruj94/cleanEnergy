import { Kafka, logLevel, Partitioners, Producer } from "kafkajs";
import { PublishType } from "./broker_types";

// configuration properties
const CLIENT_ID = process.env.CLIENT_ID
const BROKERS = [process.env.BROKER_1 || "localhost:9092"]; // specify additional brokers here
const TOPICS = [process.env.SOLAR, process.env.WIND, process.env.HYDRO]

const kafka = new Kafka({
    clientId: CLIENT_ID,
    brokers: BROKERS,
    logLevel: logLevel.INFO
});

let producer: Producer;

const createTopic = async (topic: string[]) => {
    const topics = topic.map((t) => ({
        topic: t,
        numPartitions: 2,
        replicationFactor: 1
    }));

    const admin = kafka.admin();
    await admin.connect();
    const topicExists = await admin.listTopics();
    console.log("topicExists", topicExists);

    for (const t of topics) {
        if (!topicExists.includes(t.topic)) {
            await admin.createTopics({
                topics: [t],
            });
        }
    }

    await admin.disconnect();
}

export const connectProducer = async <T>(): Promise<T> => {
    // convert topics env variables into a string array
    let string_topics: string[] = [];

    for (const t of TOPICS) {
        let temp_str = "" + t;
        string_topics.push(temp_str);
    }

    // create topic
    await createTopic(string_topics);
    
    if (producer) {
        console.log("Producer connected with pre-existing connection");
        return producer as unknown as T;
    }

    producer = kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner,
    });

    await producer.connect();
    console.log("Producer connected with new connection");
    return producer as unknown as T;
}

export const disconnectProducer = async (): Promise<void> => {
    if (producer) {
        await producer.disconnect();
    }
}

export const publishmessage = async ( data: PublishType ): Promise<boolean> => {
    await connectProducer();
    const result = await producer.send ({
        topic: data.topic,
        messages: [
            { key: data.event, value: JSON.stringify(data.message), headers: data.headers }
        ],
    });

    console.log("publishinbg result", result);
    return result.length > 0;
}
