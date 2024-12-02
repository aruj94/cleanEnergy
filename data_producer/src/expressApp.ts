import express, { Request, Response } from "express";
import cors from 'cors';
import { Producer } from "kafkajs";
import { connectProducer } from "./utils/broker/message_broker";

export const ExpressApp = async () => {
    const app = express();
    app.use(cors());
    app.use(express.json());

    // connect producer and consumer
    const producer = await connectProducer<Producer>();
    producer.on("producer.connect", () => {
        console.log("producer connected");
    });

    /*const consumer = await MessageBroker.connectConsumer<Consumer>();
    consumer.on("consumer.connect", () => {
        console.log("consumer connected");
    });

    // subscribe to the topic or publish a message
    await MessageBroker.subscribe((message) => {
        console.log("Consumer reveive the message");
        console.log("Message received", message);
    }, "OrderEvents");*/

    app.get('/', (req: Request, res: Response) => {
        res.send('Hello World from TypeScript and Express!');
    });

    return app;
}
