import { Server } from 'socket.io';
import Redis from 'ioredis';
import prismaClient from './prisma';
import { produceMessage } from './kafka';

const pub = new Redis({
    host: process.env.REDIS_HOST as string,
    port: process.env.REDIS_PORT as unknown as number,
    username: process.env.REDIS_USER as string,
    password: process.env.REDIS_PASSWORD as string,
});
const sub = new Redis({
    host: process.env.REDIS_HOST as string,
    port: process.env.REDIS_PORT as unknown as number,
    username: process.env.REDIS_USER as string,
    password: process.env.REDIS_PASSWORD as string,
});

class SocketService {
    private _io: Server;
    constructor() {
        console.log('Init Socket Server...');
        this._io = new Server({
            cors: {
                allowedHeaders: ["*"],
                origin: "*",
            }
        });
        sub.subscribe('MESSAGES');
    }

    public intiListeners() {
        const io = this._io;
        console.log('Init Socket Listeners...');
        io.on('connect', (socket) => {
            console.log(`Socket connected: ${socket.id}`);

            socket.on('event:message', async ({ message }: { message: string }) => {
                console.log(`New Message received: ${message}`);
                // Publish message to redis

                await pub.publish('MESSAGES', JSON.stringify({ message }));
            })
        });

        sub.on('message', async (channel, message) => {
            if (channel === 'MESSAGES') {
                io.emit('message', message);
                //DB store
                await produceMessage(message);
                console.log("Message Produced to Kafka Broker");
            }
        })
    }

    get io() {
        return this._io;
    }
}

export default new SocketService();