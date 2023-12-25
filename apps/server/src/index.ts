import http from 'http';
import SocketService from './services/socket';
import { startMessageConsumer } from './services/kafka';

async function init() {
    startMessageConsumer();
    const socketService = SocketService;

    const httpServer = http.createServer();
    const port = process.env.PORT ? process.env.PORT : 8000;

    socketService.io.attach(httpServer);

    httpServer.listen(port, () => {
        console.log(`HTTP Server is running on port ${port}`);
    });

    socketService.intiListeners();
}

init();
