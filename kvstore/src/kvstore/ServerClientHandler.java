package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of the methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

    private KVServer kvServer;
    private ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number passed in as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        // implement me
        this.kvServer = kvServer;
        threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        // implement me
        threadPool.addJob(new ClientHandler(client));
    }

    /**
     * Runnable class containing routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {

        private Socket client;

        /**
         * Construct a ClientHandler.
         *
         * @param client Socket connected to client with the request
         */
        public ClientHandler(Socket client) {
            this.client = client;
        }

        /**
         * Processes request from client and sends back a response with the
         * result. The delivery of the response is best-effort. If we are
         * unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
            KVMessage response;
            try {
                KVMessage mess = new KVMessage(client);
                String request = mess.getMsgType();
                response = new KVMessage(RESP, SUCCESS);
                if (request.equals(PUT_REQ))
                    kvServer.put(mess.getKey(), mess.getValue());
                else if (request.equals(DEL_REQ))
                    kvServer.del(mess.getKey());
                else if (request.equals(GET_REQ))
                    response.setValue(kvServer.get(mess.getKey()));
                else
                    throw new KVException("Invalid request");
            } catch (KVException kve) {
                response = kve.getKVMessage();
            }
            try {
                response.sendMessage(client);
            } catch (KVException kve) {
                // do nothing (no way to report to client that sending a message to it failed)
            }
        }
    }

}
