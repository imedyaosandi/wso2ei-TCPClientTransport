package org.wso2.transport.tcp.client;

import java.net.*;
import java.io.*;
import java.io.IOException;

import org.apache.axis2.transport.base.threads.WorkerPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TCPClient implements Runnable {

    private static final Log log = LogFactory.getLog(TCPClient.class);
    // initialize class variables
    private Socket socket;
    private TCPEndpoint endpoint;
    private WorkerPool workerPool;

    // constructor
    public TCPClient(TCPEndpoint endpoint, WorkerPool workerPool) {
        this.endpoint = endpoint;
        this.workerPool = workerPool;

    }

    public void connect() throws IOException {
        Thread readThread=new Thread(this);
        readThread.start();
    }

    public void closeSocket() throws IOException {
        log.info("TCP client stopping......");
        socket = null;
        log.info("TCP client stopped");
    }

    @Override
    public void run() {
        Socket socket = null;
        String del = endpoint.getRecordDelimiter();
        do {
            try {
                if (socket == null) {
                    socket = new Socket(endpoint.getHost(), endpoint.getPort());
                    log.info("TCP Client Connected to Server");
                }
                DataInputStream input = new DataInputStream(socket.getInputStream());
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try {
                    int next = input.read();
                    while (next > -1) {
                        bos.write(next);
                        next = input.read();
                        if (input.available() <= 0) {
                            if (next > -1) {
                                bos.write(next);
                            }
                            String[] segments = bos.toString().split(del);
                            for (String s : segments) {
                                //Process messages using Single Thread
                                TCPWorker worker=new TCPWorker(s,endpoint,socket);
                                worker.run();
                                //Process messages using Multiple Threads
                                //workerPool.execute(new TCPWorker(s, endpoint, socket));
                            }

                            bos = new ByteArrayOutputStream();
                            break;
                            }
                    }
                } catch (IOException e) {
                    log.error("Error while reading the message from the server",e);
                } finally {
                    try {
                        bos.close();
                    } catch (IOException e) {
                        log.error("Error while closing the output stream",e);
                    }
                }
                socket.close();
                socket = null;
            } catch (IOException e) {
                log.error("Error while creating the socket connection",e);
            }

        } while (socket == null);
    }
}
