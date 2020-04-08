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
        socket.close();
        socket = null;
        log.info("TCP client stopped");
    }

    @Override
    public void run() {
        Socket socket = null;
        char delimiter = endpoint.getRecordDelimiter().charAt(0);
        do {
            try {
                if (socket == null) {
                    if (failover) {
                        socket = new Socket(endpoint.getFailoverHost(), endpoint.getFailoverPort());
                        failover = false;
                        log.info("TCP Client Connected to the failover Server on port : "+ endpoint.getFailoverPort);
                    } else {
                        socket = new Socket(endpoint.getHost(), endpoint.getPort());
                        log.info("TCP Client Connected to the Server on port : " + endpoint.getPort);
                    }
                    input = socket.getInputStream();
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try {
                    int next = input.read();
                    while (next > -1) {
                        if ((char) next == delimiter && bos != null) {
                            bos.flush();
                            byte[] result = bos.toByteArray();
                            //Process messages using Single Thread
                            TCPWorker worker = new TCPWorker(result, endpoint, socket);
                            worker.run();
                            //Process messages using Multiple Threads
                            //workerPool.execute(new TCPWorker(s, endpoint, socket));
                            //bos.close();
                            bos = null;
                            next = input.read();
                            continue;
                        }
                        if (bos == null) {
                            bos = new ByteArrayOutputStream();
                        }
                        if ((char) next != delimiter) {
                            bos.write(next);
                        }
                        next = input.read();
                    }
                    if (bos != null) {
                        byte[] result = bos.toByteArray();
                        //Process messages using Single Thread
                        TCPWorker worker = new TCPWorker(result, endpoint, socket);
                        worker.run();
                        //Process messages using Multiple Threads
                        //workerPool.execute(new TCPWorker(s, endpoint, socket));
                        bos.close();
                    }
                } catch (IOException e) {
                    log.error("Error while reading the message from the server", e);
                } finally {
                    try {
                        if (input != null) {
                            input.close();
                        }
                    } catch (IOException e) {
                    }
                }
            } catch (IOException e) {
                log.error("Error while creating the socket connection", e);
            } finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    //ignore
                }
                failover = true;
                socket = null;
            }
        } while (socket == null);
    }
}
