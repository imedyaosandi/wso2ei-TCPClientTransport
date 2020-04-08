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
    private boolean running = true;
    private InputStream input;
    private boolean failover = false;

    // constructor
    public TCPClient(TCPEndpoint endpoint, WorkerPool workerPool) {
        this.endpoint = endpoint;
        this.workerPool = workerPool;

    }

    public void connect() throws IOException {
        Thread readThread=new Thread(this,"tcpClientThread");
        readThread.start();
    }

    public void closeSocket() throws IOException {
        running = false;
        log.info("TCP client stopping......");
        if (input != null) {
            try {
                input.close();
            } catch (Exception e) {
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception e) {
            }
        }
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
                        log.info("TCP Client Connected to the failover Server on port : " + endpoint.getFailoverPort);
                    } else {
                        socket = new Socket(endpoint.getHost(), endpoint.getPort());
                        log.info("TCP Client Connected to the Server on port : " + endpoint.getPort);
                    }
                    input = socket.getInputStream();
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try {
                    int next = -1;
                    while ((next = input.read()) > -1) {
                        if ((char) next == delimiter) {
                            bos.flush();
                            byte[] result = bos.toByteArray();

                            //Process messages using a Single Thread
                            TCPWorker worker = new TCPWorker(result, endpoint, socket);
                            worker.run();

                            //Process messages using Multiple Threads
                            //workerPool.execute(new TCPWorker(result, endpoint, socket));

                            bos.reset();
                        } else {
                            bos.write(next);
                        }
                    }
                } catch (IOException e) {
                    if (running) {
                        log.error("Error while reading the message from the server", e);
                    }
                } catch (Exception e) {
                    log.error("Exception occurred while processing message", e);
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
                if (failover) {
                    failover = false;
                } else {
                    failover = true;
                }
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException ex) {
                        //ignore
                    }
                }
            } catch (Exception e) {
                log.error("Exception occurred while processing message", e);
            } finally {
                socket = null;
            }
        }while (running && socket == null);
}
