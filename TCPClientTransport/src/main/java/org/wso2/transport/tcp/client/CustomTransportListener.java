package org.wso2.transport.tcp.client;

import org.apache.axis2.AxisFault;
import org.apache.axis2.transport.base.AbstractTransportListenerEx;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class CustomTransportListener extends AbstractTransportListenerEx<TCPEndpoint> {
    private static final Log log = LogFactory.getLog(CustomTransportListener.class);

    private Map<TCPEndpoint, TCPClient> serverTable = new ConcurrentHashMap<TCPEndpoint, TCPClient>();


    protected void doInit() throws AxisFault {

    }

    protected TCPEndpoint createEndpoint() {

        return new TCPEndpoint();
    }



    protected void startEndpoint(TCPEndpoint endpoint) throws AxisFault {
        try {
            TCPClient client = new TCPClient(endpoint,workerPool);
            serverTable.put(endpoint, client);
            client.connect();
        } catch (IOException e) {
            handleException("Error while starting the TCP endpoint", e);
        }
    }

    protected void stopEndpoint(TCPEndpoint endpoint) {
        try {

            TCPClient client = serverTable.get(endpoint);
            if (client != null) {
                client.closeSocket();
            }
        } catch (IOException e) {
            log.error("Error while stopping the TCP endpoint", e);
        } finally {
            serverTable.remove(endpoint);
        }
    }

    }

