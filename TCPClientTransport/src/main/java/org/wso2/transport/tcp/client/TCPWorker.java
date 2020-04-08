package org.wso2.transport.tcp.client;

import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisEngine;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.util.MessageContextBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class TCPWorker implements Runnable {
    private static final Log log = LogFactory.getLog(TCPClient.class);

    private byte[] input;
    private TCPEndpoint endpoint;
    private Socket socket;

    public TCPWorker(byte[] data, TCPEndpoint endpoint, Socket socket) {
        this.input = data;
        this.endpoint = endpoint;
        this.socket = socket;
    }

    @Override
    public void run() {
        //debug log to check received messages.To enable
        String string = new String(input);
        log.info("Server input string :  " + string);

        MessageContext msgContext = null;
        try {
            msgContext = endpoint.createMessageContext();
            InetAddress remoteAddress = ((InetSocketAddress) socket.getRemoteSocketAddress()).getAddress();
            msgContext.setProperty(MessageContext.REMOTE_ADDR, remoteAddress.getHostAddress());
            msgContext.setProperty(TCPConstants.PARAM_HOST, getHostName(remoteAddress));
            msgContext.setIncomingTransportName(ClientConstants.TRANSPORT_NAME);
            TCPOutTransportInfo outInfo = new TCPOutTransportInfo();
            outInfo.setSocket(socket);
            outInfo.setClientResponseRequired(endpoint.isClientResponseRequired());
            outInfo.setContentType(endpoint.getContentType());
            String delimiter = endpoint.getRecordDelimiter();
            String delimiterType = endpoint.getRecordDelimiterType();
            outInfo.setDelimiter(delimiter);
            outInfo.setDelimiterType(delimiterType);
            msgContext.setProperty(Constants.OUT_TRANSPORT_INFO, outInfo);
            // create the SOAP Envelope
            handleEnvelope(msgContext, input);
        } catch (Exception e) {
            sendFault(msgContext, e);
        }
    }

    private void handleEnvelope(MessageContext msgContext, byte[] value) throws AxisFault {
        ByteArrayInputStream bais = null;
        try {
            bais = new ByteArrayInputStream(value);
            SOAPEnvelope envelope = TransportUtils.createSOAPMessage(msgContext,
                    bais, endpoint.getContentType());
            msgContext.setEnvelope(envelope);
            AxisEngine.receive(msgContext);
        } catch (IOException e) {
            sendFault(msgContext, e);
        } catch (XMLStreamException e) {
            sendFault(msgContext, e);
        } finally {
            if (bais != null) {
                try {
                    bais.close();
                } catch (IOException e) {
                    sendFault(msgContext, e);
                }
            }
        }
    }


    private void sendFault(MessageContext msgContext, Exception fault) {
        log.error("Error while processing TCP Client request through the Axis2 engine", fault);
        try {
            if (msgContext != null) {
                log.info("message context   " + msgContext + " tout  " + MessageContext.TRANSPORT_OUT);
                msgContext.setProperty(MessageContext.TRANSPORT_OUT, input);

                MessageContext faultContext =
                        MessageContextBuilder.createFaultMessageContext(msgContext, fault);

                AxisEngine.sendFault(faultContext);
            }
        } catch (Exception e) {
            log.error("Error while sending the fault response", e);
        }
    }

    private static String getHostName(InetAddress address) {
        String result;
        String hostAddress = address.getHostAddress();
        String inetAddress = address.toString();
        int index1 = inetAddress.lastIndexOf('/');
        int index2 = inetAddress.indexOf(hostAddress);
        if (index2 == index1 + 1) {
            if (index1 == 0) {
                result = hostAddress;
            } else {
                result = inetAddress.substring(0, index1);
            }
        } else {
            result = hostAddress;
        }
        return result;
    }


}

