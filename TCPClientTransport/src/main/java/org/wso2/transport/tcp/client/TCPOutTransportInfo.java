package org.wso2.transport.tcp.client;

import org.apache.axis2.transport.OutTransportInfo;

import java.net.Socket;

public class TCPOutTransportInfo implements OutTransportInfo {

    private Socket socket;
    private String contentType;
    private boolean clientResponseRequired = false;
    private String delimiter;
    private String delimiterType;

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getContentType() {
        return contentType;
    }

    public boolean isClientResponseRequired() {
        return clientResponseRequired;
    }

    public void setClientResponseRequired(boolean clientResponseRequired) {
        this.clientResponseRequired = clientResponseRequired;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiterType() {
        return delimiterType;
    }

    public void setDelimiterType(String delimiterType) {
        this.delimiterType = delimiterType;
    }
}
