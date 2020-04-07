package org.wso2.transport.tcp.client.message;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNode;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axiom.om.OMText;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.MessageFormatter;

import java.io.OutputStream;
import java.net.URL;

public class CustomMessageFormatter implements MessageFormatter {

    @Override
    public byte[] getBytes(MessageContext messageContext, OMOutputFormat omOutputFormat) throws AxisFault {
        return new byte[0];
    }

    @Override
    public void writeTo(MessageContext messageContext, OMOutputFormat omOutputFormat, OutputStream outputStream, boolean b) throws AxisFault {

        try {

        } catch (Exception ex) {
            throw new AxisFault(
                    "An exception occured in sending the  message");
        }
    }

    @Override
    public String getContentType(MessageContext messageContext, OMOutputFormat omOutputFormat, String s) {
        return null;
    }

    @Override
    public URL getTargetAddress(MessageContext messageContext, OMOutputFormat omOutputFormat, URL url) throws AxisFault {
        return null;
    }

    @Override
    public String formatSOAPAction(MessageContext messageContext, OMOutputFormat omOutputFormat, String s) {
        return null;
    }



}
