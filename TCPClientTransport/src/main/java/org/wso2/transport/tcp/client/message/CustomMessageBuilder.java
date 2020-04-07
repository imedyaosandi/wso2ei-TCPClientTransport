package org.wso2.transport.tcp.client.message;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.context.MessageContext;

import java.io.InputStream;

public class CustomMessageBuilder implements Builder {
    @Override
    public OMElement processDocument(InputStream inputStream, String s, MessageContext messageContext) throws AxisFault {
        return null;
    }
}
