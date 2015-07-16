package org.apache.activemq.broker.transport.protocol;


public interface ProtocolVerifier {

    public boolean isProtocol(byte[] value);


}
