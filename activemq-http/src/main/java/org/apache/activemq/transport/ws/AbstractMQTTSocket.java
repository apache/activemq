package org.apache.activemq.transport.ws;

import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.mqtt.MQTTInactivityMonitor;
import org.apache.activemq.transport.mqtt.MQTTProtocolConverter;
import org.apache.activemq.transport.mqtt.MQTTTransport;
import org.apache.activemq.transport.mqtt.MQTTWireFormat;
import org.apache.activemq.util.ServiceStopper;

public abstract class AbstractMQTTSocket extends TransportSupport implements MQTTTransport, BrokerServiceAware {

    protected MQTTWireFormat wireFormat = new MQTTWireFormat();
    protected final CountDownLatch socketTransportStarted = new CountDownLatch(1);
    protected MQTTProtocolConverter protocolConverter = null;
    private BrokerService brokerService;
    protected final String remoteAddress;

    public AbstractMQTTSocket(String remoteAddress) {
        super();
        this.remoteAddress = remoteAddress;
    }

    protected boolean transportStartedAtLeastOnce() {
        return socketTransportStarted.getCount() == 0;
    }

    protected void doStart() throws Exception {
        socketTransportStarted.countDown();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
    }

    protected MQTTProtocolConverter getProtocolConverter() {
        if( protocolConverter == null ) {
            protocolConverter = new MQTTProtocolConverter(this, brokerService);
        }
        return protocolConverter;
    }

    @Override
    public int getReceiveCounter() {
        return 0;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return new X509Certificate[0];
    }

    @Override
    public MQTTInactivityMonitor getInactivityMonitor() {
        return null;
    }

    @Override
    public MQTTWireFormat getWireFormat() {
        return wireFormat;
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }
}
