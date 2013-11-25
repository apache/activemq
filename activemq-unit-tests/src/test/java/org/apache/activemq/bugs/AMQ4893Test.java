package org.apache.activemq.bugs;

import java.io.IOException;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4893Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4893Test.class);

    @Test
    public void testPropertiesInt() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setIntProperty("TestProp", 333);
        fakeUnmarshal(message);
        roundTripProperties(message);
    }

    @Test
    public void testPropertiesString() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setStringProperty("TestProp", "Value");
        fakeUnmarshal(message);
        roundTripProperties(message);
    }

    @Test
    public void testPropertiesObject() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObjectProperty("TestProp", "Value");
        fakeUnmarshal(message);
        roundTripProperties(message);
    }

    @Test
    public void testPropertiesObjectNoMarshalling() throws Exception {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setObjectProperty("TestProp", "Value");
        roundTripProperties(message);
    }

    private void roundTripProperties(ActiveMQObjectMessage message) throws IOException, JMSException {
        ActiveMQObjectMessage copy = new ActiveMQObjectMessage();
        for (Map.Entry<String, Object> prop : message.getProperties().entrySet()) {
            LOG.debug("{} -> {}", prop.getKey(), prop.getValue().getClass());
            copy.setObjectProperty(prop.getKey(), prop.getValue());
        }
    }

    private void fakeUnmarshal(ActiveMQObjectMessage message) throws IOException {
        // we need to force the unmarshalled property field to be set so it
        // gives us a hawtbuffer for the string
        OpenWireFormat format = new OpenWireFormat();
        message.beforeMarshall(format);
        message.afterMarshall(format);

        ByteSequence seq = message.getMarshalledProperties();
        message.clearProperties();
        message.setMarshalledProperties(seq);
    }
}