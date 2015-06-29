package org.apache.activemq.transport.ws;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SocketTest {

    @Test
    public void testStompSocketRemoteAddress() {

        org.apache.activemq.transport.ws.jetty8.StompSocket stompSocketJetty8 =
                new org.apache.activemq.transport.ws.jetty8.StompSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", stompSocketJetty8.getRemoteAddress());

        org.apache.activemq.transport.ws.jetty9.StompSocket stompSocketJetty9 =
                new org.apache.activemq.transport.ws.jetty9.StompSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", stompSocketJetty9.getRemoteAddress());
    }

    @Test
    public void testMqttSocketRemoteAddress() {

        org.apache.activemq.transport.ws.jetty8.MQTTSocket mqttSocketJetty8 =
                new org.apache.activemq.transport.ws.jetty8.MQTTSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", mqttSocketJetty8.getRemoteAddress());

        org.apache.activemq.transport.ws.jetty8.MQTTSocket mqttSocketJetty9 =
                new org.apache.activemq.transport.ws.jetty8.MQTTSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", mqttSocketJetty9.getRemoteAddress());
    }

}
