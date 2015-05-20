/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.UUID;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompMissingMessageTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompMissingMessageTest.class);

    protected String destination;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        destination = "/topic/" + getTopicName();
    }

    @Test(timeout = 60000)
    public void testProducerConsumerLoop() throws Exception {
        final int ITERATIONS = 500;
        int received = 0;

        for (int i = 1; i <= ITERATIONS*2; i+=2) {
            if (doTestProducerConsumer(i) != null) {
                received++;
            }
        }

        assertEquals(ITERATIONS, received);
    }

    public String doTestProducerConsumer(int index) throws Exception {
        String message = null;

        assertEquals("Should not be any consumers", 0, brokerService.getAdminView().getTopicSubscribers().length);

        StompConnection producer = stompConnect();
        StompConnection consumer = stompConnect();

        subscribe(consumer, Integer.toString(index));

        sendMessage(producer, index);

        try {
            StompFrame frame = consumer.receive();
            LOG.debug("Consumer got frame: " + message);
            assertEquals(index, (int) Integer.valueOf(frame.getBody()));
            message = frame.getBody();
        } catch(Exception e) {
            fail("Consumer["+index+"] got error while consuming: " + e.getMessage());
        }

        unsubscribe(consumer, Integer.toString(index));

        stompDisconnect(consumer);
        stompDisconnect(producer);

        return message;
    }

    @Test(timeout = 60000)
    public void testProducerDurableConsumerLoop() throws Exception {
        final int ITERATIONS = 500;
        int received = 0;

        for (int i = 1; i <= ITERATIONS*2; i+=2) {
            if (doTestProducerDurableConsumer(i) != null) {
                received++;
            }
        }

        assertEquals(ITERATIONS, received);
    }

    public String doTestProducerDurableConsumer(int index) throws Exception {
        String message = null;

        assertEquals("Should not be any consumers", 0, brokerService.getAdminView().getTopicSubscribers().length);

        StompConnection producer = stompConnect();
        StompConnection consumer = stompConnect("test");

        subscribe(consumer, Integer.toString(index), true);

        sendMessage(producer, index);

        try {
            StompFrame frame = consumer.receive();
            LOG.debug("Consumer got frame: " + message);
            assertEquals(index, (int) Integer.valueOf(frame.getBody()));
            message = frame.getBody();
        } catch(Exception e) {
            fail("Consumer["+index+"] got error while consuming: " + e.getMessage());
        }

        unsubscribe(consumer, Integer.toString(index));

        stompDisconnect(consumer);
        stompDisconnect(producer);

        return message;
    }

    protected void subscribe(StompConnection stompConnection, String subscriptionId) throws Exception {
        subscribe(stompConnection, subscriptionId, false);
    }

    protected void subscribe(StompConnection stompConnection, String subscriptionId, boolean durable) throws Exception {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("id", subscriptionId);
        if (durable) {
            headers.put("activemq.subscriptionName", subscriptionId);
        }
        headers.put(Stomp.Headers.RECEIPT_REQUESTED, UUID.randomUUID().toString());

        stompConnection.subscribe(destination, "auto", headers);

        StompFrame received = stompConnection.receive();
        assertEquals("RECEIPT", received.getAction());
        String receipt = received.getHeaders().get(Stomp.Headers.Response.RECEIPT_ID);
        assertEquals(headers.get(Stomp.Headers.RECEIPT_REQUESTED), receipt);
    }

    protected void unsubscribe(StompConnection stompConnection, String subscriptionId) throws Exception {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("id", subscriptionId);
        headers.put(Stomp.Headers.RECEIPT_REQUESTED, UUID.randomUUID().toString());

        stompConnection.unsubscribe(destination, headers);

        StompFrame received = stompConnection.receive();
        assertEquals("RECEIPT", received.getAction());
        String receipt = received.getHeaders().get(Stomp.Headers.Response.RECEIPT_ID);
        assertEquals(headers.get(Stomp.Headers.RECEIPT_REQUESTED), receipt);
    }

    protected void sendMessage(StompConnection producer, int index) throws Exception {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.RECEIPT_REQUESTED, UUID.randomUUID().toString());

        producer.send(destination, Integer.toString(index), null, headers);

        StompFrame received = producer.receive();
        assertEquals("RECEIPT", received.getAction());
        String receipt = received.getHeaders().get(Stomp.Headers.Response.RECEIPT_ID);
        assertEquals(headers.get(Stomp.Headers.RECEIPT_REQUESTED), receipt);
    }

    @Override
    protected StompConnection stompConnect() throws Exception {
        StompConnection stompConnection = new StompConnection();
        stompConnect(stompConnection);
        stompConnection.connect("system", "manager", null);
        return stompConnection;
    }

    protected StompConnection stompConnect(String clientId) throws Exception {
        StompConnection stompConnection = new StompConnection();
        stompConnect(stompConnection);
        stompConnection.connect("system", "manager", clientId);
        return stompConnection;
    }

    protected void stompDisconnect(StompConnection connection) throws Exception {
        if (connection != null) {
            String receiptId = UUID.randomUUID().toString();
            connection.disconnect(receiptId);
            if (!connection.receive().getAction().equals(Stomp.Responses.RECEIPT)) {
                throw new Exception("Failed to receive receipt for disconnect.");
            }
            connection.close();
            connection = null;
        }
    }
}
