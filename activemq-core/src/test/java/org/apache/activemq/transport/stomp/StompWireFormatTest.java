/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.*;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class StompWireFormatTest extends TestCase {
    
    protected static final Log log = LogFactory.getLog(StompWireFormatTest.class);

    private StompWireFormat wire;

    public void setUp() throws Exception {
        wire = new StompWireFormat();
    }

    public void testValidConnectHandshake() throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        ConnectionInfo ci = (ConnectionInfo) parseCommand("CONNECT\n" + "login: brianm\n" + "passcode: wombats\n" + "\n" + Stomp.NULL);
        assertNotNull(ci);
        assertTrue(ci.isResponseRequired());

        Response cr = new Response();
        cr.setCorrelationId(ci.getCommandId());

        String response = writeCommand(cr);
        log.info("Received: " + response);

        SessionInfo si = (SessionInfo) wire.readCommand(null);
        assertNotNull(si);
        assertTrue(!si.isResponseRequired());

        ProducerInfo pi = (ProducerInfo) wire.readCommand(null);
        assertNotNull(pi);
        assertTrue(pi.isResponseRequired());

        Response sr = new Response();
        sr.setCorrelationId(pi.getCommandId());
        response = writeCommand(sr);
        log.info("Received: " + response);
        assertTrue("Response should start with CONNECTED: " + response, response.startsWith("CONNECTED"));

        // now lets test subscribe
        ConsumerInfo consumerInfo = (ConsumerInfo) parseCommand("SUBSCRIBE\n" + "destination: /queue/foo\n" + "ack: client\n" + "activemq.prefetchSize: 1\n"
                + "\n" + Stomp.NULL);
        assertNotNull(consumerInfo);
        // assertTrue(consumerInfo.isResponseRequired());
        assertEquals("prefetch size", 1, consumerInfo.getPrefetchSize());

        cr = new Response();
        cr.setCorrelationId(consumerInfo.getCommandId());
        response = writeCommand(cr);
        log.info("Received: " + response);
    }

    public void _testFakeServer() throws Exception {
        final BrokerService container = new BrokerService();
        new Thread(new Runnable() {
            public void run() {
                try {
                    container.addConnector("stomp://localhost:61613");
                    container.start();
                }
                catch (Exception e) {
                    System.err.println("ARGH: caught: " + e);
                    e.printStackTrace();
                }
            }
        }).start();
        System.err.println("started container");
        System.err.println("okay, go play");

        System.err.println(System.in.read());
    }

    protected Command parseCommand(String connect_frame) throws IOException, JMSException {
        DataInputStream din = new DataInputStream(new ByteArrayInputStream(connect_frame.getBytes()));

        return wire.readCommand(din);
    }

    protected String writeCommand(Command command) throws IOException, JMSException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        wire.writeCommand(command, dout);
        return new String(bout.toByteArray());
    }

}
