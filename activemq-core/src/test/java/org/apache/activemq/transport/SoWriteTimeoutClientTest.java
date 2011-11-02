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
package org.apache.activemq.transport;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SoWriteTimeoutClientTest extends JmsTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(SoWriteTimeoutClientTest.class);

    public String brokerTransportScheme = "tcp";

    protected BrokerService createBroker() throws Exception {
        BrokerService broker =  new BrokerService();
        broker.addConnector(brokerTransportScheme + "://localhost:0?wireFormat.maxInactivityDuration=0");
        return broker;
    }

    public void x_testSendWithClientWriteTimeout() throws Exception {
        final ActiveMQQueue dest = new ActiveMQQueue("testClientWriteTimeout");
        messageTextPrefix = initMessagePrefix(80*1024);

        URI tcpBrokerUri = URISupport.removeQuery(broker.getTransportConnectors().get(0).getConnectUri());
        LOG.info("consuming using uri: " + tcpBrokerUri);


         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();
        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(dest);

        SocketProxy proxy = new SocketProxy();
        proxy.setTarget(tcpBrokerUri);
        proxy.open();

        ActiveMQConnectionFactory pFactory = new ActiveMQConnectionFactory("failover:(" + proxy.getUrl() + "?soWriteTimeout=500)?jms.useAsyncSend=true");
        final Connection pc = pFactory.createConnection();
        pc.start();
        proxy.pause();

        final int messageCount = 20;
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try{
                    sendMessages(pc, dest, messageCount);
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        });

        // wait for timeout and reconnect
        TimeUnit.SECONDS.sleep(20);
        proxy.goOn();
        for (int i=0; i<messageCount; i++) {
            assertNotNull("Got message " + i  + " after reconnect", consumer.receive(5000));
        }
    }

    private String initMessagePrefix(int i) {
        byte[] content = new byte[i];
        return new String(content);
    }

    public static Test suite() {
        return suite(SoWriteTimeoutClientTest.class);
    }
}