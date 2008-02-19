/*
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
package org.apache.activemq;

import java.io.InputStream;
import java.io.OutputStream;

import javax.jms.Queue;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ActiveMQInputStreamTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(ActiveMQInputStreamTest.class);

    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String DESTINATION = "destination";
    private static final int STREAM_LENGTH = 64 * 1024 + 0; // change 0 to 1 to make it not crash

    public void testInputStreamMatchesDefaultChuckSize() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(false);
        broker.setDestinations(new ActiveMQDestination[] {
            ActiveMQDestination.createDestination(DESTINATION, ActiveMQDestination.QUEUE_TYPE),
        });
        broker.addConnector(BROKER_URL);
        broker.start();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(DESTINATION);

        OutputStream out = null;
        try {
            out = connection.createOutputStream(destination);
            LOG.debug("writing...");
            for (int i = 0; i < STREAM_LENGTH; ++i) {
                out.write(0);
            }
            LOG.debug("wrote " + STREAM_LENGTH + " bytes");
        } finally {
            if (out != null) {
                out.close();
            }
        }

        InputStream in = null;
        try {
            in = connection.createInputStream(destination);
            LOG.debug("reading...");
            int count = 0;
            while (-1 != in.read()) {
                ++count;
            }
            LOG.debug("read " + count + " bytes");
        } finally {
            if (in != null) {
                in.close();
            }
        }

        connection.close();
        broker.stop();
    }
}
