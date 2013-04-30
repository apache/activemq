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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;

public class MemoryUsageBrokerTest extends BrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryUsageBrokerTest.class);

    protected void setUp() throws Exception {
        this.setAutoFail(true);
        super.setUp();
    }

    @Override
    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policy = super.getDefaultPolicy();
        // Disable PFC and assign a large memory limit that's larger than the default broker memory limit for queues
        policy.setProducerFlowControl(false);
        policy.setQueue(">");
        policy.setMemoryLimit(128 * 1024 * 1024);
        return policy;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBStore kaha = new KahaDBStore();
        File directory = new File("target/activemq-data/kahadb");
        IOHelper.deleteChildren(directory);
        kaha.setDirectory(directory);
        kaha.deleteAllMessages();
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    protected ConnectionFactory createConnectionFactory() {
        return new ActiveMQConnectionFactory(broker.getVmConnectorURI());
    }

    protected Connection createJmsConnection() throws JMSException {
        return createConnectionFactory().createConnection();
    }

    public void testMemoryUsage() throws Exception {
        Connection conn = createJmsConnection();
        Session session = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue.a.b");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < 100000; i++) {
            BytesMessage bm = session.createBytesMessage();
            bm.writeBytes(new byte[1024]);
            producer.send(bm);
            if ((i + 1) % 100 == 0) {
                session.commit();
                int memoryUsagePercent = broker.getSystemUsage().getMemoryUsage().getPercentUsage();
                LOG.info((i + 1) + " messages have been sent; broker memory usage " + memoryUsagePercent + "%");
                assertTrue("Used more than available broker memory", memoryUsagePercent <= 100);
            }
        }
        session.commit();
        producer.close();
        session.close();
        conn.close();
    }

}