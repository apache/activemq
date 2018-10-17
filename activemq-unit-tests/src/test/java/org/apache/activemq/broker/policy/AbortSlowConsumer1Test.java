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
package org.apache.activemq.broker.policy;

import org.apache.activemq.util.MessageIdList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class AbortSlowConsumer1Test extends AbortSlowConsumerBase {

    private static final Logger LOG = LoggerFactory.getLogger(AbortSlowConsumer1Test.class);

    @Parameterized.Parameters(name = "abortConnection({0})-isTopic({1})")
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {Boolean.TRUE, Boolean.TRUE},
                {Boolean.FALSE, Boolean.TRUE}});
    }

    public AbortSlowConsumer1Test(Boolean abortConnection, Boolean topic) {
        this.abortConnection = abortConnection;
        this.topic = topic;
    }

    @Test(timeout = 60 * 1000)
    public void testSlowConsumerIsAborted() throws Exception {
        startConsumers(destination);
        Entry<MessageConsumer, MessageIdList> consumertoAbort = consumers.entrySet().iterator().next();
        consumertoAbort.getValue().setProcessingDelay(8 * 1000);
        for (Connection c : connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 100);

        consumertoAbort.getValue().assertMessagesReceived(1);
        TimeUnit.SECONDS.sleep(5);
        consumertoAbort.getValue().assertAtMostMessagesReceived(1);
    }

    @Test(timeout = 60 * 1000)
    public void testAbortAlreadyClosedConsumers() throws Exception {
        Connection conn = createConnectionFactory().createConnection();
        conn.setExceptionListener(this);
        connections.add(conn);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(destination);
        conn.start();
        startProducers(destination, 20);
        TimeUnit.SECONDS.sleep(1);
        LOG.info("closing consumer: " + consumer);
        consumer.close();

        TimeUnit.SECONDS.sleep(5);
        assertTrue("no exceptions : " + exceptions.toArray(), exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testAbortAlreadyClosedConnection() throws Exception {
        Connection conn = createConnectionFactory().createConnection();
        conn.setExceptionListener(this);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        sess.createConsumer(destination);
        conn.start();
        startProducers(destination, 20);
        TimeUnit.SECONDS.sleep(1);
        LOG.info("closing connection: " + conn);
        conn.close();

        TimeUnit.SECONDS.sleep(5);
        assertTrue("no exceptions : " + exceptions.toArray(), exceptions.isEmpty());
    }
}
