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
package org.apache.activemq.store.kahadb;

import java.util.Arrays;
import java.util.Collection;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQ-5875
 *
 * This test shows that when multiple destinations share a single KahaDB
 * instance when using mKahaDB, that the deletion of one Queue will not cause
 * the store to be deleted if another destination is still attached.  This
 * issue was related to Topics but this test makes sure Queues work as well.
 *
 * */
@RunWith(Parameterized.class)
public class MultiKahaDBQueueDeletionTest extends AbstractMultiKahaDBDeletionTest {

    protected static final Logger LOG = LoggerFactory
            .getLogger(MultiKahaDBTopicDeletionTest.class);

    protected static ActiveMQQueue QUEUE1 = new ActiveMQQueue("test.>");
    protected static ActiveMQQueue QUEUE2 = new ActiveMQQueue("test.t.queue");

    @Parameters
    public static Collection<Object[]> data() {

        //Test with queues created in different orders
        return Arrays.asList(new Object[][] {
                {QUEUE1, QUEUE2},
                {QUEUE2, QUEUE1}
        });
    }

    public MultiKahaDBQueueDeletionTest(ActiveMQQueue dest1, ActiveMQQueue dest2) {
        super(dest1, dest2);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.kahadb.AbstractMultiKahaDBDeletionTest#createConsumer(org.apache.activemq.command.ActiveMQDestination)
     */
    @Override
    protected void createConsumer(ActiveMQDestination dest) throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                brokerConnectURI);
        Connection connection = factory.createConnection();
        connection.setClientID("client1");
        connection.start();
        Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        session.createConsumer(dest);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.kahadb.AbstractMultiKahaDBDeletionTest#getStoreFileFilter()
     */
    @Override
    protected WildcardFileFilter getStoreFileFilter() {
        return new WildcardFileFilter("queue*");
    }

}
