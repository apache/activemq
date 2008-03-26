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
package org.apache.activemq.advisory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @version $Revision$
 */
public class DestinationListenerTest extends EmbeddedBrokerTestSupport implements DestinationListener {
    private static final transient Log LOG = LogFactory.getLog(DestinationListenerTest.class);
    protected ActiveMQConnection connection;
    protected ActiveMQQueue sampleQueue = new ActiveMQQueue("foo.bar");
    protected ActiveMQTopic sampleTopic = new ActiveMQTopic("cheese");
    protected List<ActiveMQDestination> newDestinations = new ArrayList<ActiveMQDestination>();

    public void testDestiationSourceHasInitialDestinations() throws Exception {
        Thread.sleep(1000);

        DestinationSource destinationSource = connection.getDestinationSource();
        Set<ActiveMQQueue> queues = destinationSource.getQueues();
        Set<ActiveMQTopic> topics = destinationSource.getTopics();

        LOG.info("Queues: " + queues);
        LOG.info("Topics: " + topics);

        assertTrue("The queues should not be empty!", !queues.isEmpty());
        assertTrue("The topics should not be empty!", !topics.isEmpty());

        assertTrue("queues contains initial queue: " + queues, queues.contains(sampleQueue));
        assertTrue("topics contains initial topic: " + queues, topics.contains(sampleTopic));
    }

    public void testConsumerForcesNotificationOfNewDestination() throws Exception {
        // now lets cause a destination to be created
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue newQueue = new ActiveMQQueue("Test.Cheese");
        session.createConsumer(newQueue);

        Thread.sleep(3000);

        assertThat(newQueue, isIn(newDestinations));

        LOG.info("New destinations are: " + newDestinations);
    }

    public void testProducerForcesNotificationOfNewDestination() throws Exception {
        // now lets cause a destination to be created
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQQueue newQueue = new ActiveMQQueue("Test.Beer");
        MessageProducer producer = session.createProducer(newQueue);
        TextMessage message = session.createTextMessage("<hello>world</hello>");
        producer.send(message);

        Thread.sleep(3000);

        assertThat(newQueue, isIn(newDestinations));

        LOG.info("New destinations are: " + newDestinations);
    }

    public void onDestinationEvent(DestinationEvent event) {
        ActiveMQDestination destination = event.getDestination();
        if (event.isAddOperation()) {
            LOG.info("Added:   " + destination);
            newDestinations.add(destination);
        }
        else {
            LOG.info("Removed: " + destination);
            newDestinations.remove(destination);
        }
    }

    protected void setUp() throws Exception {
        super.setUp();

        connection = (ActiveMQConnection) createConnection();
        connection.start();
        connection.getDestinationSource().setDestinationListener(this);
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setDestinations(new ActiveMQDestination[]{
                sampleQueue,
                sampleTopic
        });
        return broker;
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }
}