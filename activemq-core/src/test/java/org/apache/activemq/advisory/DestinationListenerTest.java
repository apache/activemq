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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public class DestinationListenerTest extends EmbeddedBrokerTestSupport implements DestinationListener {
    private static final Log LOG = LogFactory.getLog(DestinationListenerTest.class);
    protected ActiveMQConnection connection;
    protected DestinationSource destinationSource;

    public void testDestiationSource() throws Exception {
        Thread.sleep(1000);
        System.out.println("Queues: " + destinationSource.getQueues());
        System.out.println("Topics: " + destinationSource.getTopics());
    }

    public void onDestinationEvent(DestinationEvent event) {
        ActiveMQDestination destination = event.getDestination();
        if (event.isAddOperation()) {
            System.out.println("Added:   " + destination);
        }
        else {
            System.out.println("Removed: " + destination);
        }
    }

    protected void setUp() throws Exception {
        super.setUp();

        connection = (ActiveMQConnection) createConnection();
        connection.start();

        destinationSource = connection.getDestinationSource();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setDestinations(new ActiveMQDestination[]{
                new ActiveMQQueue("foo.bar"),
                new ActiveMQTopic("cheese")
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