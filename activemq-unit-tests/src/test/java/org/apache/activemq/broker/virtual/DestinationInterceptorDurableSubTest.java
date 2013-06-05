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
package org.apache.activemq.broker.virtual;

import java.io.IOException;
import java.net.URI;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.EmbeddedBrokerTestSupport;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.xbean.XBeanBrokerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test for AMQ-4571.
 * checks that durable subscription is fully unregistered 
 * when using nested destination interceptors.
 */
public class DestinationInterceptorDurableSubTest extends EmbeddedBrokerTestSupport {

    private static final transient Logger LOG = LoggerFactory.getLogger(DestinationInterceptorDurableSubTest.class);
    private MBeanServerConnection mbsc = null;
    public static final String JMX_CONTEXT_BASE_NAME = "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName=";

    /**
     * Tests AMQ-4571.
     * @throws Exception
     */
    public void testVirtualTopicRemoval() throws Exception {

        LOG.debug("Running testVirtualTopicRemoval()");
        String clientId1 = "myId1";
        String clientId2 = "myId2";

        Connection conn = null;
        Session session = null;

        try {
            assertTrue(broker.isStarted());

            // create durable sub 1
            conn = createConnection();
            conn.setClientID(clientId1);
            conn.start();
            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Topic topic = session.createTopic(destination.getPhysicalName());
            TopicSubscriber sub1 = session.createDurableSubscriber((Topic) destination, clientId1);

            // create durable sub 2
            TopicSubscriber sub2 = session.createDurableSubscriber((Topic) destination, clientId2);

            // verify two subs registered in JMX 
            assertSubscriptionCount(destination.getPhysicalName(), 2);
            assertTrue(isSubRegisteredInJmx(destination.getPhysicalName(), clientId1));
            assertTrue(isSubRegisteredInJmx(destination.getPhysicalName(), clientId2));

            // delete sub 1
            sub1.close();
            session.unsubscribe(clientId1);

            // verify only one sub registered in JMX
            assertSubscriptionCount(destination.getPhysicalName(), 1);
            assertFalse(isSubRegisteredInJmx(destination.getPhysicalName(), clientId1));
            assertTrue(isSubRegisteredInJmx(destination.getPhysicalName(), clientId2));

            // delete sub 2
            sub2.close();
            session.unsubscribe(clientId2);

            // verify no sub registered in JMX
            assertSubscriptionCount(destination.getPhysicalName(), 0);
            assertFalse(isSubRegisteredInJmx(destination.getPhysicalName(), clientId1));
            assertFalse(isSubRegisteredInJmx(destination.getPhysicalName(), clientId2));
        } finally {
            session.close();
            conn.close();
        }
    }


    /**
     * Connects to broker using JMX
     * @return The JMX connection
     * @throws IOException in case of any errors
     */
    protected MBeanServerConnection connectJMXBroker() throws IOException {
        // connect to broker via JMX
        JMXServiceURL url =  new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:1299/jmxrmi");
        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
        LOG.debug("JMX connection established");
        return mbsc;
    }

    /**
     * Asserts that the Subscriptions JMX attribute of a topic has the expected
     * count. 
     * @param topicName name of the topic destination
     * @param expectedCount expected number of subscriptions
     * @return
     */
    protected boolean assertSubscriptionCount(String topicName, int expectedCount) {
        try {
            if (mbsc == null) {
                mbsc = connectJMXBroker();
            }
            // query broker queue size
            ObjectName[] tmp = (ObjectName[])mbsc.getAttribute(new ObjectName(JMX_CONTEXT_BASE_NAME + topicName), "Subscriptions");
            assertEquals(expectedCount, tmp.length);
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Checks if a subscriptions for topic topicName with subName is registered in JMX
     * 
     * @param topicName physical name of topic destination (excluding prefix 'topic://')
     * @param subName name of the durable subscription
     * @return true if registered, false otherwise
     */
    protected boolean isSubRegisteredInJmx(String topicName, String subName) {

        try {
            if (mbsc == null) {
                mbsc = connectJMXBroker();
            }

            // A durable sub is registered under the Subscriptions JMX attribute of the topic and 
            // as its own ObjectInstance under the topic's Consumer namespace.
            // AMQ-4571 only removed the latter not the former on unsubscribe(), so we need 
            // to check against both.
            ObjectName[] names = (ObjectName[])mbsc.getAttribute(new ObjectName(JMX_CONTEXT_BASE_NAME + topicName), "Subscriptions");
            ObjectInstance instance = (ObjectInstance)mbsc.getObjectInstance(
                new ObjectName(JMX_CONTEXT_BASE_NAME + 
                    topicName + 
                    ",endpoint=Consumer,clientId=myId1,consumerId=Durable(myId1_" + 
                    subName + 
                    ")")
            );

            if (instance == null) 
                return false;

            for (int i=0; i < names.length; i++) {
                if (names[i].toString().contains(subName))
                    return true;
            }
        } catch (InstanceNotFoundException ine) {
            //this may be expected so log at info level
            LOG.info(ine.toString());
            return false;
        }
        catch (Exception ex) {
            LOG.error(ex.toString());
            return false;
        }
        return false;
    }


    protected void tearDown() throws Exception {
        super.tearDown();
    }


    protected BrokerService createBroker() throws Exception {
        XBeanBrokerFactory factory = new XBeanBrokerFactory();
        BrokerService answer = factory.createBroker(new URI(getBrokerConfigUri()));

        // lets disable persistence as we are a test
        answer.setPersistent(false);
        useTopic = true;
        return answer;
    }


    protected String getBrokerConfigUri() {
        return "org/apache/activemq/broker/virtual/virtual-topics-and-interceptor.xml";
    }


    /**
     * Simple but custom topic interceptor.
     * To be used for testing nested interceptors in conjunction with 
     * virtual topic interceptor.
     */
    public static class SimpleDestinationInterceptor implements DestinationInterceptor {

        private final Logger LOG = LoggerFactory.getLogger(SimpleDestinationInterceptor.class);
        private BrokerService broker;

        public SimpleDestinationInterceptor() {
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.BrokerServiceAware#setBrokerService(org.apache.activemq.broker.BrokerService)
         */
        public void setBrokerService(BrokerService brokerService) {
            LOG.info("setBrokerService()");
            this.broker = brokerService;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.region.DestinationInterceptor#intercept(org.apache.activemq.broker.region.Destination)
         */
        public Destination intercept(final Destination destination) {
            LOG.info("intercept({})", destination.getName());

            if (!destination.getActiveMQDestination().getPhysicalName().startsWith("ActiveMQ")) {
                return new DestinationFilter(destination) {
                  public void send(ProducerBrokerExchange context, Message message) throws Exception {
                    // Send message to Destination
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("SimpleDestinationInterceptor: Sending message to destination:"
                          + this.getActiveMQDestination().getPhysicalName());
                    }
                    // message.setDestination(destination.getActiveMQDestination());
                    super.send(context, message);
                  }
                };
              }
              return destination;
        }


        /* (non-Javadoc)
         * @see org.apache.activemq.broker.region.DestinationInterceptor#remove(org.apache.activemq.broker.region.Destination)
         */
        public void remove(Destination destination) {
            LOG.info("remove({})", destination.getName());
            this.broker = null;
        }


        /* (non-Javadoc)
         * @see org.apache.activemq.broker.region.DestinationInterceptor#create(org.apache.activemq.broker.Broker, org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.ActiveMQDestination)
         */
        public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
            LOG.info("create("+ broker.getBrokerName() + ", " + context.toString() + ", " + destination.getPhysicalName());
        }
    }
}
