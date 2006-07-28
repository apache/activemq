/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.usecases;

import junit.framework.TestCase;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.broker.BrokerContainer;
import org.activemq.broker.impl.BrokerContainerImpl;
import org.activemq.store.vm.VMPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author bbeck
 * @version $Revision: 1.1 $
 */
public class BecksNetworkTest extends TestCase {
    private static final Log log = LogFactory.getLog(BecksNetworkTest.class);

    private static final int NUM_BROKERS = 10;
    private static final int NUM_PRODUCERS = 10;
    private static final int NUM_CONSUMERS = 1;
    private static final int NUM_MESSAGES = 1;
    private static final long MESSAGE_SEND_DELAY = 100;
    private static final long MESSAGE_RECEIVE_DELAY = 50;
    private static final int BASE_PORT = 9500;
    private static final String QUEUE_NAME = "QUEUE";
    private static final String MESSAGE_PRODUCER_KEY = "PRODUCER";
    private static final String MESSAGE_BODY_KEY = "BODY";

    public void testCase() throws Throwable {
        main(new String[]{});
    }

    public static void main(String[] args) throws Throwable {
        String[] addresses = new String[NUM_BROKERS];
        for (int i = 0; i < NUM_BROKERS; i++) {
            addresses[i] = "tcp://localhost:" + (BASE_PORT + i);
        }

        log.info("Starting brokers");
        BrokerContainer[] brokers = startBrokers(addresses);
        String reliableURL = createAddressString(addresses, "reliable:", null);

        log.info("Creating simulation state");
        final SimulationState state = new SimulationState(NUM_PRODUCERS * NUM_MESSAGES);
        Thread stateWatcher = new Thread("Simulation State Watcher Thread") {
            public void run() {
                while (state.getState() != SimulationState.FINISHED) {
                    log.info("State: " + state);

                    synchronized (this) {
                        try {
                            wait(5000);
                        }
                        catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        };
        stateWatcher.setDaemon(true);
        stateWatcher.start();

        log.info("Starting components");
        MessageProducerComponent[] producers = new MessageProducerComponent[NUM_PRODUCERS];
        MessageConsumerComponent[] consumers = new MessageConsumerComponent[NUM_CONSUMERS];
        {
            for (int i = 0; i < NUM_PRODUCERS; i++) {
                producers[i] = new MessageProducerComponent(state, "MessageProducer[" + i + "]", reliableURL, NUM_MESSAGES);
                producers[i].start();
            }

            for (int i = 0; i < NUM_CONSUMERS; i++) {
                consumers[i] = new MessageConsumerComponent(state, "MessageConsumer[" + i + "]", reliableURL);
                consumers[i].start();
            }
        }

        // Start the simulation
        log.info("##### Starting the simulation...");
        state.setState(SimulationState.RUNNING);

        for (int i = 0; i < producers.length; i++) {
            producers[i].join();
        }
        log.info("Producers finished");

        for (int i = 0; i < consumers.length; i++) {
            consumers[i].join();
            log.info(consumers[i].getId() + " consumed " + consumers[i].getNumberOfMessagesConsumed() + " messages.");
        }

        log.info("Consumers finished");
        log.info("State: " + state);

        state.waitForSimulationState(SimulationState.FINISHED);

        log.info("Stopping brokers");
        for (int i = 0; i < brokers.length; i++) {
            brokers[i].stop();
        }
    }

    private static BrokerContainer[] startBrokers(String[] addresses) throws JMSException {
        BrokerContainer[] containers = new BrokerContainer[addresses.length];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = new BrokerContainerImpl(Integer.toString(i));
            containers[i].setPersistenceAdapter(new VMPersistenceAdapter());
            containers[i].addConnector(addresses[i]);

            for (int j = 0; j < addresses.length; j++) {
                if (i == j) {
                    continue;
                }

                containers[i].addNetworkConnector("reliable:" + addresses[j]);
            }

            containers[i].start();

            log.debug("Created broker on " + addresses[i]);
        }

        // Delay so this broker has a chance to come up fully...
        try {
            Thread.sleep(2000 * containers.length);
        }
        catch (InterruptedException ignored) {
        }

        return containers;
    }

    /*
    private static BrokerContainer[] startBrokers(String[] addresses) throws JMSException, IOException
    {
    for(int i = 0; i < addresses.length; i++) {
                    File temp = File.createTempFile("broker_" + i + "_", ".xml");
                    temp.deleteOnExit();


                    PrintWriter fout = new PrintWriter(new FileWriter(temp));
                    fout.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    fout.println("<!DOCTYPE beans PUBLIC  \"-//ACTIVEMQ//DTD//EN\" \"http://activemq.codehaus.org/dtd/activemq.dtd\">");
                    fout.println("<beans>");
                    fout.println("  <broker name=\"" + "receiver" + i + "\">");
                    fout.println("          <connector>");
                    fout.println("                  <tcpServerTransport uri=\"" + addresses[i] + "\"/>");
                    fout.println("          </connector>");

                    if(addresses.length > 1) {
                            String otherAddresses = createAddressString(addresses, "list:", addresses[i]);
                            otherAddresses = "tcp://localhost:9000";

            fout.println("          <networkConnector>");
            fout.println("                  <networkChannel uri=\"" + otherAddresses + "\"/>");
            fout.println("          </networkConnector>");
                    }

                    fout.println("          <persistence>");
                    fout.println("                  <vmPersistence/>");
                    fout.println("          </persistence>");
                    fout.println("  </broker>");
                    fout.println("</beans>");
                    fout.close();

                    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://" + i);
                    factory.setUseEmbeddedBroker(true);
                    factory.setBrokerXmlConfig("file:" + temp.getAbsolutePath());
                    factory.setBrokerName("broker-" + addresses[i]);
                    factory.start();

                    Connection c = factory.createConnection();
                    c.start();
    }

            // Delay so this broker has a chance to come up fully...
            try { Thread.sleep(2000*addresses.length); }
            catch(InterruptedException ignored) {}

            return null;
    }
    */

    private static String createAddressString(String[] addresses, String prefix, String addressToSkip) {
        StringBuffer sb = new StringBuffer(prefix);
        boolean first = true;

        for (int i = 0; i < addresses.length; i++) {
            if (addressToSkip != null && addressToSkip.equals(addresses[i])) {
                continue;
            }

            if (!first) {
                sb.append(',');
            }

            sb.append(addresses[i]);
            first = false;
        }

        return sb.toString();
    }

    private static final class SimulationState {
        public static final int INITIALIZED = 1;
        public static final int RUNNING = 2;
        public static final int FINISHED = 3;

        private final Object m_stateLock;
        private int m_state;
        private final int m_numExpectedMessages;
        private final Set m_messagesProduced;
        private final Set m_messagesConsumed;

        public SimulationState(int numMessages) {
            m_stateLock = new Object();
            synchronized (m_stateLock) {
                m_state = INITIALIZED;
                m_numExpectedMessages = numMessages;
                m_messagesProduced = new HashSet();
                m_messagesConsumed = new HashSet();
            }
        }

        public int getState() {
            synchronized (m_stateLock) {
                return m_state;
            }
        }

        public void setState(int newState) {
            synchronized (m_stateLock) {
                m_state = newState;
                m_stateLock.notifyAll();
            }
        }

        public void waitForSimulationState(int state) throws InterruptedException {
            synchronized (m_stateLock) {
                while (m_state != state) {
                    m_stateLock.wait();
                }
            }
        }

        public void onMessageProduced(String producerId, String messageBody) {
            log.debug("-> onMessageProduced(" + producerId + ", " + messageBody + ")");

            synchronized (m_stateLock) {
                if (m_state == INITIALIZED) {
                    throw new RuntimeException("Message produced before the simulation has begun: messageBody=" + messageBody);
                }

                if (m_state == FINISHED) {
                    throw new RuntimeException("Message produced after the simulation has finished: messageBody=" + messageBody);
                }

                if (!m_messagesProduced.add(messageBody)) {
                    throw new RuntimeException("Duplicate message produced: messageBody=" + messageBody);
                }
            }
        }

        public void onMessageConsumed(String consumerId, String messageBody) {
            log.debug("<- onMessageConsumed(" + consumerId + ", " + messageBody + ")");

            synchronized (m_stateLock) {
                if (m_state != RUNNING) {
                    throw new RuntimeException("Message consumed while the simulation wasn't running: state = " + m_state + ", messageBody=" + messageBody);
                }

                if (!m_messagesProduced.contains(messageBody)) {
                    throw new RuntimeException("Message consumed that wasn't produced: messageBody=" + messageBody);
                }

                if (!m_messagesConsumed.add(messageBody)) {
                    throw new RuntimeException("Message consumed more than once: messageBody=" + messageBody);
                }

                if (m_messagesConsumed.size() == m_numExpectedMessages) {
                    setState(FINISHED);
                    log.info("All expected messages have been consumed, finishing simulation.");
                }
            }
        }

        public String toString() {
            synchronized (m_stateLock) {
                SortedMap unconsumed = new TreeMap();
                for (Iterator iter = m_messagesProduced.iterator(); iter.hasNext();) {
                    String message = (String) iter.next();
                    int colonIndex = message.indexOf(':');
                    String producerId = message.substring(0, colonIndex);

                    Integer numMessages = (Integer) unconsumed.get(producerId);
                    numMessages = (numMessages == null) ? new Integer(1) : new Integer(numMessages.intValue() + 1);
                    unconsumed.put(producerId, numMessages);
                }

                for (Iterator iter = m_messagesConsumed.iterator(); iter.hasNext();) {
                    String message = (String) iter.next();
                    int colonIndex = message.indexOf(':');
                    String producerId = message.substring(0, colonIndex);

                    Integer numMessages = (Integer) unconsumed.get(producerId);
                    numMessages = (numMessages == null) ? new Integer(-1) : new Integer(numMessages.intValue() - 1);
                    if (numMessages.intValue() == 0) {
                        unconsumed.remove(producerId);
                    }
                    else {
                        unconsumed.put(producerId, numMessages);
                    }
                }


                return "SimulationState["
                        + "state=" + m_state + " "
                        + "numExpectedMessages=" + m_numExpectedMessages + " "
                        + "numMessagesProduced=" + m_messagesProduced.size() + " "
                        + "numMessagesConsumed=" + m_messagesConsumed.size() + " "
                        + "unconsumed=" + unconsumed;
            }
        }
    }

    private static abstract class SimulationComponent extends Thread {
        protected final SimulationState m_simulationState;
        protected final String m_id;

        protected abstract void _initialize() throws Throwable;

        protected abstract void _run() throws Throwable;

        protected abstract void _cleanup() throws Throwable;

        public SimulationComponent(SimulationState state, String id) {
            super(id);

            m_simulationState = state;
            m_id = id;
        }

        public String getId() {
            return m_id;
        }

        public final void run() {
            try {
                try {
                    _initialize();
                }
                catch (Throwable t) {
                    log.error("Error during initialization", t);
                    return;
                }

                try {
                    if (m_simulationState.getState() == SimulationState.FINISHED) {
                        log.info(m_id + " : NO NEED TO WAIT FOR RUNNING - already FINISHED");
                    }
                    else {
                        log.info(m_id + ": WAITING for RUNNING started");
                        m_simulationState.waitForSimulationState(SimulationState.RUNNING);
                        log.info(m_id + ": WAITING for RUNNING finished");
                    }
                }
                catch (InterruptedException e) {
                    log.error("Interrupted during wait for the simulation to begin", e);
                    return;
                }

                try {
                    _run();
                }
                catch (Throwable t) {
                    log.error("Error during running", t);
                }
            }
            finally {
                try {
                    _cleanup();
                }
                catch (Throwable t) {
                    log.error("Error during cleanup", t);
                }
            }

        }
    }

    private static abstract class JMSComponent extends SimulationComponent {
        protected final String m_url;
        protected Connection m_connection;
        protected Session m_session;
        protected Queue m_queue;

        public JMSComponent(SimulationState state, String id, String url) {
            super(state, id);

            m_url = url;
        }

        protected void _initialize() throws JMSException {
            m_connection = new ActiveMQConnectionFactory(m_url).createConnection();
            m_connection.start();

            m_session = m_connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            m_queue = m_session.createQueue(QUEUE_NAME);
        }

        protected void _cleanup() throws JMSException {
            if (m_session != null) {
                m_session.close();
            }

            if (m_connection != null) {
                m_connection.close();
            }
        }
    }

    private static final class MessageProducerComponent extends JMSComponent {
        private final int m_numMessagesToSend;
        private MessageProducer m_producer;

        public MessageProducerComponent(SimulationState state, String id, String url, int numMessages) {
            super(state, id, url);

            m_numMessagesToSend = numMessages;
        }

        protected void _initialize() throws JMSException {
            super._initialize();

            m_producer = m_session.createProducer(m_queue);
            m_producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        }

        protected void _cleanup() throws JMSException {
            if (m_producer != null) {
                m_producer.close();
            }

            super._cleanup();
        }

        public void _run() throws JMSException, InterruptedException {
            log.debug(m_id + ": started");
            for (int num = 0; num < m_numMessagesToSend; num++) {
                String messageBody = createMessageBody(m_id, num);

                MapMessage message = m_session.createMapMessage();
                message.setString(MESSAGE_PRODUCER_KEY, m_id);
                message.setString(MESSAGE_BODY_KEY, messageBody);

                // Pretend to be doing some work....
                Thread.sleep(MESSAGE_SEND_DELAY);

                m_simulationState.onMessageProduced(m_id, messageBody);
                m_producer.send(message);
            }
        }

        private static String createMessageBody(String id, int num) {
            return id + ":" + Integer.toString(num);
        }
    }

    private static final class MessageConsumerComponent extends JMSComponent implements MessageListener {
        private final Object m_stateLock;
        private boolean m_inOnMessage;

        private MessageConsumer m_consumer;
        private int m_numMessagesConsumed;

        public MessageConsumerComponent(SimulationState state, String id, String url) {
            super(state, id, url);
            m_stateLock = new Object();
            m_inOnMessage = false;
        }

        protected void _initialize() throws JMSException {
            super._initialize();

            m_consumer = m_session.createConsumer(m_queue);
            m_consumer.setMessageListener(this);

            m_numMessagesConsumed = 0;
        }

        protected void _cleanup() throws JMSException {
            if (m_consumer != null) {
                m_consumer.close();
            }

            super._cleanup();
        }

        public void _run() throws InterruptedException {
            log.info(m_id + ": WAITING for FINISHED started");
            m_simulationState.waitForSimulationState(SimulationState.FINISHED);
            log.info(m_id + ": WAITING for FINISHED finished");
        }

        public int getNumberOfMessagesConsumed() {
            return m_numMessagesConsumed;
        }

        public void onMessage(Message msg) {
            synchronized (m_stateLock) {
                if (m_inOnMessage) {
                    log.error("Already in onMessage!!!");
                }

                m_inOnMessage = true;
            }

            try {
                MapMessage message = (MapMessage) msg;
                String messageBody = message.getString(MESSAGE_BODY_KEY);

                m_simulationState.onMessageConsumed(m_id, messageBody);
                m_numMessagesConsumed++;

                // Pretend to be doing some work....
                Thread.sleep(MESSAGE_RECEIVE_DELAY);
            }
            catch (Throwable t) {
                log.error("Unexpected error during onMessage: message=" + msg, t);
            }
            finally {
                synchronized (m_stateLock) {
                    if (!m_inOnMessage) {
                        log.error("Not already in onMessage!!!");
                    }

                    m_inOnMessage = false;
                }
            }
        }
    }
}
