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

package org.apache.activemq.usecases;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class RequestReplyToTopicViaThreeNetworkHopsTest {
    protected static final int CONCURRENT_CLIENT_COUNT = 5;
    protected static final int CONCURRENT_SERVER_COUNT = 5;
    protected static final int TOTAL_CLIENT_ITER = 10;

    protected static int Next_broker_num = 0;
    protected EmbeddedTcpBroker edge1;
    protected EmbeddedTcpBroker edge2;
    protected EmbeddedTcpBroker core1;
    protected EmbeddedTcpBroker core2;

    protected boolean testError = false;
    protected boolean fatalTestError = false;

    protected int echoResponseFill = 0; // Number of "filler" response messages per request

    protected static Log LOG;
    public boolean duplex = true;

    static {
        LOG = LogFactory.getLog(RequestReplyToTopicViaThreeNetworkHopsTest.class);
    }

    public RequestReplyToTopicViaThreeNetworkHopsTest() throws Exception {
        edge1 = new EmbeddedTcpBroker("edge", 1);
        edge2 = new EmbeddedTcpBroker("edge", 2);
        core1 = new EmbeddedTcpBroker("core", 1);
        core2 = new EmbeddedTcpBroker("core", 2);

        // duplex is necessary to serialise sends with consumer/destination creation
        edge1.coreConnectTo(core1, duplex);
        edge2.coreConnectTo(core2, duplex);
        core1.coreConnectTo(core2, duplex);
    }

    public void logMessage(String msg) {
        System.out.println(msg);
        System.out.flush();
    }

    public void testMessages(Session sess, MessageProducer req_prod, Destination resp_dest, int num_msg) throws Exception {
        MessageConsumer resp_cons;
        TextMessage msg;
        MessageClient cons_client;
        int cur;
        int tot_expected;

        resp_cons = sess.createConsumer(resp_dest);

        cons_client = new MessageClient(resp_cons, num_msg);
        cons_client.start();

        cur = 0;
        while ((cur < num_msg) && (!fatalTestError)) {
            msg = sess.createTextMessage("MSG AAAA " + cur);
            msg.setIntProperty("SEQ", 100 + cur);
            msg.setStringProperty("TEST", "TOPO");
            msg.setJMSReplyTo(resp_dest);

            if (cur == (num_msg - 1))
                msg.setBooleanProperty("end-of-response", true);

            sendWithRetryOnDeletedDest(req_prod, msg);
            LOG.debug("Sent:" + msg);

            cur++;
        }

        //
        // Give the consumer some time to receive the response.
        //
        cons_client.waitShutdown(5000);

        //
        // Now shutdown the consumer if it's still running.
        //
        if (cons_client.shutdown())
            LOG.debug("Consumer client shutdown complete");
        else
            LOG.debug("Consumer client shutdown incomplete!!!");

        //
        // Check that the correct number of messages was received.
        //
        tot_expected = num_msg * (echoResponseFill + 1);

        if (cons_client.getNumMsgReceived() == tot_expected) {
            LOG.debug("Have " + tot_expected + " messages, as-expected");
        } else {
            testError = true;

            if (cons_client.getNumMsgReceived() == 0)
                fatalTestError = true;

            LOG.error("Have " + cons_client.getNumMsgReceived() + " messages; expected " + tot_expected + " on destination " + resp_dest);
        }

        resp_cons.close();
    }

    protected void sendWithRetryOnDeletedDest(MessageProducer prod, Message msg) throws JMSException {
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("SENDING REQUEST message " + msg);

            prod.send(msg);
        } catch (JMSException jms_exc) {
            System.out.println("AAA: " + jms_exc.getMessage());
            throw jms_exc;
        }
    }

    /**
     * Test one destination between the given "producer broker" and "consumer broker" specified.
     */
    public void testOneDest(Connection conn, Session sess, Destination cons_dest, int num_msg) throws Exception {
        Destination prod_dest;
        MessageProducer msg_prod;

        //
        // Create the Producer to the echo request Queue
        //
        LOG.trace("Creating echo queue and producer");
        prod_dest = sess.createQueue("echo");
        msg_prod = sess.createProducer(prod_dest);

        //
        // Pass messages around.
        //
        testMessages(sess, msg_prod, cons_dest, num_msg);

        msg_prod.close();
    }

    /**
     * TEST TEMPORARY TOPICS
     */
    public void testTempTopic(String prod_broker_url, String cons_broker_url) throws Exception {
        Connection conn;
        Session sess;
        Destination cons_dest;
        int num_msg;

        num_msg = 5;

        LOG.debug("TESTING TEMP TOPICS " + prod_broker_url + " -> " + cons_broker_url + " (" + num_msg + " messages)");

        //
        // Connect to the bus.
        //
        conn = createConnection(cons_broker_url);
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //
        // Create the destination on which messages are being tested.
        //
        LOG.trace("Creating destination");
        cons_dest = sess.createTemporaryTopic();

        testOneDest(conn, sess, cons_dest, num_msg);

        //
        // Cleanup
        //
        sess.close();
        conn.close();
    }

    /**
     * TEST TOPICS
     */
    public void testTopic(String prod_broker_url, String cons_broker_url) throws Exception {
        int num_msg;

        Connection conn;
        Session sess;
        String topic_name;

        Destination cons_dest;

        num_msg = 5;

        LOG.info("TESTING TOPICS " + prod_broker_url + " -> " + cons_broker_url + " (" + num_msg + " messages)");

        conn = createConnection(cons_broker_url);
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //
        // Create the destination on which messages are being tested.
        //
        topic_name = "topotest2.perm.topic";
        LOG.trace("Removing existing Topic");
        removeTopic(conn, topic_name);
        LOG.trace("Creating Topic, " + topic_name);
        cons_dest = sess.createTopic(topic_name);

        testOneDest(conn, sess, cons_dest, num_msg);

        //
        // Cleanup
        //
        removeTopic(conn, topic_name);
        sess.close();
        conn.close();
    }

    /**
     * TEST TEMPORARY QUEUES
     */
    public void testTempQueue(String prod_broker_url, String cons_broker_url) throws Exception {
        int num_msg;

        Connection conn;
        Session sess;

        Destination cons_dest;

        num_msg = 5;

        LOG.info("TESTING TEMP QUEUES " + prod_broker_url + " -> " + cons_broker_url + " (" + num_msg + " messages)");

        //
        // Connect to the bus.
        //
        conn = createConnection(cons_broker_url);
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //
        // Create the destination on which messages are being tested.
        //
        LOG.trace("Creating destination");
        cons_dest = sess.createTemporaryQueue();

        testOneDest(conn, sess, cons_dest, num_msg);

        //
        // Cleanup
        //
        sess.close();
        conn.close();
    }

    /**
     * TEST QUEUES
     */
    public void testQueue(String prod_broker_url, String cons_broker_url) throws Exception {
        int num_msg;

        Connection conn;
        Session sess;
        String queue_name;

        Destination cons_dest;

        num_msg = 5;

        LOG.info("TESTING QUEUES " + prod_broker_url + " -> " + cons_broker_url + " (" + num_msg + " messages)");

        conn = createConnection(cons_broker_url);
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //
        // Create the destination on which messages are being tested.
        //
        queue_name = "topotest2.perm.queue";
        LOG.trace("Removing existing Queue");
        removeQueue(conn, queue_name);
        LOG.trace("Creating Queue, " + queue_name);
        cons_dest = sess.createQueue(queue_name);

        testOneDest(conn, sess, cons_dest, num_msg);

        removeQueue(conn, queue_name);
        sess.close();
        conn.close();
    }

    @Test
    public void runWithTempTopicReplyTo() throws Exception {
        EchoService echo_svc;
        TopicTrafficGenerator traffic_gen;
        Thread start1;
        Thread start2;
        Thread start3;
        Thread start4;
        ThreadPoolExecutor clientExecPool;
        final CountDownLatch clientCompletionLatch;
        int iter;

        fatalTestError = false;
        testError = false;

        //
        // Execute up to 20 clients at a time to simulate that load.
        //

        clientExecPool = new ThreadPoolExecutor(CONCURRENT_CLIENT_COUNT, CONCURRENT_CLIENT_COUNT, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10000));
        clientCompletionLatch = new CountDownLatch(TOTAL_CLIENT_ITER);

        // Use threads to avoid startup deadlock since the first broker started waits until
        // it knows the name of the remote broker before finishing its startup, which means
        // the remote must already be running.

        start1 = new Thread() {
            @Override
            public void run() {
                try {
                    edge1.start();
                } catch (Exception ex) {
                    LOG.error(null, ex);
                }
            }
        };

        start2 = new Thread() {
            @Override
            public void run() {
                try {
                    edge2.start();
                } catch (Exception ex) {
                    LOG.error(null, ex);
                }
            }
        };

        start3 = new Thread() {
            @Override
            public void run() {
                try {
                    core1.start();
                } catch (Exception ex) {
                    LOG.error(null, ex);
                }
            }
        };

        start4 = new Thread() {
            @Override
            public void run() {
                try {
                    core2.start();
                } catch (Exception ex) {
                    LOG.error(null, ex);
                }
            }
        };

        start1.start();
        start2.start();
        start3.start();
        start4.start();

        start1.join();
        start2.join();
        start3.join();
        start4.join();

        traffic_gen = new TopicTrafficGenerator(edge1.getConnectionUrl(), edge2.getConnectionUrl());
        traffic_gen.start();

        //
        // Now start the echo service with that queue.
        //
        echo_svc = new EchoService("echo", edge1.getConnectionUrl());
        echo_svc.start();

        //
        // Run the tests on Temp Topics.
        //

        LOG.info("** STARTING TEMP TOPIC TESTS");
        iter = 0;
        while ((iter < TOTAL_CLIENT_ITER) && (!fatalTestError)) {
            clientExecPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        RequestReplyToTopicViaThreeNetworkHopsTest.this.testTempTopic(edge1.getConnectionUrl(), edge2.getConnectionUrl());
                    } catch (Exception exc) {
                        LOG.error("test exception", exc);
                        fatalTestError = true;
                        testError = true;
                    }

                    clientCompletionLatch.countDown();
                }
            });

            iter++;
        }

        boolean allDoneOnTime = clientCompletionLatch.await(20, TimeUnit.MINUTES);

        LOG.info("** FINISHED TEMP TOPIC TESTS AFTER " + iter + " ITERATIONS, testError:" + testError + ", fatal: " + fatalTestError + ", onTime:"
            + allDoneOnTime);

        Thread.sleep(100);

        echo_svc.shutdown();
        traffic_gen.shutdown();

        shutdown();

        assertTrue("test completed in time", allDoneOnTime);
        assertTrue("no errors", !testError);
    }

    public void shutdown() throws Exception {
        edge1.stop();
        edge2.stop();
        core1.stop();
        core2.stop();
    }

    protected Connection createConnection(String url) throws Exception {
        return org.apache.activemq.ActiveMQConnection.makeConnection(url);
    }

    protected static void removeQueue(Connection conn, String dest_name) throws java.lang.Exception {
        org.apache.activemq.command.ActiveMQDestination dest;

        if (conn instanceof org.apache.activemq.ActiveMQConnection) {
            dest = org.apache.activemq.command.ActiveMQDestination.createDestination(dest_name, org.apache.activemq.command.ActiveMQDestination.QUEUE_TYPE);
            ((org.apache.activemq.ActiveMQConnection) conn).destroyDestination(dest);
        }
    }

    protected static void removeTopic(Connection conn, String dest_name) throws java.lang.Exception {
        org.apache.activemq.command.ActiveMQDestination dest;

        if (conn instanceof org.apache.activemq.ActiveMQConnection) {
            dest = org.apache.activemq.command.ActiveMQDestination.createDestination(dest_name, org.apache.activemq.command.ActiveMQDestination.TOPIC_TYPE);
            ((org.apache.activemq.ActiveMQConnection) conn).destroyDestination(dest);
        }
    }

    public static String fmtMsgInfo(Message msg) throws Exception {
        StringBuilder msg_desc;
        String prop;
        Enumeration<?> prop_enum;

        msg_desc = new StringBuilder();
        msg_desc = new StringBuilder();

        if (msg instanceof TextMessage) {
            msg_desc.append(((TextMessage) msg).getText());
        } else {
            msg_desc.append("[");
            msg_desc.append(msg.getClass().getName());
            msg_desc.append("]");
        }

        prop_enum = msg.getPropertyNames();
        while (prop_enum.hasMoreElements()) {
            prop = (String) prop_enum.nextElement();
            msg_desc.append("; ");
            msg_desc.append(prop);
            msg_desc.append("=");
            msg_desc.append(msg.getStringProperty(prop));
        }

        return msg_desc.toString();
    }

    protected class EmbeddedTcpBroker {
        protected BrokerService brokerSvc;
        protected int brokerNum;
        protected String brokerName;
        protected String brokerId;
        protected int port;
        protected String tcpUrl;
        protected String fullUrl;

        public EmbeddedTcpBroker(String name, int number) throws Exception {
            brokerSvc = new BrokerService();

            synchronized (this.getClass()) {
                brokerNum = Next_broker_num;
                Next_broker_num++;
            }

            brokerName = name + number;
            brokerId = brokerName;

            brokerSvc.setBrokerName(brokerName);
            brokerSvc.setBrokerId(brokerId);

            brokerSvc.setPersistent(false);
            brokerSvc.setUseJmx(false);

            port = 60000 + (brokerNum * 10);

            tcpUrl = "tcp://127.0.0.1:" + Integer.toString(port);
            fullUrl = tcpUrl + "?jms.watchTopicAdvisories=false";

            brokerSvc.addConnector(tcpUrl);
        }

        public Connection createConnection() throws URISyntaxException, JMSException {
            Connection result;

            result = org.apache.activemq.ActiveMQConnection.makeConnection(this.fullUrl);

            return result;
        }

        public String getConnectionUrl() {
            return this.fullUrl;
        }

        public void coreConnectTo(EmbeddedTcpBroker other, boolean duplex_f) throws Exception {
            this.makeConnectionTo(other, duplex_f, true);
            this.makeConnectionTo(other, duplex_f, false);
            if (!duplex_f) {
                other.makeConnectionTo(this, duplex_f, true);
                other.makeConnectionTo(this, duplex_f, false);
            }
        }

        public void start() throws Exception {
            brokerSvc.start();
            brokerSvc.waitUntilStarted();
        }

        public void stop() throws Exception {
            brokerSvc.stop();
        }

        protected void makeConnectionTo(EmbeddedTcpBroker other, boolean duplex_f, boolean queue_f) throws Exception {
            NetworkConnector nw_conn;
            String prefix;
            ActiveMQDestination excl_dest;
            ArrayList<ActiveMQDestination> excludes;

            nw_conn = new DiscoveryNetworkConnector(new URI("static:(" + other.tcpUrl + ")"));
            nw_conn.setDuplex(duplex_f);

            if (queue_f)
                nw_conn.setConduitSubscriptions(false);
            else
                nw_conn.setConduitSubscriptions(true);

            nw_conn.setNetworkTTL(3);
            nw_conn.setSuppressDuplicateQueueSubscriptions(true);
            nw_conn.setDecreaseNetworkConsumerPriority(true);
            nw_conn.setBridgeTempDestinations(queue_f);

            if (queue_f) {
                prefix = "queue";
                excl_dest = ActiveMQDestination.createDestination(">", ActiveMQDestination.TOPIC_TYPE);
            } else {
                prefix = "topic";
                excl_dest = ActiveMQDestination.createDestination(">", ActiveMQDestination.QUEUE_TYPE);
            }

            excludes = new ArrayList<ActiveMQDestination>();
            excludes.add(excl_dest);
            nw_conn.setExcludedDestinations(excludes);

            if (duplex_f)
                nw_conn.setName(this.brokerId + "<-" + prefix + "->" + other.brokerId);
            else
                nw_conn.setName(this.brokerId + "-" + prefix + "->" + other.brokerId);

            brokerSvc.addNetworkConnector(nw_conn);
        }
    }

    protected class MessageClient extends java.lang.Thread {
        protected MessageConsumer msgCons;
        protected boolean shutdownInd;
        protected int expectedCount;
        protected int lastSeq = 0;
        protected int msgCount = 0;
        protected boolean haveFirstSeq;
        protected CountDownLatch shutdownLatch;

        public MessageClient(MessageConsumer cons, int num_to_expect) {
            msgCons = cons;
            expectedCount = (num_to_expect * (echoResponseFill + 1));
            shutdownLatch = new CountDownLatch(1);
        }

        @Override
        public void run() {
            CountDownLatch latch;

            try {
                synchronized (this) {
                    latch = shutdownLatch;
                }

                shutdownInd = false;
                processMessages();

                latch.countDown();
            } catch (Exception exc) {
                LOG.error("message client error", exc);
            }
        }

        public void waitShutdown(long timeout) {
            CountDownLatch latch;

            try {
                synchronized (this) {
                    latch = shutdownLatch;
                }

                if (latch != null)
                    latch.await(timeout, TimeUnit.MILLISECONDS);
                else
                    LOG.info("echo client shutdown: client does not appear to be active");
            } catch (InterruptedException int_exc) {
                LOG.warn("wait for message client shutdown interrupted", int_exc);
            }
        }

        public boolean shutdown() {
            boolean down_ind;

            if (!shutdownInd) {
                shutdownInd = true;
            }

            waitShutdown(200);

            synchronized (this) {
                if ((shutdownLatch == null) || (shutdownLatch.getCount() == 0))
                    down_ind = true;
                else
                    down_ind = false;
            }

            return down_ind;
        }

        public int getNumMsgReceived() {
            return msgCount;
        }

        protected void processMessages() throws Exception {
            Message in_msg;

            haveFirstSeq = false;

            //
            // Stop at shutdown time or after any test error is detected.
            //

            while ((!shutdownInd) && (!fatalTestError)) {
                in_msg = msgCons.receive(100);

                if (in_msg != null) {
                    msgCount++;
                    checkMessage(in_msg);
                }
            }

            msgCons.close();
        }

        protected void checkMessage(Message in_msg) throws Exception {
            int seq;

            LOG.debug("received message " + fmtMsgInfo(in_msg) + " from " + in_msg.getJMSDestination());

            //
            // Only check messages with a sequence number.
            //

            if (in_msg.propertyExists("SEQ")) {
                seq = in_msg.getIntProperty("SEQ");

                if ((haveFirstSeq) && (seq != (lastSeq + 1))) {
                    LOG.error("***ERROR*** incorrect sequence number; expected " + Integer.toString(lastSeq + 1) + " but have " + Integer.toString(seq));

                    testError = true;
                }

                lastSeq = seq;

                if (msgCount > expectedCount) {
                    LOG.error("*** have more messages than expected; have " + msgCount + "; expect " + expectedCount);

                    testError = true;
                }
            }

            if (in_msg.propertyExists("end-of-response")) {
                LOG.trace("received end-of-response message");
            }
        }
    }

    /**
     *
     */
    protected class EchoService extends java.lang.Thread {
        protected String destName;
        protected Connection jmsConn;
        protected Session sess;
        protected MessageConsumer msg_cons;
        protected boolean Shutdown_ind;

        protected Destination req_dest;

        protected CountDownLatch waitShutdown;

        protected ThreadPoolExecutor processorPool;

        public EchoService(String dest, Connection broker_conn) throws Exception {
            destName = dest;
            jmsConn = broker_conn;

            Shutdown_ind = false;

            sess = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            req_dest = sess.createQueue(destName);
            msg_cons = sess.createConsumer(req_dest);

            jmsConn.start();

            waitShutdown = new CountDownLatch(1);

            processorPool = new ThreadPoolExecutor(CONCURRENT_SERVER_COUNT, CONCURRENT_SERVER_COUNT, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
                10000));
        }

        public EchoService(String dest, String broker_url) throws Exception {
            this(dest, ActiveMQConnection.makeConnection(broker_url));
        }

        @Override
        public void run() {
            Message req;

            try {
                LOG.info("STARTING ECHO SERVICE");

                while (!Shutdown_ind) {
                    req = msg_cons.receive(100);
                    if (req != null) {
                        processorPool.execute(new EchoRequestProcessor(sess, req));
                    }
                }
            } catch (Exception ex) {
                LOG.error("error processing echo service requests", ex);
            } finally {
                LOG.info("shutting down test echo service");

                try {
                    jmsConn.stop();
                } catch (javax.jms.JMSException jms_exc) {
                    LOG.warn("error on shutting down JMS connection", jms_exc);
                }

                synchronized (this) {
                    waitShutdown.countDown();
                }
            }
        }

        /**
         * Shut down the service, waiting up to 3 seconds for the service to terminate.
         */
        public void shutdown() {
            CountDownLatch wait_l;

            synchronized (this) {
                wait_l = waitShutdown;
            }

            Shutdown_ind = true;

            try {
                if (wait_l != null) {
                    if (wait_l.await(3000, TimeUnit.MILLISECONDS))
                        LOG.info("echo service shutdown complete");
                    else
                        LOG.warn("timeout waiting for echo service shutdown");
                } else {
                    LOG.info("echo service shutdown: service does not appear to be active");
                }
            } catch (InterruptedException int_exc) {
                LOG.warn("interrupted while waiting for echo service shutdown");
            }
        }
    }

    /**
     *
     */
    protected class EchoRequestProcessor implements Runnable {
        protected Session session;

        protected Destination resp_dest;
        protected MessageProducer msg_prod;

        protected Message request;

        public EchoRequestProcessor(Session sess, Message req) throws Exception {
            this.session = sess;
            this.request = req;

            this.resp_dest = req.getJMSReplyTo();

            if (resp_dest == null) {
                throw new Exception("invalid request: no reply-to destination given");
            }

            this.msg_prod = session.createProducer(this.resp_dest);
        }

        @Override
        public void run() {
            try {
                this.processRequest(this.request);
            } catch (Exception ex) {
                LOG.error("Failed to process request", ex);
            }
        }

        /**
         * Process one request for the Echo Service.
         */
        protected void processRequest(Message req) throws Exception {
            if (LOG.isDebugEnabled())
                LOG.debug("ECHO request message " + req.toString());

            resp_dest = req.getJMSReplyTo();
            if (resp_dest != null) {
                msg_prod = session.createProducer(resp_dest);

                LOG.debug("SENDING ECHO RESPONSE to:" + resp_dest);

                msg_prod.send(req);

                LOG.debug((((ActiveMQSession) session).getConnection()).getBrokerName() + " SENT ECHO RESPONSE to " + resp_dest);

                msg_prod.close();
                msg_prod = null;
            } else {
                LOG.warn("invalid request: no reply-to destination given");
            }
        }
    }

    protected class TopicTrafficGenerator extends java.lang.Thread {
        protected Connection conn1;
        protected Connection conn2;
        protected Session sess1;
        protected Session sess2;
        protected Destination dest;
        protected MessageProducer prod;
        protected MessageConsumer cons;
        protected boolean Shutdown_ind;
        protected int send_count;

        public TopicTrafficGenerator(String url1, String url2) throws Exception {
            conn1 = createConnection(url1);
            conn2 = createConnection(url2);

            sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

            conn1.start();
            conn2.start();

            dest = sess1.createTopic("traffic");
            prod = sess1.createProducer(dest);

            dest = sess2.createTopic("traffic");
            cons = sess2.createConsumer(dest);
        }

        public void shutdown() {
            Shutdown_ind = true;
        }

        @Override
        public void run() {
            Message msg;

            try {
                LOG.info("Starting Topic Traffic Generator");

                while (!Shutdown_ind) {
                    msg = sess1.createTextMessage("TRAFFIC");

                    prod.send(msg);

                    send_count++;

                    //
                    // Time out the receipt; early messages may not make it.
                    //

                    msg = cons.receive(250);
                }
            } catch (JMSException jms_exc) {
                LOG.warn("traffic generator failed on jms exception", jms_exc);
            } finally {
                LOG.info("Shutdown of Topic Traffic Generator; send count = " + send_count);

                if (conn1 != null) {
                    try {
                        conn1.stop();
                    } catch (JMSException jms_exc) {
                        LOG.warn("failed to shutdown connection", jms_exc);
                    }
                }

                if (conn2 != null) {
                    try {
                        conn2.stop();
                    } catch (JMSException jms_exc) {
                        LOG.warn("failed to shutdown connection", jms_exc);
                    }
                }
            }
        }
    }
}