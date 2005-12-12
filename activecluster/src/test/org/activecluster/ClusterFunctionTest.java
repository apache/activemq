/**
 *
 * Copyright 2003-2004 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.activecluster;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.activecluster.Cluster;
import org.activecluster.ClusterEvent;
import org.activecluster.ClusterListener;
import org.activecluster.impl.DefaultClusterFactory;
import org.activemq.ActiveMQConnectionFactory;

/**
 * Test ActiveCluster, ActiveMQ, with an eye to putting WADI on top of them.
 * 
 * @author <a href="mailto:jules@coredevelopers.net">Jules Gosnell </a>
 * @version $Revision: 1.4 $
 */
public class ClusterFunctionTest extends TestCase {
    protected Log _log = LogFactory.getLog(ClusterFunctionTest.class);

    public ClusterFunctionTest(String name) {
        super(name);
    }
    protected ActiveMQConnectionFactory _connectionFactory;
    protected Connection _connection;
    protected DefaultClusterFactory _clusterFactory;
    protected Cluster _cluster0;
    protected Cluster _cluster1;

    protected void setUp() throws Exception {
        testResponsePassed = false;
        _connectionFactory = new ActiveMQConnectionFactory("peer://cluster?persistent=false");
        _clusterFactory = new DefaultClusterFactory(_connectionFactory);
        _cluster0 = _clusterFactory.createCluster("ORG.CODEHAUS.WADI.TEST.CLUSTER");
        _cluster1 = _clusterFactory.createCluster("ORG.CODEHAUS.WADI.TEST.CLUSTER");
        _cluster0.start();
        _log.info("started node0: " + _cluster0.getLocalNode().getDestination());
        _cluster1.start();
        _log.info("started node1: " + _cluster1.getLocalNode().getDestination());
    }

    protected void tearDown() throws JMSException {
        //      _cluster1.stop();
        _cluster1 = null;
        //      _cluster0.stop();
        _cluster0 = null;
        _clusterFactory = null;
        //      _connection.stop();
        _connection = null;
        //      _connectionFactory.stop();
    }
    //----------------------------------------
    class MyClusterListener implements ClusterListener {
        public void onNodeAdd(ClusterEvent ce) {
            _log.info("node added: " + ce.getNode());
        }

        public void onNodeFailed(ClusterEvent ce) {
            _log.info("node failed: " + ce.getNode());
        }

        public void onNodeRemoved(ClusterEvent ce) {
            _log.info("node removed: " + ce.getNode());
        }

        public void onNodeUpdate(ClusterEvent ce) {
            _log.info("node updated: " + ce.getNode());
        }

        public void onCoordinatorChanged(ClusterEvent ce) {
            _log.info("coordinator changed: " + ce.getNode());
        }
    }

    public void testCluster() throws Exception {
        _cluster0.addClusterListener(new MyClusterListener());
        Map map = new HashMap();
        map.put("text", "testing123");
        _cluster0.getLocalNode().setState(map);
        _log.info("nodes: " + _cluster0.getNodes());
        Thread.sleep(10000);
        assertTrue(true);
    }
    /**
     * An invokable piece of work.
     */
    static interface Invocation extends java.io.Serializable {
        public void invoke(Cluster cluster, ObjectMessage om);
    }
    /**
     * Listen for messages, if they contain Invocations, invoke() them.
     */
    class InvocationListener implements MessageListener {
        protected Cluster _cluster;

        public InvocationListener(Cluster cluster) {
            _cluster = cluster;
        }

        public void onMessage(Message message) {
            _log.info("message received: " + message);
            ObjectMessage om = null;
            Object tmp = null;
            Invocation invocation = null;
            try {
                if (message instanceof ObjectMessage && (om = (ObjectMessage) message) != null
                        && (tmp = om.getObject()) != null && tmp instanceof Invocation
                        && (invocation = (Invocation) tmp) != null) {
                    _log.info("invoking message on: " + _cluster.getLocalNode());
                    invocation.invoke(_cluster, om);
                    _log.info("message successfully invoked on: " + _cluster.getLocalNode());
                }
                else {
                    _log.warn("bad message: " + message);
                }
            }
            catch (JMSException e) {
                _log.warn("unexpected problem", e);
            }
        }
    }
    /**
     * A request for a piece of work which involves sending a response back to the original requester.
     */
    static class Request implements Invocation {
        public void invoke(Cluster cluster, ObjectMessage om2) {
            try {
                System.out.println("request received");
                ObjectMessage om = cluster.createObjectMessage();
                om.setJMSReplyTo(cluster.createDestination(cluster.getLocalNode().getDestination()));
                om.setObject(new Response());
                System.out.println("sending response");
                cluster.send(om2.getJMSReplyTo(), om);
                System.out.println("request processed");
            }
            catch (JMSException e) {
                System.err.println("problem sending response");
                e.printStackTrace();
            }
        }
    }
    static boolean testResponsePassed = false;
    /**
     * A response containing a piece of work.
     */
    static class Response implements Invocation {
        public void invoke(Cluster cluster, ObjectMessage om) {
            try {
                System.out.println("response arrived from: " + om.getJMSReplyTo());
                // set a flag to test later
                ClusterFunctionTest.testResponsePassed = true;
                System.out.println("response processed on: " + cluster.getLocalNode().getDestination());
            }
            catch (JMSException e) {
                System.err.println("problem processing response");
            }
        }
    }

    public void testResponse() throws Exception {
        MessageListener listener0 = new InvocationListener(_cluster0);
        MessageListener listener1 = new InvocationListener(_cluster1);
        // 1->(n-1) messages (excludes self)
        _cluster0.createConsumer(_cluster0.getDestination(), null, true).setMessageListener(listener0);
        // 1->1 messages
        _cluster0.createConsumer(_cluster0.getLocalNode().getDestination()).setMessageListener(listener0);
        // 1->(n-1) messages (excludes self)
        _cluster1.createConsumer(_cluster1.getDestination(), null, true).setMessageListener(listener1);
        // 1->1 messages
        _cluster1.createConsumer(_cluster1.getLocalNode().getDestination()).setMessageListener(listener1);
        ObjectMessage om = _cluster0.createObjectMessage();
        om.setJMSReplyTo(_cluster0.createDestination(_cluster0.getLocalNode().getDestination()));
        om.setObject(new Request());
        testResponsePassed = false;
        _cluster0.send(_cluster0.getLocalNode().getDestination(), om);
        Thread.sleep(3000);
        assertTrue(testResponsePassed);
        _log.info("request/response between same node OK");
        testResponsePassed = false;
        _cluster0.send(_cluster1.getLocalNode().getDestination(), om);
        Thread.sleep(3000);
        assertTrue(testResponsePassed);
        _log.info("request/response between two different nodes OK");
    }
}