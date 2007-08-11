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
package org.apache.activemq.ra;

import org.jmock.MockObjectTestCase;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ServerSessionImplTest extends MockObjectTestCase {
    
    /**
     * Need to re-work this test case, it broke since the amq4 internals changed and
     * mocks were being using against the internals.
     *
     */
    public void testDummy() {
    }
    
/*
    public void testBatch() throws Exception {
        DummyActiveMQConnection connection = new DummyActiveMQConnection(new ActiveMQConnectionFactory(),
                 null,
                 null,
                 getMockTransportChannel());
        ServerSessionPoolImpl pool = new ServerSessionPoolImpl(null, 1);
        DummyActiveMQSession session = new DummyActiveMQSession(connection);
        MemoryBoundedQueue queue = connection.getMemoryBoundedQueue("Session(" + session.getSessionId() + ")");
        queue.enqueue(new ActiveMQTextMessage());
        queue.enqueue(new ActiveMQTextMessage());
        queue.enqueue(new ActiveMQTextMessage());
        DummyMessageEndpoint endpoint = new DummyMessageEndpoint();
        ServerSessionImpl serverSession = new ServerSessionImpl(pool, session, null, endpoint, true, 2);
        serverSession.run();
        assertEquals(2, endpoint.messagesPerBatch.size());
        assertEquals(new Integer(2), endpoint.messagesPerBatch.get(0));
        assertEquals(new Integer(1), endpoint.messagesPerBatch.get(1));
    }

    private class DummyMessageEndpoint implements MessageEndpoint, MessageListener {
        protected List messagesPerBatch = new ArrayList();
        protected int nbMessages = -1000;
        public void beforeDelivery(Method arg0) throws NoSuchMethodException, ResourceException {
            nbMessages = 0;
        }
        public void afterDelivery() throws ResourceException {
            messagesPerBatch.add(new Integer(nbMessages));
            nbMessages = -1000;
        }
        public void release() {
        }
        public void onMessage(Message arg0) {
            nbMessages ++;
        }
    }

    private class DummyActiveMQSession extends ActiveMQSession {
        protected DummyActiveMQSession(ActiveMQConnection connection, SessionId sessionId, int acknowledgeMode, boolean asyncDispatch) throws JMSException {
            super(connection, sessionId, acknowledgeMode, asyncDispatch);
        }
    }

    private class DummyActiveMQConnection extends ActiveMQConnection {
        protected DummyActiveMQConnection(Transport transport, String userName, String password, JMSStatsImpl factoryStats) throws IOException {
            super(transport, userName, password, factoryStats);
        }
    }

    private TransportChannel getMockTransportChannel() {
        Mock tc = new Mock(TransportChannel.class);
        tc.expects(once()).method("setPacketListener");
        tc.expects(once()).method("setExceptionListener");
        tc.expects(once()).method("addTransportStatusEventListener");
        tc.expects(atLeastOnce()).method("asyncSend");
        tc.expects(atLeastOnce()).method("send");
        return (TransportChannel) tc.proxy();
    }
    */
}
