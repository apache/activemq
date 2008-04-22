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

import java.lang.reflect.Method;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.work.WorkManager;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.jmock.Mock;
import org.jmock.cglib.MockObjectTestCase;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ServerSessionImplTest extends MockObjectTestCase {
    private static final String BROKER_URL = "vm://localhost";
    private ServerSessionImpl serverSession;
    private Mock pool;
    private Mock workManager;
    private MessageEndpoint messageEndpoint;
    private ActiveMQConnection con;
    private ActiveMQSession session;
    
    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        org.apache.activemq.ActiveMQConnectionFactory factory = 
                new org.apache.activemq.ActiveMQConnectionFactory(BROKER_URL);
        con = (ActiveMQConnection) factory.createConnection();
        session = (ActiveMQSession) con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        pool = mock(ServerSessionPoolImpl.class, new Class[]{ActiveMQEndpointWorker.class, int.class}, new Object[]{null, 10});        
        workManager = mock(WorkManager.class);
        messageEndpoint = new MockMessageEndpoint();
        
        serverSession = new ServerSessionImpl(
                (ServerSessionPoolImpl) pool.proxy(), 
                session, 
                (WorkManager) workManager.proxy(), 
                messageEndpoint, 
                false, 
                10);
    }
    
    private class MockMessageEndpoint implements MessageEndpoint, MessageListener {

        public void afterDelivery() throws ResourceException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void beforeDelivery(Method arg0) throws NoSuchMethodException, ResourceException
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void release()
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void onMessage(Message msg)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
    }
    
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
    
    public void testRunDetectsStoppedSession() throws Exception {
        con.close();
        pool.expects(once()).method("removeFromPool").with(eq(serverSession));
        serverSession.run();
        pool.verify();
}
}
