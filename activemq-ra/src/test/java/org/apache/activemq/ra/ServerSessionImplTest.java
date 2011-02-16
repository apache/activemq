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

import javax.jms.Session;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.work.WorkManager;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * 
 */
@RunWith(JMock.class)
public class ServerSessionImplTest extends TestCase {
    private static final String BROKER_URL = "vm://localhost";
    private ServerSessionImpl serverSession;
    private ServerSessionPoolImpl pool;
    private WorkManager workManager;
    private MessageEndpoint messageEndpoint;
    private ActiveMQConnection con;
    private ActiveMQSession session;
    private Mockery context;
    
    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        context = new Mockery() {{
            setImposteriser(ClassImposteriser.INSTANCE);
        }};
        
        org.apache.activemq.ActiveMQConnectionFactory factory = 
                new org.apache.activemq.ActiveMQConnectionFactory(BROKER_URL);
        con = (ActiveMQConnection) factory.createConnection();
        session = (ActiveMQSession) con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        pool = context.mock(ServerSessionPoolImpl.class);        
        workManager = context.mock(WorkManager.class);
        
        serverSession = new ServerSessionImpl(
                (ServerSessionPoolImpl) pool, 
                session, 
                (WorkManager) workManager, 
                messageEndpoint, 
                false, 
                10);
    }
    
    @Test
    public void testRunDetectsStoppedSession() throws Exception {
        con.close();
        context.checking(new Expectations() {{
            oneOf (pool).removeFromPool(with(same(serverSession)));
        }});   
        serverSession.run();
    }
}
